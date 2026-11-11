// Package controller implements the controller node: the client object API,
// worker membership, chunk placement, replica reads and repair.
//
// Decision logic lives in pure, deterministic code (packages membership,
// placement and repair); this package is the driver that feeds it HTTP requests and
// clock ticks. All I/O goes through injected dependencies: metadata through
// meta.Store, worker calls through ChunkClient, randomness through an
// io.Reader, and time through the now function.
package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/chunker"
	"github.com/Dd1235/objstore/internal/ids"
	"github.com/Dd1235/objstore/internal/membership"
	"github.com/Dd1235/objstore/internal/meta"
)

// Config holds controller settings.
type Config struct {
	Membership        membership.Config
	HeartbeatInterval time.Duration // told to workers at registration
	TickInterval      time.Duration // how often liveness timeouts are applied
	ChunkSize         int           // bytes per chunk
	Replicas          int           // copies of each chunk
	WorkerTimeout     time.Duration // limit on each call to a worker
	RepairInterval    time.Duration // time between repair passes
	RepairDelay       time.Duration // wait before the first repair pass after start
}

// DefaultConfig returns settings for the local Docker deployment.
func DefaultConfig() Config {
	return Config{
		Membership:        membership.DefaultConfig(),
		HeartbeatInterval: time.Second,
		TickInterval:      500 * time.Millisecond,
		ChunkSize:         chunker.Size,
		Replicas:          2,
		WorkerTimeout:     5 * time.Second,
		RepairInterval:    5 * time.Second,
		// Workers that are running register within a heartbeat or two of a
		// controller restart; one that has not after dead-after is lost.
		RepairDelay: membership.DefaultConfig().DeadAfter,
	}
}

// ChunkClient is how the controller talks to workers. workerclient.Client
// implements it over HTTP.
type ChunkClient interface {
	Put(ctx context.Context, addr, chunkID string, data []byte, sum [sha256.Size]byte) error
	Get(ctx context.Context, addr, chunkID string) ([]byte, error)
	Delete(ctx context.Context, addr, chunkID string) error
	Inventory(ctx context.Context, addr, workerID string) (api.ChunkInventory, error)
}

// Deps are the controller's external dependencies.
type Deps struct {
	Meta   meta.Store
	Chunks ChunkClient
	Rand   io.Reader    // source for object IDs; default crypto/rand
	Log    *slog.Logger // default slog.Default()
}

// Controller is the controller node.
type Controller struct {
	cfg    Config
	log    *slog.Logger
	now    func() time.Time
	meta   meta.Store
	chunks ChunkClient
	rand   io.Reader

	mu      sync.Mutex // guards members
	members *membership.Table

	rmu      sync.Mutex      // orders downloads against deletes
	readers  map[string]int  // object ID -> downloads in progress
	deleting map[string]bool // deleted objects whose chunks wait for readers to finish

	repairMu   sync.Mutex
	lastRepair *api.RepairSummary

	bgCtx    context.Context
	bgCancel context.CancelFunc
	bg       sync.WaitGroup
}

// New returns a controller with no registered workers.
func New(cfg Config, d Deps) (*Controller, error) {
	if cfg.HeartbeatInterval <= 0 || cfg.TickInterval <= 0 || cfg.WorkerTimeout <= 0 || cfg.RepairInterval <= 0 || cfg.RepairDelay < 0 {
		return nil, errors.New("controller: intervals must be positive")
	}
	if cfg.ChunkSize <= 0 || cfg.Replicas <= 0 {
		return nil, errors.New("controller: chunk size and replica count must be positive")
	}
	if d.Meta == nil || d.Chunks == nil {
		return nil, errors.New("controller: Meta and Chunks are required")
	}
	members, err := membership.New(cfg.Membership)
	if err != nil {
		return nil, err
	}
	if d.Log == nil {
		d.Log = slog.Default()
	}
	if d.Rand == nil {
		d.Rand = rand.Reader
	}
	bgCtx, cancel := context.WithCancel(context.Background())
	return &Controller{
		cfg: cfg, log: d.Log, now: time.Now, meta: d.Meta, chunks: d.Chunks, rand: d.Rand,
		members:  members,
		readers:  make(map[string]int),
		deleting: make(map[string]bool),
		bgCtx:    bgCtx, bgCancel: cancel,
	}, nil
}

// Close stops background work (repair, chunk clean-up, read repair) and
// waits for it.
func (c *Controller) Close() {
	c.bgCancel()
	c.bg.Wait()
}

// background runs fn on its own goroutine, tracked so Close can wait for it.
func (c *Controller) background(fn func(ctx context.Context)) {
	c.bg.Add(1)
	go func() {
		defer c.bg.Done()
		fn(c.bgCtx)
	}()
}

// Handler returns the controller's HTTP routes.
func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/objects", c.uploadObject)
	mux.HandleFunc("GET /api/v1/objects", c.listObjects)
	mux.HandleFunc("GET /api/v1/objects/{object_id}", c.downloadObject)
	mux.HandleFunc("DELETE /api/v1/objects/{object_id}", c.deleteObject)
	mux.HandleFunc("GET /api/v1/objects/{object_id}/metadata", c.getMetadata)
	mux.HandleFunc("PATCH /api/v1/objects/{object_id}/metadata", c.patchMetadata)

	mux.HandleFunc("POST /internal/workers/register", c.register)
	mux.HandleFunc("POST /internal/workers/heartbeat", c.heartbeat)
	mux.HandleFunc("GET /internal/workers", c.listWorkers)
	mux.HandleFunc("GET /internal/workers/{worker_id}/chunks", c.workerChunks)
	mux.HandleFunc("GET /internal/replication", c.replicationStatus)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		api.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
}

// workerRef is a worker ID with the address used to reach it.
type workerRef struct{ id, addr string }

// placeable returns the workers that may receive new chunk copies.
func (c *Controller) placeable() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]string)
	for _, w := range c.members.List() {
		if w.Placeable() {
			out[w.ID] = w.Addr
		}
	}
	return out
}

// readOrder returns the replicas worth reading from: alive workers first,
// then suspect ones, in the order given. Dead and unknown workers are skipped.
func (c *Controller) readOrder(replicas []string) []workerRef {
	c.mu.Lock()
	defer c.mu.Unlock()
	var alive, suspect []workerRef
	for _, id := range replicas {
		w, ok := c.members.Get(id)
		switch {
		case !ok || !w.Readable():
		case w.State == membership.Alive:
			alive = append(alive, workerRef{id, w.Addr})
		default:
			suspect = append(suspect, workerRef{id, w.Addr})
		}
	}
	return append(alive, suspect...)
}

// addr returns the address of a known worker.
func (c *Controller) addr(id string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	w, ok := c.members.Get(id)
	return w.Addr, ok
}

// workerChunks proxies a worker's chunk inventory, so it can be inspected
// through the controller's published port.
func (c *Controller) workerChunks(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("worker_id")
	addr, ok := c.addr(id)
	if !ok {
		api.WriteError(w, http.StatusNotFound, api.CodeNotFound, "unknown worker "+id)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), c.cfg.WorkerTimeout)
	defer cancel()
	inv, err := c.chunks.Inventory(ctx, addr, id)
	if err != nil {
		api.WriteError(w, http.StatusServiceUnavailable, api.CodeUnavailable, "worker "+id+" unreachable: "+err.Error())
		return
	}
	api.WriteJSON(w, http.StatusOK, inv)
}

// Tick applies membership timeouts as of the current time.
func (c *Controller) Tick() {
	c.mu.Lock()
	trs := c.members.Tick(c.now())
	c.mu.Unlock()
	c.logTransitions(trs)
}

// RunTicker calls Tick every TickInterval until done is closed.
func (c *Controller) RunTicker(done <-chan struct{}) {
	t := time.NewTicker(c.cfg.TickInterval)
	defer t.Stop()
	for {
		select {
		case <-done:
			return
		case <-t.C:
			c.Tick()
		}
	}
}

func (c *Controller) register(w http.ResponseWriter, r *http.Request) {
	var req api.RegisterRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if !ids.ValidChunkID(req.WorkerID) {
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, "worker_id must be 1-128 characters of [A-Za-z0-9_.-]")
		return
	}
	if u, err := url.Parse(req.Addr); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, "addr must be an http(s) URL")
		return
	}
	c.mu.Lock()
	trs := c.members.Register(c.now(), req.WorkerID, req.Addr, membership.Stats{ChunkCount: req.ChunkCount, UsedBytes: req.UsedBytes})
	c.mu.Unlock()
	c.logTransitions(trs)
	api.WriteJSON(w, http.StatusOK, api.RegisterResponse{HeartbeatIntervalMS: c.cfg.HeartbeatInterval.Milliseconds()})
}

func (c *Controller) heartbeat(w http.ResponseWriter, r *http.Request) {
	var req api.HeartbeatRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	c.mu.Lock()
	trs, err := c.members.Heartbeat(c.now(), req.WorkerID, membership.Stats{ChunkCount: req.ChunkCount, UsedBytes: req.UsedBytes})
	c.mu.Unlock()
	if errors.Is(err, membership.ErrUnknownWorker) {
		api.WriteError(w, http.StatusNotFound, api.CodeUnknownWorker, "worker "+req.WorkerID+" is not registered")
		return
	}
	c.logTransitions(trs)
	api.WriteJSON(w, http.StatusOK, struct{}{})
}

func (c *Controller) listWorkers(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	workers := c.members.List()
	c.mu.Unlock()
	out := api.WorkerList{Workers: make([]api.WorkerStatus, 0, len(workers))}
	for _, wk := range workers {
		out.Workers = append(out.Workers, api.WorkerStatus{
			WorkerID:      wk.ID,
			Addr:          wk.Addr,
			State:         wk.State.String(),
			Rejoining:     wk.Rejoining,
			LastHeartbeat: wk.LastHeartbeat,
			StateSince:    wk.StateSince,
			ChunkCount:    wk.ChunkCount,
			UsedBytes:     wk.UsedBytes,
		})
	}
	api.WriteJSON(w, http.StatusOK, out)
}

func (c *Controller) logTransitions(trs []membership.Transition) {
	for _, tr := range trs {
		level := slog.LevelInfo
		if tr.To == membership.Suspect || tr.To == membership.Dead {
			level = slog.LevelWarn
		}
		c.log.Log(context.Background(), level, "worker state changed",
			"worker_id", tr.WorkerID, "from", tr.From.String(), "to", tr.To.String(), "reason", tr.Reason)
	}
}

// decodeJSON reads a small JSON body into v, writing a 400 on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	body := http.MaxBytesReader(w, r.Body, 64<<10)
	if err := json.NewDecoder(body).Decode(v); err != nil {
		msg := "invalid JSON body"
		if !errors.Is(err, io.EOF) {
			msg += ": " + err.Error()
		}
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, msg)
		return false
	}
	return true
}
