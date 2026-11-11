// Package controller implements the controller node: worker membership now,
// and later the client object API, metadata, placement and repair.
//
// Decision logic lives in pure state machines (see package membership); this
// package is the driver that feeds them HTTP requests and clock ticks.
package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/ids"
	"github.com/Dd1235/objstore/internal/membership"
)

// Config holds controller settings.
type Config struct {
	Membership        membership.Config
	HeartbeatInterval time.Duration // told to workers at registration
	TickInterval      time.Duration // how often liveness timeouts are applied
}

// DefaultConfig returns settings for the local Docker deployment.
func DefaultConfig() Config {
	return Config{
		Membership:        membership.DefaultConfig(),
		HeartbeatInterval: time.Second,
		TickInterval:      500 * time.Millisecond,
	}
}

// Controller is the controller node.
type Controller struct {
	cfg Config
	log *slog.Logger
	now func() time.Time

	mu      sync.Mutex
	members *membership.Table
}

// New returns a controller with no registered workers.
func New(cfg Config, log *slog.Logger) (*Controller, error) {
	if cfg.HeartbeatInterval <= 0 || cfg.TickInterval <= 0 {
		return nil, errors.New("controller: intervals must be positive")
	}
	members, err := membership.New(cfg.Membership)
	if err != nil {
		return nil, err
	}
	if log == nil {
		log = slog.Default()
	}
	return &Controller{cfg: cfg, log: log, now: time.Now, members: members}, nil
}

// Handler returns the controller's HTTP routes.
func (c *Controller) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /internal/workers/register", c.register)
	mux.HandleFunc("POST /internal/workers/heartbeat", c.heartbeat)
	mux.HandleFunc("GET /internal/workers", c.listWorkers)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		api.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	return mux
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
