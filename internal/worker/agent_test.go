package worker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Dd1235/objstore/internal/api"
)

// handlerTransport serves requests in-process so tests need no network and
// run under synctest's fake clock.
type handlerTransport struct{ h http.Handler }

func (t handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	t.h.ServeHTTP(rec, r)
	return rec.Result(), nil
}

type fakeController struct {
	mu            sync.Mutex
	failRegisters int // reject this many registrations with 503
	known         bool
	registers     int
	heartbeats    int
	last          api.HeartbeatRequest
}

func (c *fakeController) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch r.URL.Path {
	case "/internal/workers/register":
		if c.failRegisters > 0 {
			c.failRegisters--
			api.WriteError(w, http.StatusServiceUnavailable, api.CodeUnavailable, "starting")
			return
		}
		c.registers++
		c.known = true
		api.WriteJSON(w, http.StatusOK, api.RegisterResponse{HeartbeatIntervalMS: 1000})
	case "/internal/workers/heartbeat":
		if !c.known {
			api.WriteError(w, http.StatusNotFound, api.CodeUnknownWorker, "who are you")
			return
		}
		c.heartbeats++
		json.NewDecoder(r.Body).Decode(&c.last)
		api.WriteJSON(w, http.StatusOK, struct{}{})
	default:
		http.NotFound(w, r)
	}
}

func (c *fakeController) counts() (registers, heartbeats int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.registers, c.heartbeats
}

type fixedStats struct{}

func (fixedStats) Stats() (int, int64) { return 7, 7000 }

func TestAgentRegistersAndHeartbeats(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeController{failRegisters: 3}
		ctx, cancel := context.WithCancel(t.Context())
		a := &Agent{
			WorkerID: "worker1", Addr: "http://worker1:8080", ControllerURL: "http://controller:8080",
			Stats: fixedStats{}, Client: &http.Client{Transport: handlerTransport{fc}},
			RetryDelay: 500 * time.Millisecond,
		}
		done := make(chan error)
		go func() { done <- a.Run(ctx) }()

		// Three rejected attempts 500 ms apart, then success at t=1.5s.
		time.Sleep(1400 * time.Millisecond)
		synctest.Wait()
		if r, _ := fc.counts(); r != 0 {
			t.Fatalf("registered too early: %d", r)
		}
		time.Sleep(200 * time.Millisecond)
		synctest.Wait()
		if r, _ := fc.counts(); r != 1 {
			t.Fatalf("registers = %d, want 1", r)
		}

		// Heartbeats every second after registration.
		time.Sleep(5 * time.Second)
		synctest.Wait()
		if _, h := fc.counts(); h != 5 {
			t.Fatalf("heartbeats = %d, want 5", h)
		}
		fc.mu.Lock()
		last := fc.last
		fc.mu.Unlock()
		if last.WorkerID != "worker1" || last.ChunkCount != 7 || last.UsedBytes != 7000 {
			t.Fatalf("heartbeat body = %+v", last)
		}

		// The controller restarts and forgets the worker: it must re-register.
		fc.mu.Lock()
		fc.known = false
		fc.mu.Unlock()
		time.Sleep(time.Second)
		synctest.Wait()
		if r, _ := fc.counts(); r != 2 {
			t.Fatalf("registers after controller restart = %d, want 2", r)
		}

		cancel()
		if err := <-done; err != context.Canceled {
			t.Fatalf("Run returned %v", err)
		}
	})
}
