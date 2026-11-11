package controller

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Dd1235/objstore/internal/api"
)

type fakeClock struct{ t time.Time }

func (f *fakeClock) now() time.Time          { return f.t }
func (f *fakeClock) advance(d time.Duration) { f.t = f.t.Add(d) }

func newTestController(t *testing.T) (*Controller, *fakeClock) {
	t.Helper()
	c, err := New(DefaultConfig(), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	c.now = clk.now
	return c, clk
}

func call(t *testing.T, h http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if s, ok := body.(string); ok {
			buf.WriteString(s)
		} else if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(method, path, &buf))
	return rec
}

func workers(t *testing.T, h http.Handler) map[string]api.WorkerStatus {
	t.Helper()
	rec := call(t, h, "GET", "/internal/workers", nil)
	var list api.WorkerList
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]api.WorkerStatus)
	for _, w := range list.Workers {
		out[w.WorkerID] = w
	}
	return out
}

func TestRegisterHeartbeatAndTimeouts(t *testing.T) {
	c, clk := newTestController(t)
	h := c.Handler()

	rec := call(t, h, "POST", "/internal/workers/register",
		api.RegisterRequest{WorkerID: "worker1", Addr: "http://worker1:8080", ChunkCount: 3})
	if rec.Code != http.StatusOK {
		t.Fatalf("register: %d %s", rec.Code, rec.Body)
	}
	var reg api.RegisterResponse
	json.Unmarshal(rec.Body.Bytes(), &reg)
	if reg.HeartbeatIntervalMS != 1000 {
		t.Fatalf("heartbeat interval = %d", reg.HeartbeatIntervalMS)
	}
	if w := workers(t, h)["worker1"]; w.State != "alive" || w.ChunkCount != 3 {
		t.Fatalf("worker1 = %+v", w)
	}

	clk.advance(time.Second)
	if rec := call(t, h, "POST", "/internal/workers/heartbeat", api.HeartbeatRequest{WorkerID: "worker1", ChunkCount: 4}); rec.Code != http.StatusOK {
		t.Fatalf("heartbeat: %d", rec.Code)
	}

	clk.advance(4 * time.Second)
	c.Tick()
	if w := workers(t, h)["worker1"]; w.State != "suspect" || w.ChunkCount != 4 {
		t.Fatalf("after 4 s silence: %+v", w)
	}
	clk.advance(30 * time.Second)
	c.Tick()
	if w := workers(t, h)["worker1"]; w.State != "dead" {
		t.Fatalf("after 34 s silence: %+v", w)
	}

	// The worker comes back and must pass the rejoin delay.
	call(t, h, "POST", "/internal/workers/register", api.RegisterRequest{WorkerID: "worker1", Addr: "http://worker1:8080"})
	if w := workers(t, h)["worker1"]; w.State != "suspect" || !w.Rejoining {
		t.Fatalf("after return: %+v", w)
	}
}

func TestHeartbeatFromUnknownWorker(t *testing.T) {
	c, _ := newTestController(t)
	rec := call(t, c.Handler(), "POST", "/internal/workers/heartbeat", api.HeartbeatRequest{WorkerID: "ghost"})
	var e api.Error
	json.Unmarshal(rec.Body.Bytes(), &e)
	if rec.Code != http.StatusNotFound || e.Code != api.CodeUnknownWorker {
		t.Fatalf("got %d %+v", rec.Code, e)
	}
}

func TestRegisterValidation(t *testing.T) {
	c, _ := newTestController(t)
	h := c.Handler()
	bad := []any{
		"not json",
		"",
		api.RegisterRequest{WorkerID: "", Addr: "http://w:8080"},
		api.RegisterRequest{WorkerID: "../w", Addr: "http://w:8080"},
		api.RegisterRequest{WorkerID: "w1", Addr: "w1:8080"},
		api.RegisterRequest{WorkerID: "w1", Addr: "ftp://w1"},
	}
	for _, body := range bad {
		if rec := call(t, h, "POST", "/internal/workers/register", body); rec.Code != http.StatusBadRequest {
			t.Errorf("body %v: got %d, want 400", body, rec.Code)
		}
	}
	if len(workers(t, h)) != 0 {
		t.Fatal("invalid registration was recorded")
	}
}
