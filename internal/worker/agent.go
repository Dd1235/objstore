package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Dd1235/objstore/internal/api"
)

// StatsSource reports how many chunks a worker holds and their total size.
type StatsSource interface {
	Stats() (count int, bytes int64)
}

// Agent registers a worker with the controller and then sends heartbeats.
// If the controller stops recognising the worker (for example after a
// controller restart) the agent registers again.
type Agent struct {
	WorkerID      string
	Addr          string // base URL the controller uses to reach this worker
	ControllerURL string
	Stats         StatsSource
	Client        *http.Client
	Log           *slog.Logger
	RetryDelay    time.Duration // wait between failed registrations; default 1s
}

var errReregister = errors.New("controller does not know this worker")

// Run blocks until ctx is cancelled.
func (a *Agent) Run(ctx context.Context) error {
	for {
		interval, err := a.register(ctx)
		if err != nil {
			return err
		}
		err = a.heartbeatLoop(ctx, interval)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		a.log().Warn("re-registering with controller", "reason", err)
	}
}

func (a *Agent) register(ctx context.Context) (time.Duration, error) {
	delay := a.RetryDelay
	if delay <= 0 {
		delay = time.Second
	}
	failing := false
	for {
		count, used := a.Stats.Stats()
		var resp api.RegisterResponse
		err := a.post(ctx, "/internal/workers/register", api.RegisterRequest{
			WorkerID: a.WorkerID, Addr: a.Addr, ChunkCount: count, UsedBytes: used,
		}, &resp)
		if err == nil {
			interval := time.Duration(resp.HeartbeatIntervalMS) * time.Millisecond
			if interval <= 0 {
				interval = time.Second
			}
			a.log().Info("registered with controller", "controller", a.ControllerURL, "heartbeat_interval", interval)
			return interval, nil
		}
		if !failing {
			a.log().Warn("registration failed, retrying", "err", err)
			failing = true
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (a *Agent) heartbeatLoop(ctx context.Context, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	failing := false
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		count, used := a.Stats.Stats()
		err := a.post(ctx, "/internal/workers/heartbeat", api.HeartbeatRequest{
			WorkerID: a.WorkerID, ChunkCount: count, UsedBytes: used,
		}, nil)
		var apiErr *api.Error
		switch {
		case err == nil:
			if failing {
				a.log().Info("heartbeats restored")
				failing = false
			}
		case errors.As(err, &apiErr) && apiErr.Code == api.CodeUnknownWorker:
			return errReregister
		case ctx.Err() != nil:
			return ctx.Err()
		default:
			if !failing {
				a.log().Warn("heartbeat failed", "err", err)
				failing = true
			}
		}
	}
}

// post sends body as JSON and decodes a 2xx JSON response into out (if not
// nil). A non-2xx response with an api.Error body is returned as *api.Error.
func (a *Agent) post(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	url := strings.TrimSuffix(a.ControllerURL, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client().Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		var e api.Error
		if json.Unmarshal(data, &e) == nil && e.Code != "" {
			return &e
		}
		return fmt.Errorf("%s: %s", resp.Status, bytes.TrimSpace(data))
	}
	if out == nil {
		return nil
	}
	return json.Unmarshal(data, out)
}

func (a *Agent) client() *http.Client {
	if a.Client != nil {
		return a.Client
	}
	return &http.Client{Timeout: 2 * time.Second}
}

func (a *Agent) log() *slog.Logger {
	if a.Log != nil {
		return a.Log
	}
	return slog.Default()
}
