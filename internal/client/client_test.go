package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/controller"
	"github.com/Dd1235/objstore/internal/meta"
	"github.com/Dd1235/objstore/internal/worker"
	"github.com/Dd1235/objstore/internal/worker/store"
	"github.com/Dd1235/objstore/internal/workerclient"
)

const chunkSize = 4096

// newSystem starts a controller and four workers on real local HTTP servers
// and returns a client for the controller.
func newSystem(t *testing.T) *Client {
	t.Helper()
	quiet := slog.New(slog.DiscardHandler)
	cfg := controller.DefaultConfig()
	cfg.ChunkSize = chunkSize
	c, err := controller.New(cfg, controller.Deps{
		Meta:   meta.NewMem(),
		Chunks: &workerclient.Client{HTTP: &http.Client{}, MaxChunk: chunkSize},
		Log:    quiet,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	ctrl := httptest.NewServer(c.Handler())
	t.Cleanup(ctrl.Close)

	for i := 1; i <= 4; i++ {
		id := fmt.Sprintf("worker%d", i)
		st, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer((&worker.Server{ID: id, Store: st, MaxChunk: chunkSize, Log: quiet}).Handler())
		t.Cleanup(srv.Close)
		body, _ := json.Marshal(api.RegisterRequest{WorkerID: id, Addr: srv.URL})
		resp, err := http.Post(ctrl.URL+"/internal/workers/register", "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("register %s: %d", id, resp.StatusCode)
		}
	}
	return &Client{BaseURL: ctrl.URL + "/"}
}

func TestClientEndToEnd(t *testing.T) {
	cl := newSystem(t)
	ctx := context.Background()
	data := make([]byte, 5*chunkSize+123)
	r := rand.New(rand.NewPCG(1, 2))
	for i := range data {
		data[i] = byte(r.Uint32())
	}

	info, err := cl.Upload(ctx, "report.pdf", bytes.NewReader(data), "", map[string]string{"owner": "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if info.ChunkCount != 6 || info.ContentType != "application/pdf" || info.Metadata["owner"] != "alice" {
		t.Fatalf("upload: %+v", info)
	}
	for _, ch := range info.Chunks {
		if len(ch.Replicas) != 2 || ch.Replicas[0] == ch.Replicas[1] {
			t.Errorf("chunk %d replicas %v", ch.Index, ch.Replicas)
		}
	}

	var buf bytes.Buffer
	d, err := cl.Download(ctx, info.ObjectID, &buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), data) || d.Name != "report.pdf" || d.SHA256 != info.SHA256 || d.ObjectID != info.ObjectID {
		t.Fatalf("download: %+v", d)
	}

	status := "reviewed"
	got, err := cl.PatchMetadata(ctx, info.ObjectID, map[string]*string{"status": &status, "owner": nil})
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata["status"] != "reviewed" || len(got.Metadata) != 1 {
		t.Fatalf("patch: %v", got.Metadata)
	}
	if m, err := cl.Metadata(ctx, info.ObjectID); err != nil || m.Metadata["status"] != "reviewed" {
		t.Fatalf("metadata: %+v %v", m.Metadata, err)
	}

	list, err := cl.List(ctx, 10, 0)
	if err != nil || list.Total != 1 || list.Objects[0].ObjectID != info.ObjectID {
		t.Fatalf("list: %+v %v", list, err)
	}
	ws, err := cl.Workers(ctx)
	if err != nil || len(ws.Workers) != 4 {
		t.Fatalf("workers: %+v %v", ws, err)
	}
	inv, err := cl.WorkerChunks(ctx, info.Chunks[0].Replicas[0])
	if err != nil || inv.ChunkCount == 0 {
		t.Fatalf("worker chunks: %+v %v", inv, err)
	}
	rs, err := cl.Replication(ctx)
	if err != nil || !rs.Maintained || rs.Chunks != 6 {
		t.Fatalf("replication: %+v %v", rs, err)
	}

	if err := cl.Delete(ctx, info.ObjectID); err != nil {
		t.Fatal(err)
	}
	if _, err := cl.Metadata(ctx, info.ObjectID); !IsNotFound(err) {
		t.Fatalf("metadata after delete: %v", err)
	}
	if _, err := cl.Download(ctx, info.ObjectID, io.Discard); !IsNotFound(err) {
		t.Fatalf("download after delete: %v", err)
	}
	var e *Error
	if err := cl.Delete(ctx, "not-an-id"); !errors.As(err, &e) || e.Code != api.CodeNotFound {
		t.Fatalf("delete unknown: %v", err)
	}
}

func TestDownloadDetectsMismatch(t *testing.T) {
	// A server that sends the wrong bytes for the advertised checksum.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"`+strings.Repeat("0", 64)+`"`)
		w.Header().Set("Content-Length", "5")
		io.WriteString(w, "hello")
	}))
	defer srv.Close()
	cl := &Client{BaseURL: srv.URL}
	if _, err := cl.Download(context.Background(), "x", io.Discard); !errors.Is(err, ErrIntegrity) {
		t.Fatalf("got %v, want ErrIntegrity", err)
	}
}

func TestUploadErrorReported(t *testing.T) {
	cl := newSystem(t)
	_, err := cl.Upload(context.Background(), "a.txt", strings.NewReader("hi"), "", map[string]string{"": "x"})
	var e *Error
	if !errors.As(err, &e) || e.Status != http.StatusBadRequest {
		t.Fatalf("got %v, want a 400", err)
	}
}
