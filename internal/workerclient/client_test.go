package workerclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Dd1235/objstore/internal/worker"
	"github.com/Dd1235/objstore/internal/worker/store"
)

func TestClientAgainstWorker(t *testing.T) {
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer((&worker.Server{ID: "w1", Store: st, MaxChunk: 4096}).Handler())
	defer srv.Close()
	c := &Client{HTTP: srv.Client(), MaxChunk: 4096}
	ctx := context.Background()

	data := bytes.Repeat([]byte("ab"), 1000)
	sum := sha256.Sum256(data)
	if err := c.Put(ctx, srv.URL, "o_000000", data, sum); err != nil {
		t.Fatal(err)
	}
	if err := c.Put(ctx, srv.URL, "o_000000", data, sum); err != nil {
		t.Fatalf("idempotent Put: %v", err)
	}
	other := []byte("other")
	if err := c.Put(ctx, srv.URL, "o_000000", other, sha256.Sum256(other)); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Put: %v", err)
	}
	if err := c.Put(ctx, srv.URL, "o_000001", data, sha256.Sum256(other)); err == nil {
		t.Fatal("Put with wrong checksum succeeded")
	}

	got, err := c.Get(ctx, srv.URL, "o_000000")
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("Get: %v", err)
	}
	if _, err := c.Get(ctx, srv.URL, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: %v", err)
	}

	inv, err := c.Inventory(ctx, srv.URL, "w1")
	if err != nil || inv.ChunkCount != 1 || inv.Chunks[0].ChunkID != "o_000000" {
		t.Fatalf("Inventory = %+v, %v", inv, err)
	}

	raw, _ := os.ReadFile(st.Path("o_000000"))
	raw[len(raw)-1] ^= 1
	os.WriteFile(st.Path("o_000000"), raw, 0o644)
	if _, err := c.Get(ctx, srv.URL, "o_000000"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get corrupt: %v", err)
	}

	if err := c.Delete(ctx, srv.URL, "o_000000"); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, srv.URL, "o_000000"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete: %v", err)
	}
}

func TestClientUnreachableWorker(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	c := &Client{HTTP: &http.Client{}}
	if _, err := c.Get(context.Background(), url, "x"); err == nil {
		t.Fatal("Get from a closed server succeeded")
	}
}
