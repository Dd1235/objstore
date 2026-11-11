package worker

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/worker/store"
)

func newTestServer(t *testing.T) (*Server, *store.Dir) {
	t.Helper()
	st, err := store.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return &Server{ID: "worker1", Store: st, MaxChunk: 1024}, st
}

func do(t *testing.T, h http.Handler, method, path string, body []byte, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func sumHeader(data []byte) map[string]string {
	s := sha256.Sum256(data)
	return map[string]string{api.HeaderChunkSHA256: hex.EncodeToString(s[:])}
}

func errCode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var e api.Error
	if err := json.Unmarshal(rec.Body.Bytes(), &e); err != nil {
		t.Fatalf("error body %q: %v", rec.Body.String(), err)
	}
	return e.Code
}

func TestChunkLifecycle(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	data := []byte("chunk bytes")

	if rec := do(t, h, "PUT", "/internal/chunks/o_000000", data, sumHeader(data)); rec.Code != http.StatusCreated {
		t.Fatalf("PUT: %d %s", rec.Code, rec.Body)
	}
	if rec := do(t, h, "PUT", "/internal/chunks/o_000000", data, sumHeader(data)); rec.Code != http.StatusOK {
		t.Fatalf("repeat PUT: %d, want 200", rec.Code)
	}

	rec := do(t, h, "GET", "/internal/chunks/o_000000", nil, nil)
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("GET: %d %q", rec.Code, rec.Body)
	}
	if got, want := rec.Header().Get(api.HeaderChunkSHA256), sumHeader(data)[api.HeaderChunkSHA256]; got != want {
		t.Fatalf("checksum header %q, want %q", got, want)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("Content-Type %q", ct)
	}

	rec = do(t, h, "GET", "/internal/workers/worker1/chunks", nil, nil)
	var inv api.ChunkInventory
	if err := json.Unmarshal(rec.Body.Bytes(), &inv); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("inventory: %d %v", rec.Code, err)
	}
	if inv.WorkerID != "worker1" || inv.ChunkCount != 1 || inv.UsedBytes != int64(len(data)) || inv.Chunks[0].ChunkID != "o_000000" {
		t.Fatalf("inventory = %+v", inv)
	}

	if rec := do(t, h, "DELETE", "/internal/chunks/o_000000", nil, nil); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE: %d", rec.Code)
	}
	if rec := do(t, h, "GET", "/internal/chunks/o_000000", nil, nil); rec.Code != http.StatusNotFound || errCode(t, rec) != api.CodeNotFound {
		t.Fatalf("GET after delete: %d", rec.Code)
	}
	if rec := do(t, h, "DELETE", "/internal/chunks/o_000000", nil, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("second DELETE: %d", rec.Code)
	}
}

func TestPutErrors(t *testing.T) {
	srv, _ := newTestServer(t)
	h := srv.Handler()
	data := []byte("abc")

	cases := []struct {
		name   string
		path   string
		body   []byte
		hdr    map[string]string
		status int
		code   string
	}{
		{"missing checksum", "/internal/chunks/a", data, nil, 400, api.CodeBadRequest},
		{"malformed checksum", "/internal/chunks/a", data, map[string]string{api.HeaderChunkSHA256: "zz"}, 400, api.CodeBadRequest},
		{"wrong checksum", "/internal/chunks/a", data, sumHeader([]byte("abd")), 400, api.CodeChecksumMismatch},
		{"bad id", "/internal/chunks/.hidden", data, sumHeader(data), 400, api.CodeBadRequest},
		{"too large", "/internal/chunks/a", make([]byte, 1025), sumHeader(make([]byte, 1025)), 413, api.CodeTooLarge},
	}
	for _, c := range cases {
		rec := do(t, h, "PUT", c.path, c.body, c.hdr)
		if rec.Code != c.status || errCode(t, rec) != c.code {
			t.Errorf("%s: got %d %s, want %d %s", c.name, rec.Code, rec.Body, c.status, c.code)
		}
	}

	do(t, h, "PUT", "/internal/chunks/a", data, sumHeader(data))
	other := []byte("xyz")
	if rec := do(t, h, "PUT", "/internal/chunks/a", other, sumHeader(other)); rec.Code != http.StatusConflict {
		t.Errorf("conflicting PUT: %d", rec.Code)
	}
}

func TestGetCorruptChunk(t *testing.T) {
	srv, st := newTestServer(t)
	h := srv.Handler()
	data := []byte("precious data")
	do(t, h, "PUT", "/internal/chunks/c", data, sumHeader(data))

	raw, _ := os.ReadFile(st.Path("c"))
	raw[len(raw)-1] ^= 0x01
	os.WriteFile(st.Path("c"), raw, 0o644)

	rec := do(t, h, "GET", "/internal/chunks/c", nil, nil)
	if rec.Code != http.StatusInternalServerError || errCode(t, rec) != api.CodeChunkCorrupt {
		t.Fatalf("GET corrupt: %d %s", rec.Code, rec.Body)
	}
	if bytes.Contains(rec.Body.Bytes(), raw[len(raw)-5:]) {
		t.Fatal("response leaked corrupt bytes")
	}

	var inv api.ChunkInventory
	json.Unmarshal(do(t, h, "GET", "/internal/workers/worker1/chunks", nil, nil).Body.Bytes(), &inv)
	if len(inv.Chunks) != 1 || !inv.Chunks[0].Corrupt {
		t.Fatalf("inventory = %+v, want chunk marked corrupt", inv)
	}
}

func TestInventoryWrongWorker(t *testing.T) {
	srv, _ := newTestServer(t)
	if rec := do(t, srv.Handler(), "GET", "/internal/workers/worker9/chunks", nil, nil); rec.Code != http.StatusNotFound {
		t.Fatalf("got %d, want 404", rec.Code)
	}
}
