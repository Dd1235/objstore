package controller

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/membership"
	"github.com/Dd1235/objstore/internal/meta"
	"github.com/Dd1235/objstore/internal/worker"
	"github.com/Dd1235/objstore/internal/worker/store"
	"github.com/Dd1235/objstore/internal/workerclient"
)

const testChunk = 1024

// cluster is a controller plus in-process workers with real on-disk stores.
// Worker HTTP calls are served in memory; a worker can be taken "down" so
// its calls fail as if the container were stopped.
type cluster struct {
	t       *testing.T
	c       *Controller
	h       http.Handler
	clk     *fakeClock
	stores  map[string]*store.Dir
	workers map[string]http.Handler

	mu   sync.Mutex
	down map[string]bool
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	cl := &cluster{
		t: t, clk: &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
		stores: map[string]*store.Dir{}, workers: map[string]http.Handler{}, down: map[string]bool{},
	}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("worker%d", i)
		st, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		cl.stores[id] = st
		cl.workers[id] = (&worker.Server{ID: id, Store: st, MaxChunk: testChunk, Log: slog.New(slog.DiscardHandler)}).Handler()
	}
	cfg := DefaultConfig()
	cfg.ChunkSize = testChunk
	c, err := New(cfg, Deps{
		Meta:   meta.NewMem(),
		Chunks: &workerclient.Client{HTTP: &http.Client{Transport: cl}, MaxChunk: testChunk},
		Log:    slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	c.now = cl.clk.now
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("worker%d", i)
		c.members.Register(cl.clk.now(), id, "http://"+id+":8080", membership.Stats{})
	}
	t.Cleanup(c.Close)
	cl.c, cl.h = c, c.Handler()
	return cl
}

func (cl *cluster) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Body != nil {
		defer r.Body.Close()
	}
	host := r.URL.Hostname()
	cl.mu.Lock()
	h, ok := cl.workers[host]
	down := cl.down[host]
	cl.mu.Unlock()
	if !ok || down {
		return nil, fmt.Errorf("dial %s: connection refused", host)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, r)
	return rec.Result(), nil
}

func (cl *cluster) setDown(id string, down bool) {
	cl.mu.Lock()
	cl.down[id] = down
	cl.mu.Unlock()
}

// wait blocks until background work (clean-up, repair) has finished.
func (cl *cluster) wait() { cl.c.bg.Wait() }

type part struct {
	field, filename, ctype string
	body                   []byte
}

func (cl *cluster) post(parts ...part) *httptest.ResponseRecorder {
	cl.t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	for _, p := range parts {
		hdr := textproto.MIMEHeader{}
		cd := fmt.Sprintf(`form-data; name=%q`, p.field)
		if p.filename != "" {
			cd += fmt.Sprintf(`; filename=%q`, p.filename)
		}
		hdr.Set("Content-Disposition", cd)
		if p.ctype != "" {
			hdr.Set("Content-Type", p.ctype)
		}
		w, _ := mw.CreatePart(hdr)
		w.Write(p.body)
	}
	mw.Close()
	req := httptest.NewRequest("POST", "/api/v1/objects", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	rec := httptest.NewRecorder()
	cl.h.ServeHTTP(rec, req)
	return rec
}

func (cl *cluster) upload(name string, data []byte, metadata string) api.ObjectInfo {
	cl.t.Helper()
	parts := []part{}
	if metadata != "" {
		parts = append(parts, part{field: "metadata", body: []byte(metadata)})
	}
	parts = append(parts, part{field: "file", filename: name, body: data})
	rec := cl.post(parts...)
	if rec.Code != http.StatusCreated {
		cl.t.Fatalf("upload %s: %d %s", name, rec.Code, rec.Body)
	}
	var info api.ObjectInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		cl.t.Fatal(err)
	}
	return info
}

func (cl *cluster) do(method, path, body string) *httptest.ResponseRecorder {
	cl.t.Helper()
	rec := httptest.NewRecorder()
	cl.h.ServeHTTP(rec, httptest.NewRequest(method, path, strings.NewReader(body)))
	return rec
}

func decode[T any](t *testing.T, rec *httptest.ResponseRecorder) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return v
}

func randomData(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func corruptFile(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	cl := newCluster(t, 4)
	data := randomData(3*testChunk+512, 1)
	rec := cl.post(
		part{field: "metadata", body: []byte(`{"owner":"alice","project":"demo"}`)},
		part{field: "file", filename: "photo.png", ctype: "image/png", body: data},
	)
	if rec.Code != http.StatusCreated {
		t.Fatalf("upload: %d %s", rec.Code, rec.Body)
	}
	info := decode[api.ObjectInfo](t, rec)
	if rec.Header().Get("Location") != "/api/v1/objects/"+info.ObjectID {
		t.Fatalf("Location = %q", rec.Header().Get("Location"))
	}
	sum := sha256.Sum256(data)
	if info.Name != "photo.png" || info.Size != int64(len(data)) || info.ContentType != "image/png" ||
		info.SHA256 != hex.EncodeToString(sum[:]) || info.ChunkCount != 4 || info.ChunkSize != testChunk ||
		info.Metadata["owner"] != "alice" || !info.CreatedAt.Equal(info.ModifiedAt) {
		t.Fatalf("info = %+v", info)
	}

	// Every chunk has two copies on distinct workers, and each copy holds the right bytes.
	used := map[string]bool{}
	for i, ch := range info.Chunks {
		want := data[i*testChunk : min((i+1)*testChunk, len(data))]
		if ch.Index != i || ch.Size != int64(len(want)) || len(ch.Replicas) != 2 || ch.Replicas[0] == ch.Replicas[1] {
			t.Fatalf("chunk %d = %+v", i, ch)
		}
		for _, w := range ch.Replicas {
			got, _, err := cl.stores[w].Get(ch.ChunkID)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("chunk %d on %s: %v", i, w, err)
			}
			used[w] = true
		}
	}
	if len(used) < 3 {
		t.Fatalf("chunks spread over only %v", used)
	}

	rec = cl.do("GET", "/api/v1/objects/"+info.ObjectID, "")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("download: %d, %d bytes", rec.Code, rec.Body.Len())
	}
	h := rec.Header()
	if h.Get("Content-Type") != "image/png" || h.Get("Content-Length") != fmt.Sprint(len(data)) ||
		h.Get("ETag") != `"`+info.SHA256+`"` || !strings.Contains(h.Get("Content-Disposition"), "photo.png") {
		t.Fatalf("headers = %v", h)
	}

	rec = cl.do("HEAD", "/api/v1/objects/"+info.ObjectID, "")
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 || rec.Header().Get("Content-Length") != fmt.Sprint(len(data)) {
		t.Fatalf("HEAD: %d %v", rec.Code, rec.Header())
	}

	rec = cl.do("GET", "/api/v1/objects/"+info.ObjectID+"/metadata", "")
	if got := decode[api.ObjectInfo](t, rec); rec.Code != http.StatusOK || got.SHA256 != info.SHA256 || len(got.Chunks) != 4 {
		t.Fatalf("metadata: %d %+v", rec.Code, got)
	}
}

func TestContentTypeDetection(t *testing.T) {
	cl := newCluster(t, 2)
	cases := []struct {
		name, declared string
		body           []byte
		want           string
	}{
		{"report", "", []byte("%PDF-1.7 rest of file"), "application/pdf"},
		{"data.json", "application/octet-stream", []byte(`{"a":1}`), "application/json"},
		{"clip", "video/mp4", []byte("anything"), "video/mp4"},
	}
	for _, c := range cases {
		rec := cl.post(part{field: "file", filename: c.name, ctype: c.declared, body: c.body})
		if got := decode[api.ObjectInfo](t, rec).ContentType; got != c.want {
			t.Errorf("%s: content type %q, want %q", c.name, got, c.want)
		}
	}
}

func TestEmptyObject(t *testing.T) {
	cl := newCluster(t, 4)
	info := cl.upload("empty.txt", nil, "")
	if info.Size != 0 || info.ChunkCount != 0 {
		t.Fatalf("info = %+v", info)
	}
	rec := cl.do("GET", "/api/v1/objects/"+info.ObjectID, "")
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 || rec.Header().Get("Content-Length") != "0" {
		t.Fatalf("download: %d %q", rec.Code, rec.Body)
	}
}

func TestUnknownObject(t *testing.T) {
	cl := newCluster(t, 2)
	id := "0b9f6c1e-2a7d-4c4e-9d0e-5c3a1f2b7e10"
	for _, req := range [][2]string{
		{"GET", "/api/v1/objects/" + id},
		{"GET", "/api/v1/objects/" + id + "/metadata"},
		{"PATCH", "/api/v1/objects/" + id + "/metadata"},
		{"DELETE", "/api/v1/objects/" + id},
	} {
		rec := cl.do(req[0], req[1], `{"a":"b"}`)
		if rec.Code != http.StatusNotFound || decode[api.Error](t, rec).Code != api.CodeNotFound {
			t.Errorf("%s %s: %d %s", req[0], req[1], rec.Code, rec.Body)
		}
	}
}

func TestPatchMetadata(t *testing.T) {
	cl := newCluster(t, 4)
	info := cl.upload("a.txt", []byte("hello"), `{"a":"1","b":"2"}`)
	path := "/api/v1/objects/" + info.ObjectID + "/metadata"

	cl.clk.advance(time.Minute)
	rec := cl.do("PATCH", path, `{"b":"3","c":"4","a":null}`)
	got := decode[api.ObjectInfo](t, rec)
	if rec.Code != http.StatusOK || len(got.Metadata) != 2 || got.Metadata["b"] != "3" || got.Metadata["c"] != "4" {
		t.Fatalf("PATCH: %d %+v", rec.Code, got.Metadata)
	}
	if !got.ModifiedAt.After(got.CreatedAt) || got.Chunks[0].ChunkID != info.Chunks[0].ChunkID {
		t.Fatalf("after PATCH: %+v", got)
	}
	if again := decode[api.ObjectInfo](t, cl.do("GET", path, "")); again.Metadata["c"] != "4" {
		t.Fatal("PATCH not persisted")
	}

	// A no-op PATCH leaves the modification time alone.
	cl.clk.advance(time.Minute)
	if noop := decode[api.ObjectInfo](t, cl.do("PATCH", path, `{"b":"3"}`)); !noop.ModifiedAt.Equal(got.ModifiedAt) {
		t.Fatal("no-op PATCH changed modified_at")
	}

	for _, body := range []string{"", "not json", "[1]", `{"a":1}`, `{"":"x"}`, `{"k":` + strings.Repeat(`"`, 1) + strings.Repeat("v", maxMetaValueLen+1) + `"}`} {
		if rec := cl.do("PATCH", path, body); rec.Code != http.StatusBadRequest {
			t.Errorf("PATCH %.40q: %d, want 400", body, rec.Code)
		}
	}
}

func TestListObjects(t *testing.T) {
	cl := newCluster(t, 4)
	var ids []string
	for i := range 3 {
		cl.clk.advance(time.Second)
		ids = append(ids, cl.upload(fmt.Sprintf("f%d.bin", i), randomData(100*(i+1), uint64(i)), "").ObjectID)
	}
	list := decode[api.ObjectList](t, cl.do("GET", "/api/v1/objects", ""))
	if list.Total != 3 || len(list.Objects) != 3 || list.NextOffset != nil {
		t.Fatalf("list = %+v", list)
	}
	for i, o := range list.Objects {
		if o.ObjectID != ids[i] || o.Name != fmt.Sprintf("f%d.bin", i) || o.Size != int64(100*(i+1)) || o.ContentType == "" {
			t.Fatalf("entry %d = %+v", i, o)
		}
	}

	page := decode[api.ObjectList](t, cl.do("GET", "/api/v1/objects?limit=2", ""))
	if len(page.Objects) != 2 || page.NextOffset == nil || *page.NextOffset != 2 {
		t.Fatalf("page 1 = %+v", page)
	}
	page = decode[api.ObjectList](t, cl.do("GET", "/api/v1/objects?limit=2&offset=2", ""))
	if len(page.Objects) != 1 || page.Objects[0].ObjectID != ids[2] || page.NextOffset != nil {
		t.Fatalf("page 2 = %+v", page)
	}
	for _, q := range []string{"limit=0", "limit=x", "offset=-1", "limit=5000"} {
		if rec := cl.do("GET", "/api/v1/objects?"+q, ""); rec.Code != http.StatusBadRequest {
			t.Errorf("?%s: %d", q, rec.Code)
		}
	}
}

func chunkCount(cl *cluster) int {
	n := 0
	for _, st := range cl.stores {
		c, _ := st.Stats()
		n += c
	}
	return n
}

func TestDeleteObject(t *testing.T) {
	cl := newCluster(t, 4)
	keep := cl.upload("keep.bin", randomData(2*testChunk, 1), "")
	gone := cl.upload("gone.bin", randomData(3*testChunk, 2), "")
	if n := chunkCount(cl); n != 10 {
		t.Fatalf("%d chunk copies stored, want 10", n)
	}

	if rec := cl.do("DELETE", "/api/v1/objects/"+gone.ObjectID, ""); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE: %d %s", rec.Code, rec.Body)
	}
	if rec := cl.do("GET", "/api/v1/objects/"+gone.ObjectID, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET after delete: %d", rec.Code)
	}
	if rec := cl.do("DELETE", "/api/v1/objects/"+gone.ObjectID, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("second DELETE: %d", rec.Code)
	}
	cl.wait()
	if n := chunkCount(cl); n != 4 {
		t.Fatalf("%d chunk copies left, want 4", n)
	}
	if _, err := cl.c.meta.Get(gone.ObjectID); !errors.Is(err, meta.ErrNotFound) {
		t.Fatalf("tombstone not removed: %v", err)
	}
	list := decode[api.ObjectList](t, cl.do("GET", "/api/v1/objects", ""))
	if list.Total != 1 || list.Objects[0].ObjectID != keep.ObjectID {
		t.Fatalf("list after delete = %+v", list)
	}
}

func TestDeleteWaitsForDownloads(t *testing.T) {
	cl := newCluster(t, 4)
	info := cl.upload("big.bin", randomData(2*testChunk, 3), "")

	// A download is in progress when the delete arrives.
	if _, err := cl.c.acquire(info.ObjectID); err != nil {
		t.Fatal(err)
	}
	if rec := cl.do("DELETE", "/api/v1/objects/"+info.ObjectID, ""); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE: %d", rec.Code)
	}
	if rec := cl.do("GET", "/api/v1/objects/"+info.ObjectID, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("new download after delete: %d", rec.Code)
	}
	cl.wait()
	if n := chunkCount(cl); n != 4 {
		t.Fatalf("chunks removed while a download was running: %d left", n)
	}

	cl.c.release(info.ObjectID)
	cl.wait()
	if n := chunkCount(cl); n != 0 {
		t.Fatalf("%d chunk copies left after the download finished", n)
	}
}

func TestDeleteWithWorkerDownKeepsTombstone(t *testing.T) {
	cl := newCluster(t, 4)
	info := cl.upload("x.bin", randomData(testChunk, 4), "")
	down := info.Chunks[0].Replicas[0]
	cl.setDown(down, true)

	if rec := cl.do("DELETE", "/api/v1/objects/"+info.ObjectID, ""); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE: %d", rec.Code)
	}
	cl.wait()
	o, err := cl.c.meta.Get(info.ObjectID)
	if err != nil || o.State != meta.Deleted {
		t.Fatalf("tombstone = %+v, %v", o, err)
	}
	if rec := cl.do("GET", "/api/v1/objects/"+info.ObjectID+"/metadata", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("tombstoned object visible: %d", rec.Code)
	}
}

func TestDownloadSurvivesWorkerFailure(t *testing.T) {
	cl := newCluster(t, 4)
	data := randomData(6*testChunk, 5)
	info := cl.upload("v.bin", data, "")

	cl.setDown("worker2", true)
	rec := cl.do("GET", "/api/v1/objects/"+info.ObjectID, "")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("download with worker2 unreachable: %d", rec.Code)
	}

	// Once worker2 is declared dead it is not even tried.
	cl.clk.advance(30 * time.Second)
	for _, id := range []string{"worker1", "worker3", "worker4"} {
		cl.c.members.Heartbeat(cl.clk.now(), id, membership.Stats{})
	}
	cl.c.Tick()
	if w, _ := cl.c.members.Get("worker2"); w.State != membership.Dead {
		t.Fatalf("worker2 = %v", w.State)
	}
	rec = cl.do("GET", "/api/v1/objects/"+info.ObjectID, "")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("download with worker2 dead: %d", rec.Code)
	}
}

func TestDownloadFailsWhenNoCopyReachable(t *testing.T) {
	cl := newCluster(t, 4)
	info := cl.upload("v.bin", randomData(testChunk, 6), "")
	for _, w := range info.Chunks[0].Replicas {
		cl.setDown(w, true)
	}
	rec := cl.do("GET", "/api/v1/objects/"+info.ObjectID, "")
	if rec.Code != http.StatusServiceUnavailable || decode[api.Error](t, rec).Code != api.CodeUnavailable {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
}

func TestCorruptReplicaServedFromOtherCopyAndRepaired(t *testing.T) {
	cl := newCluster(t, 4)
	data := randomData(3*testChunk, 7)
	info := cl.upload("c.bin", data, "")
	ch := info.Chunks[1]
	bad := ch.Replicas[0] // read first, since all workers are alive
	corruptFile(t, cl.stores[bad].Path(ch.ChunkID))

	rec := cl.do("GET", "/api/v1/objects/"+info.ObjectID, "")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("download with a corrupt replica: %d", rec.Code)
	}
	cl.wait()
	got, st, err := cl.stores[bad].Get(ch.ChunkID)
	if err != nil || st.Corrupt || !bytes.Equal(got, data[testChunk:2*testChunk]) {
		t.Fatalf("replica not repaired: %v %+v", err, st)
	}
}

func TestTamperedReplicaCaughtByController(t *testing.T) {
	// The worker's copy is self-consistent (header matches data) but is not
	// the data the controller recorded, so only the end-to-end check sees it.
	cl := newCluster(t, 4)
	data := randomData(2*testChunk, 8)
	info := cl.upload("t.bin", data, "")
	ch := info.Chunks[0]
	bad := ch.Replicas[0]
	st := cl.stores[bad]
	st.Delete(ch.ChunkID)
	fake := bytes.Repeat([]byte("x"), testChunk)
	if _, err := st.Put(ch.ChunkID, fake, sha256.Sum256(fake)); err != nil {
		t.Fatal(err)
	}

	rec := cl.do("GET", "/api/v1/objects/"+info.ObjectID, "")
	if rec.Code != http.StatusOK || !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("download: %d", rec.Code)
	}
	cl.wait()
	if got, _, err := st.Get(ch.ChunkID); err != nil || !bytes.Equal(got, data[:testChunk]) {
		t.Fatalf("tampered replica not replaced: %v", err)
	}
}

func TestMissingReplicaRepaired(t *testing.T) {
	cl := newCluster(t, 4)
	data := randomData(testChunk, 9)
	info := cl.upload("m.bin", data, "")
	ch := info.Chunks[0]
	st := cl.stores[ch.Replicas[0]]
	st.Delete(ch.ChunkID)

	if rec := cl.do("GET", "/api/v1/objects/"+info.ObjectID, ""); !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatalf("download: %d", rec.Code)
	}
	cl.wait()
	if _, _, err := st.Get(ch.ChunkID); err != nil {
		t.Fatalf("missing replica not restored: %v", err)
	}
}

func TestUploadSkipsFailingWorker(t *testing.T) {
	cl := newCluster(t, 4)
	cl.setDown("worker2", true) // still "alive" to membership, but every call fails
	data := randomData(8*testChunk, 10)
	info := cl.upload("f.bin", data, "")
	for _, ch := range info.Chunks {
		if len(ch.Replicas) != 2 || slices.Contains(ch.Replicas, "worker2") {
			t.Fatalf("chunk %d replicas %v", ch.Index, ch.Replicas)
		}
	}
	if rec := cl.do("GET", "/api/v1/objects/"+info.ObjectID, ""); !bytes.Equal(rec.Body.Bytes(), data) {
		t.Fatal("download mismatch")
	}
}

func TestUploadWithoutEnoughWorkers(t *testing.T) {
	cl := newCluster(t, 4)
	// Three workers fail every call: the first chunk gets only one copy.
	for _, id := range []string{"worker2", "worker3", "worker4"} {
		cl.setDown(id, true)
	}
	rec := cl.post(part{field: "file", filename: "f.bin", body: randomData(3*testChunk, 11)})
	if rec.Code != http.StatusServiceUnavailable || decode[api.Error](t, rec).Code != api.CodeUnavailable {
		t.Fatalf("got %d %s", rec.Code, rec.Body)
	}
	cl.wait()
	if n := chunkCount(cl); n != 0 {
		t.Fatalf("%d chunk copies left behind by the failed upload", n)
	}
	if all, _ := cl.c.meta.List(); len(all) != 0 {
		t.Fatalf("records left behind: %+v", all)
	}

	// With only one worker alive in membership the upload is refused up front.
	cl.clk.advance(30 * time.Second)
	cl.c.members.Heartbeat(cl.clk.now(), "worker1", membership.Stats{})
	cl.c.Tick()
	rec = cl.post(part{field: "file", filename: "f.bin", body: []byte("x")})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("got %d", rec.Code)
	}
}

func TestUploadValidation(t *testing.T) {
	cl := newCluster(t, 4)
	file := part{field: "file", filename: "f.bin", body: randomData(2*testChunk, 12)}
	cases := []struct {
		name  string
		parts []part
	}{
		{"no file", []part{{field: "metadata", body: []byte(`{"a":"b"}`)}}},
		{"metadata not an object", []part{{field: "metadata", body: []byte(`[1]`)}, file}},
		{"metadata value not a string", []part{{field: "metadata", body: []byte(`{"a":2}`)}, file}},
		{"bad metadata after file", []part{file, {field: "metadata", body: []byte(`nope`)}}},
		{"two files", []part{file, file}},
	}
	for _, c := range cases {
		if rec := cl.post(c.parts...); rec.Code != http.StatusBadRequest {
			t.Errorf("%s: %d %s", c.name, rec.Code, rec.Body)
		}
	}
	if rec := cl.do("POST", "/api/v1/objects", "plain body"); rec.Code != http.StatusBadRequest {
		t.Errorf("non-multipart: %d", rec.Code)
	}
	cl.wait()
	if n := chunkCount(cl); n != 0 {
		t.Fatalf("%d chunk copies left behind by rejected uploads", n)
	}
	if all, _ := cl.c.meta.List(); len(all) != 0 {
		t.Fatalf("records left behind: %d", len(all))
	}
}

func TestWorkerChunksProxy(t *testing.T) {
	cl := newCluster(t, 4)
	info := cl.upload("p.bin", randomData(testChunk, 13), "")
	w := info.Chunks[0].Replicas[0]
	rec := cl.do("GET", "/internal/workers/"+w+"/chunks", "")
	inv := decode[api.ChunkInventory](t, rec)
	if rec.Code != http.StatusOK || inv.WorkerID != w || inv.ChunkCount != 1 || inv.Chunks[0].ChunkID != info.Chunks[0].ChunkID {
		t.Fatalf("inventory: %d %+v", rec.Code, inv)
	}
	if rec := cl.do("GET", "/internal/workers/nobody/chunks", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown worker: %d", rec.Code)
	}
	cl.setDown(w, true)
	if rec := cl.do("GET", "/internal/workers/"+w+"/chunks", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("down worker: %d", rec.Code)
	}
}
