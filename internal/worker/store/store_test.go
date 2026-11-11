package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func open(t *testing.T, root string) *Dir {
	t.Helper()
	d, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func put(t *testing.T, d *Dir, id string, data []byte) {
	t.Helper()
	if _, err := d.Put(id, data, sha256.Sum256(data)); err != nil {
		t.Fatalf("Put(%s): %v", id, err)
	}
}

// flipByte corrupts one byte of the chunk's data region on disk.
func flipByte(t *testing.T, d *Dir, id string, offset int) {
	t.Helper()
	path := d.Path(id)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[headerSize+offset] ^= 0xff
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestPutGetRoundTrip(t *testing.T) {
	d := open(t, t.TempDir())
	data := []byte("hello chunk")
	sum := sha256.Sum256(data)

	created, err := d.Put("obj_000000", data, sum)
	if err != nil || !created {
		t.Fatalf("first Put: created=%v err=%v", created, err)
	}
	created, err = d.Put("obj_000000", data, sum)
	if err != nil || created {
		t.Fatalf("second Put: created=%v err=%v, want idempotent", created, err)
	}

	got, info, err := d.Get("obj_000000")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) || info.Size != int64(len(data)) || info.SHA256 != sum || info.Corrupt {
		t.Fatalf("Get = %q, %+v", got, info)
	}
	if n, b := d.Stats(); n != 1 || b != int64(len(data)) {
		t.Fatalf("Stats = %d, %d", n, b)
	}
}

func TestEmptyChunk(t *testing.T) {
	d := open(t, t.TempDir())
	put(t, d, "empty", nil)
	got, _, err := d.Get("empty")
	if err != nil || len(got) != 0 {
		t.Fatalf("Get = %q, %v", got, err)
	}
}

func TestPutRejectsBadInput(t *testing.T) {
	d := open(t, t.TempDir())
	data := []byte("abc")
	if _, err := d.Put("x", data, sha256.Sum256([]byte("abd"))); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("wrong checksum: got %v", err)
	}
	for _, id := range []string{"", "../escape", "a/b", ".hidden"} {
		if _, err := d.Put(id, data, sha256.Sum256(data)); !errors.Is(err, ErrInvalidID) {
			t.Fatalf("Put(%q): got %v, want ErrInvalidID", id, err)
		}
		if _, _, err := d.Get(id); !errors.Is(err, ErrInvalidID) {
			t.Fatalf("Get(%q): got %v, want ErrInvalidID", id, err)
		}
		if err := d.Delete(id); !errors.Is(err, ErrInvalidID) {
			t.Fatalf("Delete(%q): got %v, want ErrInvalidID", id, err)
		}
	}
	if entries, _ := os.ReadDir(d.tmpDir); len(entries) != 0 {
		t.Fatalf("tmp dir not empty: %v", entries)
	}
}

func TestPutConflict(t *testing.T) {
	d := open(t, t.TempDir())
	put(t, d, "c", []byte("one"))
	other := []byte("two")
	if _, err := d.Put("c", other, sha256.Sum256(other)); !errors.Is(err, ErrConflict) {
		t.Fatalf("got %v, want ErrConflict", err)
	}
	got, _, _ := d.Get("c")
	if string(got) != "one" {
		t.Fatalf("original overwritten: %q", got)
	}
}

func TestGetMissing(t *testing.T) {
	d := open(t, t.TempDir())
	if _, _, err := d.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("got %v, want ErrNotFound", err)
	}
}

func TestCorruptionDetectedAndRepaired(t *testing.T) {
	d := open(t, t.TempDir())
	data := bytes.Repeat([]byte("x"), 4096)
	put(t, d, "c", data)
	flipByte(t, d, "c", 100)

	if _, info, err := d.Get("c"); !errors.Is(err, ErrCorrupt) || !info.Corrupt {
		t.Fatalf("Get after corruption: info=%+v err=%v", info, err)
	}
	if l := d.List(); len(l) != 1 || !l[0].Corrupt {
		t.Fatalf("List = %+v, want one corrupt entry", l)
	}

	// Writing a good copy replaces the corrupt one.
	created, err := d.Put("c", data, sha256.Sum256(data))
	if err != nil || !created {
		t.Fatalf("repair Put: created=%v err=%v", created, err)
	}
	got, info, err := d.Get("c")
	if err != nil || info.Corrupt || !bytes.Equal(got, data) {
		t.Fatalf("after repair: info=%+v err=%v", info, err)
	}
}

func TestPutOverUndetectedCorruption(t *testing.T) {
	// The index still says the chunk is fine, but the bytes on disk are bad.
	d := open(t, t.TempDir())
	data := []byte("some chunk data")
	put(t, d, "c", data)
	flipByte(t, d, "c", 0)

	created, err := d.Put("c", data, sha256.Sum256(data))
	if err != nil || !created {
		t.Fatalf("Put: created=%v err=%v, want overwrite", created, err)
	}
	if _, _, err := d.Get("c"); err != nil {
		t.Fatal(err)
	}
}

func TestDelete(t *testing.T) {
	d := open(t, t.TempDir())
	put(t, d, "c", []byte("data"))
	if err := d.Delete("c"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := d.Get("c"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after delete: %v", err)
	}
	if err := d.Delete("c"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete: %v", err)
	}
	if n, _ := d.Stats(); n != 0 {
		t.Fatalf("Stats count = %d", n)
	}
}

func TestReopen(t *testing.T) {
	root := t.TempDir()
	d := open(t, root)
	put(t, d, "a", []byte("alpha"))
	put(t, d, "b", []byte("bravo!"))
	put(t, d, "bad-header", []byte("charlie"))
	put(t, d, "truncated", []byte("delta delta"))

	// Damage two files and leave junk behind, as a crash might.
	raw, _ := os.ReadFile(d.Path("bad-header"))
	copy(raw, "XXXX")
	os.WriteFile(d.Path("bad-header"), raw, 0o644)
	raw, _ = os.ReadFile(d.Path("truncated"))
	os.WriteFile(d.Path("truncated"), raw[:len(raw)-3], 0o644)
	os.WriteFile(filepath.Join(root, "tmp", "a.123"), []byte("partial"), 0o644)
	os.WriteFile(filepath.Join(root, "chunks", ".DS_Store"), nil, 0o644)

	d2 := open(t, root)
	if entries, _ := os.ReadDir(filepath.Join(root, "tmp")); len(entries) != 0 {
		t.Fatalf("tmp not cleaned: %v", entries)
	}
	got := map[string]bool{}
	for _, info := range d2.List() {
		got[info.ID] = info.Corrupt
	}
	want := map[string]bool{"a": false, "b": false, "bad-header": true, "truncated": true}
	if len(got) != len(want) {
		t.Fatalf("List = %v, want %v", got, want)
	}
	for id, corrupt := range want {
		if c, ok := got[id]; !ok || c != corrupt {
			t.Fatalf("chunk %s: corrupt=%v present=%v, want corrupt=%v", id, c, ok, corrupt)
		}
	}
	if data, _, err := d2.Get("b"); err != nil || string(data) != "bravo!" {
		t.Fatalf("Get(b) = %q, %v", data, err)
	}
}

func TestScrub(t *testing.T) {
	d := open(t, t.TempDir())
	for _, id := range []string{"a", "b", "c", "d"} {
		put(t, d, id, []byte("payload-"+id))
	}
	flipByte(t, d, "b", 1)
	flipByte(t, d, "d", 2)

	checked, corrupt, err := Scrub(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	if checked != 4 || len(corrupt) != 2 || corrupt[0] != "b" || corrupt[1] != "d" {
		t.Fatalf("Scrub = %d, %v", checked, corrupt)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := Scrub(ctx, d); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Scrub: %v", err)
	}
}

func TestConcurrentAccess(t *testing.T) {
	d := open(t, t.TempDir())
	data := bytes.Repeat([]byte("z"), 1<<16)
	sum := sha256.Sum256(data)
	var wg sync.WaitGroup
	for i := range 16 {
		wg.Go(func() {
			if _, err := d.Put("shared", data, sum); err != nil {
				t.Errorf("Put: %v", err)
			}
			if i%2 == 0 {
				if got, _, err := d.Get("shared"); err != nil || !bytes.Equal(got, data) {
					t.Errorf("Get: %v", err)
				}
			}
			d.List()
			d.Stats()
		})
	}
	wg.Wait()
	if n, _ := d.Stats(); n != 1 {
		t.Fatalf("Stats count = %d", n)
	}
}
