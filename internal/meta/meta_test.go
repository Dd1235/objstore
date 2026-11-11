package meta

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func sample(id string) Object {
	t := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	return Object{
		ID: id, Name: "photo.jpg", Size: 3, ContentType: "image/jpeg", SHA256: "abc",
		State: Committed, StartedAt: t, CreatedAt: t, ModifiedAt: t, ChunkSize: 1 << 20,
		Chunks: []Chunk{{Index: 0, ID: id + "_000000", Size: 3, SHA256: "abc", Replicas: []string{"w1", "w2"}}},
		Custom: map[string]string{"owner": "alice"},
	}
}

// testStore runs the behaviour every Store must have.
func testStore(t *testing.T, s Store) {
	if _, err := s.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing: %v", err)
	}
	if err := s.Create(sample("b")); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(sample("a")); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(sample("a")); !errors.Is(err, ErrExists) {
		t.Fatalf("duplicate Create: %v", err)
	}

	got, err := s.Get("a")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "photo.jpg" || got.Custom["owner"] != "alice" || got.Chunks[0].Replicas[1] != "w2" ||
		!got.CreatedAt.Equal(sample("a").CreatedAt) {
		t.Fatalf("Get = %+v", got)
	}

	// Mutating a returned copy must not change the store.
	got.Custom["owner"] = "mallory"
	got.Chunks[0].Replicas[0] = "w9"
	again, _ := s.Get("a")
	if again.Custom["owner"] != "alice" || again.Chunks[0].Replicas[0] != "w1" {
		t.Fatal("store aliased a returned value")
	}

	upd, err := s.Update("a", func(o *Object) error {
		o.Custom["team"] = "storage"
		o.ID = "ignored"
		return nil
	})
	if err != nil || upd.ID != "a" || upd.Custom["team"] != "storage" {
		t.Fatalf("Update = %+v, %v", upd, err)
	}
	if o, _ := s.Get("a"); o.Custom["team"] != "storage" {
		t.Fatal("update not stored")
	}

	boom := errors.New("boom")
	if _, err := s.Update("a", func(o *Object) error { o.Name = "changed"; return boom }); !errors.Is(err, boom) {
		t.Fatalf("Update error: %v", err)
	}
	if o, _ := s.Get("a"); o.Name != "photo.jpg" {
		t.Fatal("failed update was written")
	}
	if _, err := s.Update("zzz", func(*Object) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Update missing: %v", err)
	}

	list, err := s.List()
	if err != nil || len(list) != 2 || list[0].ID != "a" || list[1].ID != "b" {
		t.Fatalf("List = %v, %v", list, err)
	}

	if err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete: %v", err)
	}
	if list, _ := s.List(); len(list) != 1 {
		t.Fatalf("List after delete = %v", list)
	}
}

func TestMem(t *testing.T) { testStore(t, NewMem()) }

func TestBolt(t *testing.T) {
	s, err := OpenBolt(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	testStore(t, s)
}

func TestBoltSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	s, err := OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Create(sample("x")); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s, err = OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	o, err := s.Get("x")
	if err != nil || o.Custom["owner"] != "alice" || o.State != Committed {
		t.Fatalf("after reopen: %+v, %v", o, err)
	}
}

func TestBoltLockedByAnotherHandle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	s, err := OpenBolt(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := OpenBolt(path); err == nil {
		t.Fatal("second open of a locked database succeeded")
	}
}
