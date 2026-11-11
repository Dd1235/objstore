package ids

import (
	"bytes"
	"crypto/rand"
	"testing"
)

func TestNewObjectIDFormat(t *testing.T) {
	seen := make(map[string]bool)
	for range 1000 {
		id, err := NewObjectID(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if !ValidObjectID(id) {
			t.Fatalf("invalid id %q", id)
		}
		if id[14] != '4' {
			t.Fatalf("version nibble = %c, want 4 in %q", id[14], id)
		}
		if c := id[19]; c != '8' && c != '9' && c != 'a' && c != 'b' {
			t.Fatalf("variant nibble = %c in %q", c, id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}

func TestNewObjectIDDeterministic(t *testing.T) {
	src := bytes.Repeat([]byte{0xab}, 16)
	a, err := NewObjectID(bytes.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := NewObjectID(bytes.NewReader(src))
	if a != b {
		t.Fatalf("same input gave %q and %q", a, b)
	}
	if want := "abababab-abab-4bab-abab-abababababab"; a != want {
		t.Fatalf("got %q, want %q", a, want)
	}
}

func TestNewObjectIDShortReader(t *testing.T) {
	if _, err := NewObjectID(bytes.NewReader(make([]byte, 5))); err == nil {
		t.Fatal("expected error for short random source")
	}
}

func TestChunkIDRoundTrip(t *testing.T) {
	obj := "0b9f6c1e-2a7d-4c4e-9d0e-5c3a1f2b7e10"
	for _, idx := range []int{0, 1, 42, 999999, 1234567} {
		id := ChunkID(obj, idx)
		if !ValidChunkID(id) {
			t.Fatalf("ChunkID produced invalid id %q", id)
		}
		gotObj, gotIdx, err := ParseChunkID(id)
		if err != nil {
			t.Fatalf("ParseChunkID(%q): %v", id, err)
		}
		if gotObj != obj || gotIdx != idx {
			t.Fatalf("ParseChunkID(%q) = %q, %d", id, gotObj, gotIdx)
		}
	}
	if got := ChunkID(obj, 3); got != obj+"_000003" {
		t.Fatalf("ChunkID = %q", got)
	}
}

func TestParseChunkIDErrors(t *testing.T) {
	for _, id := range []string{"", "abc", "_1", "abc_", "abc_x", "abc_-1"} {
		if _, _, err := ParseChunkID(id); err == nil {
			t.Errorf("ParseChunkID(%q) succeeded, want error", id)
		}
	}
}

func TestValidChunkID(t *testing.T) {
	good := []string{"a", "abc_000001", "A-b.c_9"}
	bad := []string{"", ".", "..", ".hidden", "a/b", "../etc", "a b", "a\x00b", string(make([]byte, MaxLen+1))}
	for _, id := range good {
		if !ValidChunkID(id) {
			t.Errorf("ValidChunkID(%q) = false", id)
		}
	}
	for _, id := range bad {
		if ValidChunkID(id) {
			t.Errorf("ValidChunkID(%q) = true", id)
		}
	}
}

func TestValidObjectID(t *testing.T) {
	if ValidObjectID("0b9f6c1e-2a7d-4c4e-9d0e-5c3a1f2b7e1") {
		t.Error("accepted short id")
	}
	if ValidObjectID("0B9F6C1E-2A7D-4C4E-9D0E-5C3A1F2B7E10") {
		t.Error("accepted upper-case id")
	}
	if ValidObjectID("0b9f6c1e_2a7d-4c4e-9d0e-5c3a1f2b7e10") {
		t.Error("accepted wrong separator")
	}
}
