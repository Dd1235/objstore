package chunker

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"math/rand/v2"
	"testing"
	"testing/iotest"
)

func randomBytes(n int, seed uint64) []byte {
	r := rand.New(rand.NewPCG(seed, seed))
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func split(t *testing.T, r io.Reader, size int) (chunks [][]byte, sums [][32]byte, s *Splitter) {
	t.Helper()
	s = NewSplitter(r, size)
	for {
		c, err := s.Next()
		if err == io.EOF {
			return chunks, sums, s
		}
		if err != nil {
			t.Fatal(err)
		}
		if c.Index != len(chunks) {
			t.Fatalf("chunk index %d, want %d", c.Index, len(chunks))
		}
		chunks = append(chunks, bytes.Clone(c.Data))
		sums = append(sums, c.SHA256)
	}
}

func TestSplitSizes(t *testing.T) {
	const size = 64
	for _, n := range []int{0, 1, size - 1, size, size + 1, 3*size + 17, 5 * size} {
		data := randomBytes(n, uint64(n))
		chunks, sums, s := split(t, bytes.NewReader(data), size)

		if want := Count(int64(n), size); len(chunks) != want {
			t.Fatalf("n=%d: %d chunks, want %d", n, len(chunks), want)
		}
		for i, c := range chunks {
			last := i == len(chunks)-1
			if !last && len(c) != size {
				t.Fatalf("n=%d: chunk %d has %d bytes, want %d", n, i, len(c), size)
			}
			if last && (len(c) == 0 || len(c) > size) {
				t.Fatalf("n=%d: last chunk has %d bytes", n, len(c))
			}
			if sums[i] != sha256.Sum256(c) {
				t.Fatalf("n=%d: chunk %d checksum mismatch", n, i)
			}
		}
		if got := bytes.Join(chunks, nil); !bytes.Equal(got, data) {
			t.Fatalf("n=%d: reassembled data differs", n)
		}
		if s.Sum() != sha256.Sum256(data) {
			t.Fatalf("n=%d: whole-object checksum mismatch", n)
		}
		if s.Size() != int64(n) || s.Count() != len(chunks) {
			t.Fatalf("n=%d: Size()=%d Count()=%d", n, s.Size(), s.Count())
		}
	}
}

func TestSplitShortReads(t *testing.T) {
	data := randomBytes(1000, 7)
	chunks, _, _ := split(t, iotest.OneByteReader(bytes.NewReader(data)), 300)
	if len(chunks) != 4 || len(chunks[3]) != 100 {
		t.Fatalf("got %d chunks, last %d bytes", len(chunks), len(chunks[len(chunks)-1]))
	}
	if !bytes.Equal(bytes.Join(chunks, nil), data) {
		t.Fatal("reassembled data differs")
	}
}

func TestSplitReadError(t *testing.T) {
	boom := errors.New("boom")
	r := io.MultiReader(bytes.NewReader(make([]byte, 100)), iotest.ErrReader(boom))
	s := NewSplitter(r, 64)
	if _, err := s.Next(); err != nil {
		t.Fatalf("first chunk: %v", err)
	}
	if _, err := s.Next(); !errors.Is(err, boom) {
		t.Fatalf("got %v, want %v", err, boom)
	}
}

func TestNextAfterEOF(t *testing.T) {
	s := NewSplitter(bytes.NewReader([]byte("abc")), 64)
	if _, err := s.Next(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if _, err := s.Next(); err != io.EOF {
			t.Fatalf("got %v, want io.EOF", err)
		}
	}
}

func TestCount(t *testing.T) {
	cases := []struct {
		size int64
		want int
	}{{0, 0}, {1, 1}, {Size - 1, 1}, {Size, 1}, {Size + 1, 2}, {10*Size + 5, 11}}
	for _, c := range cases {
		if got := Count(c.size, Size); got != c.want {
			t.Errorf("Count(%d) = %d, want %d", c.size, got, c.want)
		}
	}
}
