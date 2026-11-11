// Package chunker splits a byte stream into fixed-size chunks and computes
// SHA-256 checksums for each chunk and for the whole stream.
package chunker

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
)

// Size is the chunk size used by the object store: 1 MiB.
const Size = 1 << 20

// Chunk is one piece of a stream.
type Chunk struct {
	Index  int
	Data   []byte // only valid until the next call to Next
	SHA256 [sha256.Size]byte
}

// Splitter reads a stream and returns it as a sequence of chunks. All chunks
// except the last are exactly the configured size; the last may be shorter.
// An empty stream yields no chunks.
type Splitter struct {
	r     io.Reader
	buf   []byte
	next  int
	total int64
	whole hash.Hash
	done  bool
}

// NewSplitter returns a Splitter that cuts r into chunks of size bytes.
func NewSplitter(r io.Reader, size int) *Splitter {
	if size <= 0 {
		panic(fmt.Sprintf("chunker: invalid chunk size %d", size))
	}
	return &Splitter{r: r, buf: make([]byte, size), whole: sha256.New()}
}

// Next returns the next chunk, or io.EOF after the last one. The returned
// Data slice is reused by the following call, so copy it if you keep it.
func (s *Splitter) Next() (Chunk, error) {
	if s.done {
		return Chunk{}, io.EOF
	}
	n, err := io.ReadFull(s.r, s.buf)
	switch {
	case err == io.EOF:
		s.done = true
		return Chunk{}, io.EOF
	case errors.Is(err, io.ErrUnexpectedEOF):
		s.done = true // short final chunk
	case err != nil:
		return Chunk{}, fmt.Errorf("read chunk %d: %w", s.next, err)
	}
	data := s.buf[:n]
	s.whole.Write(data)
	s.total += int64(n)
	c := Chunk{Index: s.next, Data: data, SHA256: sha256.Sum256(data)}
	s.next++
	return c, nil
}

// Count returns the number of chunks returned so far.
func (s *Splitter) Count() int { return s.next }

// Size returns the number of bytes consumed so far.
func (s *Splitter) Size() int64 { return s.total }

// Sum returns the SHA-256 of all bytes consumed so far. After Next has
// returned io.EOF this is the checksum of the whole stream.
func (s *Splitter) Sum() [sha256.Size]byte {
	var out [sha256.Size]byte
	s.whole.Sum(out[:0])
	return out
}

// Count returns how many chunks an object of the given size has.
func Count(objectSize int64, chunkSize int) int {
	if objectSize <= 0 {
		return 0
	}
	cs := int64(chunkSize)
	return int((objectSize + cs - 1) / cs)
}
