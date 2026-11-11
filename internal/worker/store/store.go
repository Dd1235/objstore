// Package store keeps a worker's chunks on local disk.
//
// Each chunk is one file under <root>/chunks named by its chunk ID. The file
// begins with a 44-byte header (magic "OSC1", big-endian data length, SHA-256
// of the data) followed by the data. Writes go to <root>/tmp, are fsynced and
// then renamed into place, so a reader sees either the old file or the new
// one. Every read re-hashes the data, so corrupt bytes are never returned.
package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/Dd1235/objstore/internal/ids"
)

const (
	magic      = "OSC1"
	headerSize = 4 + 8 + sha256.Size
)

var (
	ErrNotFound         = errors.New("chunk not found")
	ErrCorrupt          = errors.New("chunk data is corrupt")
	ErrChecksumMismatch = errors.New("data does not match its checksum")
	ErrConflict         = errors.New("chunk already stored with a different checksum")
	ErrInvalidID        = errors.New("invalid chunk id")
)

// Info describes a stored chunk.
type Info struct {
	ID      string
	Size    int64
	SHA256  [sha256.Size]byte
	Corrupt bool // set when the last check found the data did not match
}

// Store is a worker's chunk storage.
type Store interface {
	// Put stores data under id. sum must be the SHA-256 of data. It reports
	// created=false when an intact copy with the same checksum already exists.
	Put(id string, data []byte, sum [sha256.Size]byte) (created bool, err error)
	// Get returns the chunk's data after verifying it against its checksum.
	Get(id string) ([]byte, Info, error)
	// Delete removes the chunk.
	Delete(id string) error
	// Verify re-reads the chunk and checks it, updating its Corrupt flag.
	Verify(id string) (Info, error)
	// List returns all known chunks sorted by ID.
	List() []Info
	// Stats returns the number of chunks and their total data size.
	Stats() (count int, bytes int64)
}

// Dir is a Store backed by a directory.
type Dir struct {
	chunksDir string
	tmpDir    string

	mu    sync.Mutex // guards index and serialises renames and removals
	index map[string]Info
}

var _ Store = (*Dir)(nil)

// Open opens or creates a store rooted at root. It removes leftover temporary
// files and builds the index from chunk headers. Data is not verified here;
// use Scrub for that.
func Open(root string) (*Dir, error) {
	d := &Dir{
		chunksDir: filepath.Join(root, "chunks"),
		tmpDir:    filepath.Join(root, "tmp"),
		index:     make(map[string]Info),
	}
	for _, dir := range []string{d.chunksDir, d.tmpDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	if err := clearDir(d.tmpDir); err != nil {
		return nil, fmt.Errorf("clean tmp: %w", err)
	}
	entries, err := os.ReadDir(d.chunksDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.Type().IsRegular() || !ids.ValidChunkID(e.Name()) {
			continue
		}
		info, err := d.readHeader(e.Name())
		if err != nil {
			return nil, err
		}
		d.index[info.ID] = info
	}
	return d, nil
}

// Path returns the file that holds chunk id. Useful for tests and demos.
func (d *Dir) Path(id string) string { return filepath.Join(d.chunksDir, id) }

func (d *Dir) Put(id string, data []byte, sum [sha256.Size]byte) (bool, error) {
	if !ids.ValidChunkID(id) {
		return false, ErrInvalidID
	}
	if sha256.Sum256(data) != sum {
		return false, ErrChecksumMismatch
	}

	d.mu.Lock()
	cur, exists := d.index[id]
	d.mu.Unlock()
	if exists {
		// Keep an intact copy; overwrite only a corrupt or vanished one.
		if _, err := d.Verify(id); err == nil {
			if cur.SHA256 == sum {
				return false, nil
			}
			return false, ErrConflict
		}
	}

	tmp, err := d.writeTemp(id, data, sum)
	if err != nil {
		return false, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if cur, ok := d.index[id]; ok && !cur.Corrupt && cur.SHA256 != sum {
		os.Remove(tmp)
		return false, ErrConflict
	}
	if err := os.Rename(tmp, d.Path(id)); err != nil {
		os.Remove(tmp)
		return false, err
	}
	if err := syncDir(d.chunksDir); err != nil {
		return false, err
	}
	d.index[id] = Info{ID: id, Size: int64(len(data)), SHA256: sum}
	return true, nil
}

func (d *Dir) Get(id string) ([]byte, Info, error) {
	if !ids.ValidChunkID(id) {
		return nil, Info{}, ErrInvalidID
	}
	raw, err := os.ReadFile(d.Path(id))
	if errors.Is(err, fs.ErrNotExist) {
		d.mu.Lock()
		delete(d.index, id)
		d.mu.Unlock()
		return nil, Info{}, ErrNotFound
	}
	if err != nil {
		return nil, Info{}, err
	}
	info, data, derr := decode(id, raw)
	d.mu.Lock()
	d.index[id] = info
	d.mu.Unlock()
	if derr != nil {
		return nil, info, fmt.Errorf("%w: %s: %v", ErrCorrupt, id, derr)
	}
	return data, info, nil
}

func (d *Dir) Verify(id string) (Info, error) {
	_, info, err := d.Get(id)
	return info, err
}

func (d *Dir) Delete(id string) error {
	if !ids.ValidChunkID(id) {
		return ErrInvalidID
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	err := os.Remove(d.Path(id))
	delete(d.index, id)
	if errors.Is(err, fs.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return syncDir(d.chunksDir)
}

func (d *Dir) List() []Info {
	d.mu.Lock()
	out := make([]Info, 0, len(d.index))
	for _, info := range d.index {
		out = append(out, info)
	}
	d.mu.Unlock()
	slices.SortFunc(out, func(a, b Info) int { return strings.Compare(a.ID, b.ID) })
	return out
}

func (d *Dir) Stats() (int, int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	var total int64
	for _, info := range d.index {
		total += info.Size
	}
	return len(d.index), total
}

// Scrub verifies every chunk and returns the IDs found corrupt. It stops
// early if ctx is cancelled.
func Scrub(ctx context.Context, s Store) (checked int, corrupt []string, err error) {
	for _, info := range s.List() {
		if err := ctx.Err(); err != nil {
			return checked, corrupt, err
		}
		_, verr := s.Verify(info.ID)
		switch {
		case verr == nil, errors.Is(verr, ErrNotFound):
		case errors.Is(verr, ErrCorrupt):
			corrupt = append(corrupt, info.ID)
		default:
			return checked, corrupt, verr
		}
		checked++
	}
	return checked, corrupt, nil
}

func (d *Dir) writeTemp(id string, data []byte, sum [sha256.Size]byte) (string, error) {
	f, err := os.CreateTemp(d.tmpDir, id+".*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	fail := func(err error) (string, error) {
		f.Close()
		os.Remove(name)
		return "", err
	}
	if _, err := f.Write(encodeHeader(int64(len(data)), sum)); err != nil {
		return fail(err)
	}
	if _, err := f.Write(data); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		os.Remove(name)
		return "", err
	}
	return name, nil
}

// readHeader builds an Info from a chunk file's header without reading the
// data. A malformed header or a length that disagrees with the file size marks
// the chunk corrupt.
func (d *Dir) readHeader(id string) (Info, error) {
	f, err := os.Open(d.Path(id))
	if err != nil {
		return Info{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return Info{}, err
	}
	info := Info{ID: id, Size: max(st.Size()-headerSize, 0)}
	var hdr [headerSize]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		info.Corrupt = true
		return info, nil
	}
	size, sum, ok := decodeHeader(hdr[:])
	if !ok || size != st.Size()-headerSize {
		info.Corrupt = true
		return info, nil
	}
	info.Size, info.SHA256 = size, sum
	return info, nil
}

func encodeHeader(size int64, sum [sha256.Size]byte) []byte {
	b := make([]byte, headerSize)
	copy(b, magic)
	binary.BigEndian.PutUint64(b[4:12], uint64(size))
	copy(b[12:], sum[:])
	return b
}

func decodeHeader(b []byte) (size int64, sum [sha256.Size]byte, ok bool) {
	if len(b) < headerSize || !bytes.Equal(b[:4], []byte(magic)) {
		return 0, sum, false
	}
	copy(sum[:], b[12:headerSize])
	return int64(binary.BigEndian.Uint64(b[4:12])), sum, true
}

// decode parses a whole chunk file. It always returns the best Info it can;
// a non-nil error means the chunk is corrupt and Info.Corrupt is set.
func decode(id string, raw []byte) (Info, []byte, error) {
	info := Info{ID: id, Size: int64(max(len(raw)-headerSize, 0)), Corrupt: true}
	size, sum, ok := decodeHeader(raw)
	if !ok {
		return info, nil, errors.New("bad header")
	}
	info.SHA256 = sum
	data := raw[headerSize:]
	if size != int64(len(data)) {
		return info, nil, fmt.Errorf("header says %d bytes, file has %d", size, len(data))
	}
	if sha256.Sum256(data) != sum {
		return info, nil, errors.New("checksum mismatch")
	}
	info.Corrupt = false
	return info, data, nil
}

func clearDir(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
