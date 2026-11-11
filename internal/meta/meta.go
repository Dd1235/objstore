// Package meta stores object metadata on the controller.
//
// An object record moves through three states:
//
//	pending   -> written before any chunk is stored, so a crashed or failed
//	             upload leaves a record that garbage collection can find
//	committed -> every chunk has all its replicas; the object is visible
//	deleted   -> a tombstone: invisible to clients, kept until its chunks
//	             have been removed from the workers
//
// Two implementations exist: Bolt (durable, used by the controller) and Mem
// (for tests and simulation).
package meta

import (
	"errors"
	"maps"
	"slices"
	"time"
)

// State is the lifecycle state of an object record.
type State string

const (
	Pending   State = "pending"
	Committed State = "committed"
	Deleted   State = "deleted"
)

var (
	ErrNotFound = errors.New("object not found")
	ErrExists   = errors.New("object already exists")
)

// Chunk records one chunk of an object and where its copies live.
type Chunk struct {
	Index    int      `json:"index"`
	ID       string   `json:"chunk_id"`
	Size     int64    `json:"size"`
	SHA256   string   `json:"sha256"`
	Replicas []string `json:"replicas"` // worker IDs
}

// Object is the stored record for one object.
type Object struct {
	ID          string            `json:"object_id"`
	Name        string            `json:"name"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	SHA256      string            `json:"sha256"`
	State       State             `json:"state"`
	StartedAt   time.Time         `json:"started_at"` // upload start; used to expire pending records
	CreatedAt   time.Time         `json:"created_at"`
	ModifiedAt  time.Time         `json:"modified_at"`
	ChunkSize   int               `json:"chunk_size"`
	Chunks      []Chunk           `json:"chunks"`
	Custom      map[string]string `json:"custom_metadata"`
}

// Clone returns a deep copy of o.
func (o Object) Clone() Object {
	c := o
	c.Custom = maps.Clone(o.Custom)
	c.Chunks = slices.Clone(o.Chunks)
	for i := range c.Chunks {
		c.Chunks[i].Replicas = slices.Clone(o.Chunks[i].Replicas)
	}
	return c
}

// Store persists object records. Implementations are safe for concurrent use.
type Store interface {
	// Create adds a new record; it fails with ErrExists if the ID is taken.
	Create(o Object) error
	// Get returns a copy of a record.
	Get(id string) (Object, error)
	// Update applies fn to a record atomically and stores the result. If fn
	// returns an error nothing is written and that error is returned.
	Update(id string, fn func(*Object) error) (Object, error)
	// Delete removes a record.
	Delete(id string) error
	// List returns copies of all records in every state, sorted by ID.
	List() ([]Object, error)
	Close() error
}
