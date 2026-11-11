// Package api holds the JSON types and HTTP helpers shared by the
// controller, the workers and the client.
package api

import (
	"encoding/json"
	"net/http"
	"time"
)

// HeaderChunkSHA256 carries the hex SHA-256 of a chunk on internal chunk
// requests and responses.
const HeaderChunkSHA256 = "X-Chunk-SHA256"

// Error codes returned in Error.Code.
const (
	CodeBadRequest       = "bad_request"
	CodeNotFound         = "not_found"
	CodeConflict         = "conflict"
	CodeChecksumMismatch = "checksum_mismatch"
	CodeChunkCorrupt     = "chunk_corrupt"
	CodeTooLarge         = "too_large"
	CodeUnknownWorker    = "unknown_worker"
	CodeUnavailable      = "unavailable"
	CodeInternal         = "internal"
)

// Error is the body of every error response.
type Error struct {
	Code    string `json:"error"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// WriteJSON writes v as JSON with the given status code.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// WriteError writes an Error body with the given status code.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	WriteJSON(w, status, Error{Code: code, Message: message})
}

// RegisterRequest is sent by a worker when it starts.
type RegisterRequest struct {
	WorkerID   string `json:"worker_id"`
	Addr       string `json:"addr"` // base URL the controller uses to reach the worker
	ChunkCount int    `json:"chunk_count"`
	UsedBytes  int64  `json:"used_bytes"`
}

// RegisterResponse tells the worker how often to heartbeat.
type RegisterResponse struct {
	HeartbeatIntervalMS int64 `json:"heartbeat_interval_ms"`
}

// HeartbeatRequest is sent by a worker periodically.
type HeartbeatRequest struct {
	WorkerID   string `json:"worker_id"`
	ChunkCount int    `json:"chunk_count"`
	UsedBytes  int64  `json:"used_bytes"`
}

// ChunkInfo describes one chunk held by a worker.
type ChunkInfo struct {
	ChunkID string `json:"chunk_id"`
	Size    int64  `json:"size"`
	SHA256  string `json:"sha256"`
	Corrupt bool   `json:"corrupt,omitempty"`
}

// ChunkInventory lists the chunks held by a worker.
type ChunkInventory struct {
	WorkerID   string      `json:"worker_id"`
	ChunkCount int         `json:"chunk_count"`
	UsedBytes  int64       `json:"used_bytes"`
	Chunks     []ChunkInfo `json:"chunks"`
}

// WorkerStatus is the controller's view of one worker.
type WorkerStatus struct {
	WorkerID      string    `json:"worker_id"`
	Addr          string    `json:"addr"`
	State         string    `json:"state"`
	Rejoining     bool      `json:"rejoining,omitempty"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	StateSince    time.Time `json:"state_since"`
	ChunkCount    int       `json:"chunk_count"`
	UsedBytes     int64     `json:"used_bytes"`
}

// WorkerList is the response of GET /internal/workers.
type WorkerList struct {
	Workers []WorkerStatus `json:"workers"`
}

// ObjectChunk describes one chunk of an object and where its copies are.
type ObjectChunk struct {
	Index    int      `json:"index"`
	ChunkID  string   `json:"chunk_id"`
	Size     int64    `json:"size"`
	SHA256   string   `json:"sha256"`
	Replicas []string `json:"replicas"`
}

// ObjectInfo is the full metadata of an object: the response of upload and
// of GET/PATCH /api/v1/objects/{id}/metadata.
type ObjectInfo struct {
	ObjectID    string            `json:"object_id"`
	Name        string            `json:"name"`
	Size        int64             `json:"size"`
	ContentType string            `json:"content_type"`
	SHA256      string            `json:"sha256"`
	CreatedAt   time.Time         `json:"created_at"`
	ModifiedAt  time.Time         `json:"modified_at"`
	ChunkSize   int               `json:"chunk_size"`
	ChunkCount  int               `json:"chunk_count"`
	Chunks      []ObjectChunk     `json:"chunks"`
	Metadata    map[string]string `json:"metadata"`
}

// ObjectSummary is one entry of GET /api/v1/objects.
type ObjectSummary struct {
	ObjectID    string    `json:"object_id"`
	Name        string    `json:"name"`
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type"`
	CreatedAt   time.Time `json:"created_at"`
	ChunkCount  int       `json:"chunk_count"`
}

// ObjectList is the response of GET /api/v1/objects.
type ObjectList struct {
	Objects    []ObjectSummary `json:"objects"`
	Total      int             `json:"total"`
	Offset     int             `json:"offset"`
	Limit      int             `json:"limit"`
	NextOffset *int            `json:"next_offset,omitempty"`
}
