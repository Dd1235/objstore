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
