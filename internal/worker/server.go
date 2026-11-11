// Package worker implements a storage worker: the internal chunk API, the
// registration and heartbeat agent, and the background scrubber.
package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/chunker"
	"github.com/Dd1235/objstore/internal/worker/store"
)

// Server serves the internal chunk API of one worker.
type Server struct {
	ID       string
	Store    store.Store
	MaxChunk int64 // largest accepted chunk body; defaults to chunker.Size
	Log      *slog.Logger
}

// Handler returns the HTTP routes for the worker.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /internal/chunks/{chunk_id}", s.putChunk)
	mux.HandleFunc("GET /internal/chunks/{chunk_id}", s.getChunk)
	mux.HandleFunc("DELETE /internal/chunks/{chunk_id}", s.deleteChunk)
	mux.HandleFunc("GET /internal/workers/{worker_id}/chunks", s.listChunks)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		api.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok", "worker_id": s.ID})
	})
	return mux
}

func (s *Server) maxChunk() int64 {
	if s.MaxChunk > 0 {
		return s.MaxChunk
	}
	return chunker.Size
}

func (s *Server) log() *slog.Logger {
	if s.Log != nil {
		return s.Log
	}
	return slog.Default()
}

func (s *Server) putChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")
	sum, ok := parseSum(r.Header.Get(api.HeaderChunkSHA256))
	if !ok {
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest,
			"header "+api.HeaderChunkSHA256+" must be a hex SHA-256")
		return
	}
	limit := s.maxChunk()
	if r.ContentLength > limit {
		api.WriteError(w, http.StatusRequestEntityTooLarge, api.CodeTooLarge,
			"chunk exceeds "+strconv.FormatInt(limit, 10)+" bytes")
		return
	}
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			api.WriteError(w, http.StatusRequestEntityTooLarge, api.CodeTooLarge,
				"chunk exceeds "+strconv.FormatInt(limit, 10)+" bytes")
			return
		}
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, "read body: "+err.Error())
		return
	}

	created, err := s.Store.Put(id, data, sum)
	if err != nil {
		s.writeStoreError(w, id, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	api.WriteJSON(w, status, api.ChunkInfo{ChunkID: id, Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])})
}

func (s *Server) getChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")
	data, info, err := s.Store.Get(id)
	if err != nil {
		s.writeStoreError(w, id, err)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.Itoa(len(data)))
	h.Set(api.HeaderChunkSHA256, hex.EncodeToString(info.SHA256[:]))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (s *Server) deleteChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("chunk_id")
	if err := s.Store.Delete(id); err != nil {
		s.writeStoreError(w, id, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listChunks(w http.ResponseWriter, r *http.Request) {
	if r.PathValue("worker_id") != s.ID {
		api.WriteError(w, http.StatusNotFound, api.CodeNotFound, "this is worker "+s.ID)
		return
	}
	infos := s.Store.List()
	inv := api.ChunkInventory{WorkerID: s.ID, Chunks: make([]api.ChunkInfo, 0, len(infos))}
	for _, info := range infos {
		inv.Chunks = append(inv.Chunks, api.ChunkInfo{
			ChunkID: info.ID,
			Size:    info.Size,
			SHA256:  hex.EncodeToString(info.SHA256[:]),
			Corrupt: info.Corrupt,
		})
		inv.UsedBytes += info.Size
	}
	inv.ChunkCount = len(inv.Chunks)
	api.WriteJSON(w, http.StatusOK, inv)
}

func (s *Server) writeStoreError(w http.ResponseWriter, id string, err error) {
	switch {
	case errors.Is(err, store.ErrInvalidID):
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, "invalid chunk id")
	case errors.Is(err, store.ErrNotFound):
		api.WriteError(w, http.StatusNotFound, api.CodeNotFound, "chunk "+id+" not found")
	case errors.Is(err, store.ErrChecksumMismatch):
		api.WriteError(w, http.StatusBadRequest, api.CodeChecksumMismatch, "body does not match "+api.HeaderChunkSHA256)
	case errors.Is(err, store.ErrConflict):
		api.WriteError(w, http.StatusConflict, api.CodeConflict, "chunk "+id+" already stored with a different checksum")
	case errors.Is(err, store.ErrCorrupt):
		s.log().Warn("corrupt chunk detected on read", "chunk_id", id)
		api.WriteError(w, http.StatusInternalServerError, api.CodeChunkCorrupt, "chunk "+id+" failed checksum verification")
	default:
		s.log().Error("store error", "chunk_id", id, "err", err)
		api.WriteError(w, http.StatusInternalServerError, api.CodeInternal, err.Error())
	}
}

func parseSum(s string) ([sha256.Size]byte, bool) {
	var sum [sha256.Size]byte
	if len(s) != hex.EncodedLen(sha256.Size) {
		return sum, false
	}
	if _, err := hex.Decode(sum[:], []byte(s)); err != nil {
		return sum, false
	}
	return sum, true
}
