package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"strconv"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/meta"
	"github.com/Dd1235/objstore/internal/workerclient"
)

var errNoReplica = errors.New("no readable replica")

// downloadObject handles GET (and HEAD) /api/v1/objects/{id}. Chunks are
// fetched one at a time and streamed in order. Every chunk is checked against
// the controller's checksum, so corrupt bytes are never sent while another
// copy is valid (I2). If no valid copy of the first chunk is available the
// request fails with 503; if a later chunk fails the connection is aborted, so
// the client sees a truncated transfer rather than wrong data.
func (c *Controller) downloadObject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	obj, err := c.acquire(id)
	if err != nil {
		c.writeMetaError(w, id, err)
		return
	}
	defer c.release(id)

	if r.Method == http.MethodHead {
		setObjectHeaders(w, obj)
		w.WriteHeader(http.StatusOK)
		return
	}
	if len(obj.Chunks) == 0 {
		setObjectHeaders(w, obj)
		w.WriteHeader(http.StatusOK)
		return
	}
	for i, ch := range obj.Chunks {
		data, err := c.readChunk(r.Context(), ch)
		if err != nil {
			if r.Context().Err() != nil {
				return // client went away
			}
			c.log.Error("download failed", "object_id", id, "chunk_id", ch.ID, "err", err)
			if i == 0 {
				api.WriteError(w, http.StatusServiceUnavailable, api.CodeUnavailable,
					"no valid copy of chunk "+ch.ID+" is reachable")
				return
			}
			panic(http.ErrAbortHandler)
		}
		if i == 0 {
			setObjectHeaders(w, obj)
			w.WriteHeader(http.StatusOK)
		}
		if _, err := w.Write(data); err != nil {
			return
		}
	}
}

func setObjectHeaders(w http.ResponseWriter, o meta.Object) {
	h := w.Header()
	h.Set("Content-Type", o.ContentType)
	h.Set("Content-Length", strconv.FormatInt(o.Size, 10))
	h.Set("ETag", `"`+o.SHA256+`"`)
	h.Set("Last-Modified", o.ModifiedAt.UTC().Format(http.TimeFormat))
	h.Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": o.Name}))
	h.Set("X-Object-Id", o.ID)
}

// acquire returns a committed object and registers a download in progress,
// which delays removal of its chunks if it is deleted meanwhile (C6).
func (c *Controller) acquire(id string) (meta.Object, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	o, err := c.committed(id)
	if err != nil {
		return meta.Object{}, err
	}
	c.readers[id]++
	return o, nil
}

// release ends a download; the last reader of a deleted object starts the
// removal of its chunks.
func (c *Controller) release(id string) {
	c.rmu.Lock()
	c.readers[id]--
	last := c.readers[id] == 0
	if last {
		delete(c.readers, id)
	}
	purge := last && c.deleting[id]
	if purge {
		delete(c.deleting, id)
	}
	c.rmu.Unlock()
	if purge {
		c.purge(id)
	}
}

// readChunk returns a verified copy of a chunk. Replicas that are corrupt or
// missing the chunk are repaired in the background from the verified copy.
func (c *Controller) readChunk(ctx context.Context, ch meta.Chunk) ([]byte, error) {
	want, err := hex.DecodeString(ch.SHA256)
	if err != nil || len(want) != sha256.Size {
		return nil, fmt.Errorf("bad checksum in metadata for chunk %s", ch.ID)
	}
	var bad []workerRef
	lastErr := errNoReplica
	for _, wk := range c.readOrder(ch.Replicas) {
		wctx, cancel := context.WithTimeout(ctx, c.cfg.WorkerTimeout)
		data, err := c.chunks.Get(wctx, wk.addr, ch.ID)
		cancel()
		if err == nil {
			if sum := sha256.Sum256(data); int64(len(data)) != ch.Size || string(sum[:]) != string(want) {
				err = fmt.Errorf("%w: checksum differs from controller metadata", workerclient.ErrCorrupt)
			}
		}
		if err == nil {
			if len(bad) > 0 {
				c.repairReplicas(ch, data, bad)
			}
			return data, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		lastErr = err
		if errors.Is(err, workerclient.ErrCorrupt) || errors.Is(err, workerclient.ErrNotFound) {
			c.log.Warn("bad replica, trying another copy", "chunk_id", ch.ID, "worker_id", wk.id, "err", err)
			bad = append(bad, wk)
		} else {
			c.log.Warn("replica unreachable, trying another copy", "chunk_id", ch.ID, "worker_id", wk.id, "err", err)
		}
	}
	return nil, fmt.Errorf("chunk %s: %w", ch.ID, lastErr)
}

// repairReplicas rewrites a chunk on workers whose copy was corrupt or
// missing, using data that has just been verified against the controller's
// checksum. A worker holding different but self-consistent bytes is told to
// delete them first.
func (c *Controller) repairReplicas(ch meta.Chunk, data []byte, bad []workerRef) {
	sum := sha256.Sum256(data)
	c.background(func(ctx context.Context) {
		for _, wk := range bad {
			wctx, cancel := context.WithTimeout(ctx, c.cfg.WorkerTimeout)
			err := c.chunks.Put(wctx, wk.addr, ch.ID, data, sum)
			if errors.Is(err, workerclient.ErrConflict) {
				if err = c.chunks.Delete(wctx, wk.addr, ch.ID); err == nil || errors.Is(err, workerclient.ErrNotFound) {
					err = c.chunks.Put(wctx, wk.addr, ch.ID, data, sum)
				}
			}
			cancel()
			if err != nil {
				c.log.Error("replica repair failed", "chunk_id", ch.ID, "worker_id", wk.id, "err", err)
				continue
			}
			c.log.Info("replica repaired from verified copy", "chunk_id", ch.ID, "worker_id", wk.id)
		}
	})
}
