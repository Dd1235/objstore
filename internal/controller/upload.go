package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"path/filepath"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/chunker"
	"github.com/Dd1235/objstore/internal/ids"
	"github.com/Dd1235/objstore/internal/meta"
	"github.com/Dd1235/objstore/internal/placement"
)

var errNoCapacity = errors.New("not enough storage workers available")

// uploadObject handles POST /api/v1/objects (multipart/form-data with a
// required "file" field and an optional "metadata" JSON field).
//
// The upload is streamed: each chunk is written to its workers as soon as it
// has been read. A pending metadata record exists from the first byte, and the
// object only becomes visible once every chunk has all its copies (C2). On any
// failure the chunks written so far are removed in the background.
func (c *Controller) uploadObject(w http.ResponseWriter, r *http.Request) {
	mr, err := r.MultipartReader()
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest,
			"request must be multipart/form-data with a \"file\" field")
		return
	}

	var (
		obj      *meta.Object // the pending record, once the file part starts
		custom   = map[string]string{}
		haveFile bool
	)
	fail := func(status int, code, msg string) {
		if obj != nil {
			c.abortUpload(*obj)
		}
		api.WriteError(w, status, code, msg)
	}

	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			fail(http.StatusBadRequest, api.CodeBadRequest, "malformed multipart body: "+err.Error())
			return
		}
		switch part.FormName() {
		case "metadata":
			body, err := io.ReadAll(io.LimitReader(part, maxMetaBody+1))
			if err != nil || len(body) > maxMetaBody {
				fail(http.StatusBadRequest, api.CodeBadRequest, "metadata field too large or unreadable")
				return
			}
			parsed, err := parseMetadata(body, false)
			if err != nil {
				fail(http.StatusBadRequest, api.CodeBadRequest, err.Error())
				return
			}
			for k, v := range parsed {
				custom[k] = *v
			}
		case "file":
			if haveFile {
				fail(http.StatusBadRequest, api.CodeBadRequest, "only one \"file\" field is allowed")
				return
			}
			haveFile = true
			o, status, err := c.receiveFile(r.Context(), part)
			obj = o
			if err != nil {
				code := api.CodeUnavailable
				if status == http.StatusBadRequest {
					code = api.CodeBadRequest
				} else if status == http.StatusInternalServerError {
					code = api.CodeInternal
				}
				fail(status, code, err.Error())
				return
			}
		}
		part.Close()
	}
	if !haveFile {
		fail(http.StatusBadRequest, api.CodeBadRequest, "missing \"file\" field")
		return
	}

	now := c.now().UTC()
	committed, err := c.meta.Update(obj.ID, func(o *meta.Object) error {
		*o = *obj
		o.Custom = custom
		o.State = meta.Committed
		o.CreatedAt = now
		o.ModifiedAt = now
		return nil
	})
	if err != nil {
		c.log.Error("commit object", "object_id", obj.ID, "err", err)
		fail(http.StatusInternalServerError, api.CodeInternal, "could not commit object metadata")
		return
	}
	c.log.Info("object stored", "object_id", committed.ID, "name", committed.Name,
		"size", committed.Size, "chunks", len(committed.Chunks))
	w.Header().Set("Location", "/api/v1/objects/"+committed.ID)
	api.WriteJSON(w, http.StatusCreated, objectInfo(committed))
}

// receiveFile creates the pending record and streams the file part to the
// workers. It returns the record built so far (also on error, so the caller
// can clean up) and, on error, the HTTP status to report.
func (c *Controller) receiveFile(ctx context.Context, part *multipart.Part) (*meta.Object, int, error) {
	id, err := ids.NewObjectID(c.rand)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}
	name := part.FileName()
	if name == "" {
		name = "unnamed"
	}
	obj := &meta.Object{
		ID: id, Name: name, State: meta.Pending, StartedAt: c.now().UTC(),
		ChunkSize: c.cfg.ChunkSize, Custom: map[string]string{},
	}
	if err := c.meta.Create(*obj); err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("create metadata record: %w", err)
	}

	sp := chunker.NewSplitter(part, c.cfg.ChunkSize)
	var sniff []byte
	for {
		ch, err := sp.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return obj, http.StatusBadRequest, fmt.Errorf("reading upload: %w", err)
		}
		if ch.Index == 0 {
			sniff = append([]byte(nil), ch.Data[:min(len(ch.Data), 512)]...)
		}
		chunkID := ids.ChunkID(id, ch.Index)
		replicas, err := c.writeChunk(ctx, chunkID, ch.Data, ch.SHA256)
		obj.Chunks = append(obj.Chunks, meta.Chunk{
			Index: ch.Index, ID: chunkID, Size: int64(len(ch.Data)),
			SHA256: hex.EncodeToString(ch.SHA256[:]), Replicas: replicas,
		})
		if err != nil {
			if ctx.Err() != nil {
				return obj, http.StatusBadRequest, fmt.Errorf("upload cancelled: %w", ctx.Err())
			}
			return obj, http.StatusServiceUnavailable, err
		}
	}
	sum := sp.Sum()
	obj.Size = sp.Size()
	obj.SHA256 = hex.EncodeToString(sum[:])
	obj.ContentType = contentType(part.Header.Get("Content-Type"), name, sniff)
	return obj, 0, nil
}

// writeChunk stores Replicas copies of a chunk on distinct workers, trying
// workers in rendezvous order and skipping any that fail. It returns the
// workers that acknowledged the write.
func (c *Controller) writeChunk(ctx context.Context, chunkID string, data []byte, sum [sha256.Size]byte) ([]string, error) {
	workers := c.placeable()
	if len(workers) < c.cfg.Replicas {
		return nil, fmt.Errorf("%w: %d alive, %d copies required", errNoCapacity, len(workers), c.cfg.Replicas)
	}
	candidates := make([]string, 0, len(workers))
	for id := range workers {
		candidates = append(candidates, id)
	}
	var stored []string
	for _, wid := range placement.Rank(chunkID, candidates) {
		if len(stored) == c.cfg.Replicas {
			break
		}
		wctx, cancel := context.WithTimeout(ctx, c.cfg.WorkerTimeout)
		err := c.chunks.Put(wctx, workers[wid], chunkID, data, sum)
		cancel()
		if err == nil {
			stored = append(stored, wid)
			continue
		}
		if ctx.Err() != nil {
			return stored, ctx.Err()
		}
		c.log.Warn("chunk write failed, trying next worker", "chunk_id", chunkID, "worker_id", wid, "err", err)
	}
	if len(stored) < c.cfg.Replicas {
		return stored, fmt.Errorf("%w: stored %d of %d copies of chunk %s",
			errNoCapacity, len(stored), c.cfg.Replicas, chunkID)
	}
	return stored, nil
}

// abortUpload removes the chunks of a failed upload and its pending record.
// If some chunk cannot be removed now, the pending record is kept so garbage
// collection can finish the job later.
func (c *Controller) abortUpload(obj meta.Object) {
	c.background(func(ctx context.Context) {
		if c.deleteChunks(ctx, obj) {
			if err := c.meta.Delete(obj.ID); err != nil && !errors.Is(err, meta.ErrNotFound) {
				c.log.Error("remove aborted upload record", "object_id", obj.ID, "err", err)
			}
		}
		c.log.Info("upload aborted", "object_id", obj.ID, "chunks_written", len(obj.Chunks))
	})
}

// contentType picks the object's content type: the type declared on the file
// part unless it is missing or generic, then the file extension, then content
// sniffing.
func contentType(declared, name string, sniff []byte) string {
	if declared != "" {
		if mt, params, err := mime.ParseMediaType(declared); err == nil && mt != "application/octet-stream" {
			return mime.FormatMediaType(mt, params)
		}
	}
	if t := mime.TypeByExtension(filepath.Ext(name)); t != "" {
		return t
	}
	if len(sniff) > 0 {
		return http.DetectContentType(sniff)
	}
	return "application/octet-stream"
}
