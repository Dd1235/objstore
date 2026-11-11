package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"unicode/utf8"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/meta"
)

// Limits on custom metadata.
const (
	maxMetaKeys     = 64
	maxMetaKeyLen   = 128
	maxMetaValueLen = 2048
	maxMetaBody     = 64 << 10
)

// Paging limits for GET /api/v1/objects.
const (
	defaultListLimit = 100
	maxListLimit     = 1000
)

var errBadMetadata = errors.New("invalid metadata")

func objectInfo(o meta.Object) api.ObjectInfo {
	info := api.ObjectInfo{
		ObjectID:    o.ID,
		Name:        o.Name,
		Size:        o.Size,
		ContentType: o.ContentType,
		SHA256:      o.SHA256,
		CreatedAt:   o.CreatedAt,
		ModifiedAt:  o.ModifiedAt,
		ChunkSize:   o.ChunkSize,
		ChunkCount:  len(o.Chunks),
		Chunks:      make([]api.ObjectChunk, len(o.Chunks)),
		Metadata:    o.Custom,
	}
	if info.Metadata == nil {
		info.Metadata = map[string]string{}
	}
	for i, ch := range o.Chunks {
		info.Chunks[i] = api.ObjectChunk{
			Index: ch.Index, ChunkID: ch.ID, Size: ch.Size, SHA256: ch.SHA256, Replicas: ch.Replicas,
		}
	}
	return info
}

// committed returns the object if it exists and is visible to clients.
func (c *Controller) committed(id string) (meta.Object, error) {
	o, err := c.meta.Get(id)
	if err != nil {
		return meta.Object{}, err
	}
	if o.State != meta.Committed {
		return meta.Object{}, meta.ErrNotFound
	}
	return o, nil
}

func (c *Controller) getMetadata(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	o, err := c.committed(id)
	if err != nil {
		c.writeMetaError(w, id, err)
		return
	}
	api.WriteJSON(w, http.StatusOK, objectInfo(o))
}

func (c *Controller) patchMetadata(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxMetaBody))
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, "metadata body too large or unreadable")
		return
	}
	changes, err := parseMetadata(body, true)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, err.Error())
		return
	}
	o, err := c.meta.Update(id, func(o *meta.Object) error {
		if o.State != meta.Committed {
			return meta.ErrNotFound
		}
		merged := maps.Clone(o.Custom)
		if merged == nil {
			merged = map[string]string{}
		}
		for k, v := range changes {
			if v == nil {
				delete(merged, k)
			} else {
				merged[k] = *v
			}
		}
		if len(merged) > maxMetaKeys {
			return fmt.Errorf("%w: at most %d keys allowed", errBadMetadata, maxMetaKeys)
		}
		if !maps.Equal(merged, o.Custom) {
			o.Custom = merged
			o.ModifiedAt = c.now().UTC()
		}
		return nil
	})
	if err != nil {
		c.writeMetaError(w, id, err)
		return
	}
	api.WriteJSON(w, http.StatusOK, objectInfo(o))
}

func (c *Controller) listObjects(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", defaultListLimit, 1, maxListLimit)
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, err.Error())
		return
	}
	offset, err := queryInt(r, "offset", 0, 0, int(^uint(0)>>1))
	if err != nil {
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, err.Error())
		return
	}
	all, err := c.meta.List()
	if err != nil {
		c.log.Error("list metadata", "err", err)
		api.WriteError(w, http.StatusInternalServerError, api.CodeInternal, "could not read metadata")
		return
	}
	visible := slices.DeleteFunc(all, func(o meta.Object) bool { return o.State != meta.Committed })
	slices.SortStableFunc(visible, func(a, b meta.Object) int { return a.CreatedAt.Compare(b.CreatedAt) })

	out := api.ObjectList{Objects: []api.ObjectSummary{}, Total: len(visible), Offset: offset, Limit: limit}
	if offset < len(visible) {
		end := min(offset+limit, len(visible))
		for _, o := range visible[offset:end] {
			out.Objects = append(out.Objects, api.ObjectSummary{
				ObjectID: o.ID, Name: o.Name, Size: o.Size, ContentType: o.ContentType,
				CreatedAt: o.CreatedAt, ChunkCount: len(o.Chunks),
			})
		}
		if end < len(visible) {
			out.NextOffset = &end
		}
	}
	api.WriteJSON(w, http.StatusOK, out)
}

func (c *Controller) writeMetaError(w http.ResponseWriter, id string, err error) {
	switch {
	case errors.Is(err, meta.ErrNotFound):
		api.WriteError(w, http.StatusNotFound, api.CodeNotFound, "object "+id+" not found")
	case errors.Is(err, errBadMetadata):
		api.WriteError(w, http.StatusBadRequest, api.CodeBadRequest, err.Error())
	default:
		c.log.Error("metadata store error", "object_id", id, "err", err)
		api.WriteError(w, http.StatusInternalServerError, api.CodeInternal, "metadata store error")
	}
}

// parseMetadata parses a JSON object of custom metadata. Values must be
// strings; with allowNull a null value means "remove this key" (JSON merge
// patch semantics).
func parseMetadata(body []byte, allowNull bool) (map[string]*string, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil || raw == nil {
		return nil, fmt.Errorf("%w: body must be a JSON object of string values", errBadMetadata)
	}
	if len(raw) > maxMetaKeys {
		return nil, fmt.Errorf("%w: at most %d keys allowed", errBadMetadata, maxMetaKeys)
	}
	out := make(map[string]*string, len(raw))
	for k, v := range raw {
		if k == "" || len(k) > maxMetaKeyLen || !utf8.ValidString(k) {
			return nil, fmt.Errorf("%w: keys must be 1-%d bytes of UTF-8", errBadMetadata, maxMetaKeyLen)
		}
		if string(v) == "null" {
			if !allowNull {
				return nil, fmt.Errorf("%w: value of %q must be a string", errBadMetadata, k)
			}
			out[k] = nil
			continue
		}
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return nil, fmt.Errorf("%w: value of %q must be a string", errBadMetadata, k)
		}
		if len(s) > maxMetaValueLen {
			return nil, fmt.Errorf("%w: value of %q exceeds %d bytes", errBadMetadata, k, maxMetaValueLen)
		}
		out[k] = &s
	}
	return out, nil
}

func queryInt(r *http.Request, name string, def, lo, hi int) (int, error) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("%s must be an integer between %d and %d", name, lo, hi)
	}
	return n, nil
}
