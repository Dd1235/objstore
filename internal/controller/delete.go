package controller

import (
	"context"
	"errors"
	"net/http"

	"github.com/Dd1235/objstore/internal/meta"
	"github.com/Dd1235/objstore/internal/workerclient"
)

// deleteObject handles DELETE /api/v1/objects/{id}. The record becomes a
// durable tombstone before 204 is returned, so the object is never served
// again (I6). Its chunks are removed once no download of it is in progress
// (C6); the tombstone is dropped when every copy is gone.
func (c *Controller) deleteObject(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("object_id")
	c.rmu.Lock()
	_, err := c.meta.Update(id, func(o *meta.Object) error {
		if o.State != meta.Committed {
			return meta.ErrNotFound
		}
		o.State = meta.Deleted
		o.ModifiedAt = c.now().UTC()
		return nil
	})
	purgeNow := false
	if err == nil {
		if c.readers[id] > 0 {
			c.deleting[id] = true
		} else {
			purgeNow = true
		}
	}
	c.rmu.Unlock()
	if err != nil {
		c.writeMetaError(w, id, err)
		return
	}
	c.log.Info("object deleted", "object_id", id, "deferred", !purgeNow)
	if purgeNow {
		c.purge(id)
	}
	w.WriteHeader(http.StatusNoContent)
}

// purge removes a tombstoned object's chunks in the background and then its
// record. Copies on unreachable workers are left for garbage collection.
func (c *Controller) purge(id string) {
	c.background(func(ctx context.Context) {
		o, err := c.meta.Get(id)
		if err != nil || o.State != meta.Deleted {
			return
		}
		if !c.deleteChunks(ctx, o) {
			c.log.Warn("some chunks of deleted object not removed yet", "object_id", id)
			return
		}
		if err := c.meta.Delete(id); err != nil && !errors.Is(err, meta.ErrNotFound) {
			c.log.Error("remove tombstone", "object_id", id, "err", err)
		}
	})
}

// deleteChunks asks every replica holder to delete the object's chunks. It
// reports whether every copy is known to be gone.
func (c *Controller) deleteChunks(ctx context.Context, o meta.Object) bool {
	done := true
	for _, ch := range o.Chunks {
		for _, wid := range ch.Replicas {
			addr, ok := c.addr(wid)
			if !ok {
				done = false
				continue
			}
			wctx, cancel := context.WithTimeout(ctx, c.cfg.WorkerTimeout)
			err := c.chunks.Delete(wctx, addr, ch.ID)
			cancel()
			if err != nil && !errors.Is(err, workerclient.ErrNotFound) {
				c.log.Warn("chunk delete failed", "chunk_id", ch.ID, "worker_id", wid, "err", err)
				done = false
			}
		}
	}
	return done
}
