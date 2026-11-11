package controller

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/membership"
	"github.com/Dd1235/objstore/internal/meta"
	"github.com/Dd1235/objstore/internal/repair"
	"github.com/Dd1235/objstore/internal/workerclient"
)

var (
	// errStale means an object changed (was deleted) after the repair plan
	// was made; the action is skipped.
	errStale = errors.New("object changed since the repair plan was made")
	// errUnchanged aborts a metadata update that would change nothing.
	errUnchanged = errors.New("no change")
)

// repairView is a snapshot of everything the repair planner needs.
type repairView struct {
	objects int
	chunks  []repair.Chunk
	members []membership.Worker
	workers []repair.Worker
	addrs   map[string]string
	invErr  map[string]error
}

// snapshot reads the chunk map of committed objects, then worker liveness and
// inventories. The metadata is read first, so every chunk it lists was stored
// before the inventories were taken and shows up in them.
func (c *Controller) snapshot(ctx context.Context) (repairView, error) {
	var v repairView
	objs, err := c.meta.List()
	if err != nil {
		return v, err
	}
	for _, o := range objs {
		if o.State != meta.Committed {
			continue
		}
		v.objects++
		for _, ch := range o.Chunks {
			v.chunks = append(v.chunks, repair.Chunk{
				ObjectID: o.ID, Index: ch.Index, ID: ch.ID, Size: ch.Size, SHA256: ch.SHA256,
				Replicas: slices.Clone(ch.Replicas),
			})
		}
	}

	c.mu.Lock()
	v.members = c.members.List()
	c.mu.Unlock()

	v.workers = make([]repair.Worker, len(v.members))
	v.addrs = make(map[string]string, len(v.members))
	errs := make([]error, len(v.members))
	var wg sync.WaitGroup
	for i, m := range v.members {
		v.addrs[m.ID] = m.Addr
		v.workers[i] = repair.Worker{ID: m.ID, Counts: m.CountsReplicas(), Placeable: m.Placeable(), Readable: m.Readable()}
		if !m.Readable() {
			continue
		}
		wg.Go(func() {
			wctx, cancel := context.WithTimeout(ctx, c.cfg.WorkerTimeout)
			defer cancel()
			inv, err := c.chunks.Inventory(wctx, m.Addr, m.ID)
			if err != nil {
				errs[i] = err
				return
			}
			copies := make(map[string]repair.Copy, len(inv.Chunks))
			for _, ci := range inv.Chunks {
				copies[ci.ChunkID] = repair.Copy{Size: ci.Size, SHA256: ci.SHA256, Corrupt: ci.Corrupt}
			}
			v.workers[i].Inventory = copies
		})
	}
	wg.Wait()
	v.invErr = make(map[string]error)
	for i, err := range errs {
		if err != nil {
			v.invErr[v.members[i].ID] = err
		}
	}
	return v, nil
}

// RepairOnce plans one repair pass and carries it out. It returns the plan
// (the state before the pass) and what was done.
func (c *Controller) RepairOnce(ctx context.Context) (repair.Plan, api.RepairSummary, error) {
	start := c.now()
	v, err := c.snapshot(ctx)
	if err != nil {
		return repair.Plan{}, api.RepairSummary{}, err
	}
	for id, err := range v.invErr {
		c.log.Warn("worker inventory unavailable", "worker_id", id, "err", err)
	}
	plan := repair.Make(v.chunks, v.workers, c.cfg.Replicas)

	sum := api.RepairSummary{At: start.UTC()}
	var last verifiedChunk // reused when a chunk needs more than one new copy
	for _, a := range plan.Actions {
		if ctx.Err() != nil {
			break
		}
		var err error
		switch a.Kind {
		case repair.CopyTo:
			if err = c.repairCopy(ctx, a, v.addrs, &last); err == nil {
				sum.Copied++
			}
		case repair.Adopt:
			err = c.updateReplicas(a.Chunk, func(r []string) []string { return appendNew(r, a.Worker) })
			if err == nil {
				c.log.Info("existing copy recorded", "chunk_id", a.Chunk.ID, "worker_id", a.Worker, "reason", a.Reason)
				sum.Adopted++
			}
		case repair.Drop:
			err = c.repairDrop(ctx, a, v.addrs)
			if err == nil {
				sum.Dropped++
			}
		}
		if errors.Is(err, errStale) {
			continue
		}
		if err != nil {
			sum.Failed++
			c.log.Error("repair step failed", "action", string(a.Kind), "chunk_id", a.Chunk.ID, "worker_id", a.Worker, "err", err)
		}
	}
	sum.DurationMS = c.now().Sub(start).Milliseconds()

	c.repairMu.Lock()
	c.lastRepair = &sum
	c.repairMu.Unlock()
	if len(plan.Actions) > 0 {
		c.log.Info("repair pass finished", "chunks", len(plan.Chunks),
			"under_replicated", plan.Count(repair.UnderReplicated), "over_replicated", plan.Count(repair.OverReplicated),
			"unavailable", plan.Count(repair.Unavailable),
			"copied", sum.Copied, "adopted", sum.Adopted, "dropped", sum.Dropped, "failed", sum.Failed)
	}
	return plan, sum, nil
}

// verifiedChunk is chunk data read from a worker and checked against the
// controller's checksum.
type verifiedChunk struct {
	id, from string
	data     []byte
}

// repairCopy writes a verified copy of a chunk to a.Worker and records it.
// vc holds the last chunk read; it is reused if it is the same chunk.
func (c *Controller) repairCopy(ctx context.Context, a repair.Action, addrs map[string]string, vc *verifiedChunk) error {
	if vc.id != a.Chunk.ID {
		*vc = verifiedChunk{}
		lastErr := errNoReplica
		for _, id := range a.Sources {
			data, _, err := c.fetchVerified(ctx, a.Chunk.ID, a.Chunk.Size, a.Chunk.SHA256, []workerRef{{id, addrs[id]}})
			if err == nil {
				*vc = verifiedChunk{id: a.Chunk.ID, from: id, data: data}
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			lastErr = err
		}
		if vc.id == "" {
			return lastErr
		}
	}
	addr := addrs[a.Worker]
	sum := sha256.Sum256(vc.data)
	wctx, cancel := context.WithTimeout(ctx, c.cfg.WorkerTimeout)
	err := c.putReplacing(wctx, addr, a.Chunk.ID, vc.data, sum)
	cancel()
	if err != nil {
		return err
	}
	// Record the new copy. If the object was deleted meanwhile, the copy is
	// not wanted: remove it again.
	err = c.updateReplicas(a.Chunk, func(r []string) []string { return appendNew(r, a.Worker) })
	if errors.Is(err, errStale) {
		dctx, cancel := context.WithTimeout(ctx, c.cfg.WorkerTimeout)
		if derr := c.chunks.Delete(dctx, addr, a.Chunk.ID); derr != nil && !errors.Is(derr, workerclient.ErrNotFound) {
			c.log.Warn("copy of deleted object left for garbage collection", "chunk_id", a.Chunk.ID, "worker_id", a.Worker, "err", derr)
		}
		cancel()
		return err
	}
	if err != nil {
		return err
	}
	c.log.Info("chunk copied", "chunk_id", a.Chunk.ID, "from", vc.from, "to", a.Worker, "reason", a.Reason)
	return nil
}

// repairDrop removes a copy from the metadata, then from the worker. A copy
// the worker cannot delete now is left for garbage collection.
func (c *Controller) repairDrop(ctx context.Context, a repair.Action, addrs map[string]string) error {
	err := c.updateReplicas(a.Chunk, func(r []string) []string {
		return slices.DeleteFunc(r, func(id string) bool { return id == a.Worker })
	})
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, c.cfg.WorkerTimeout)
	defer cancel()
	if err := c.chunks.Delete(wctx, addrs[a.Worker], a.Chunk.ID); err != nil && !errors.Is(err, workerclient.ErrNotFound) {
		c.log.Warn("dropped copy left for garbage collection", "chunk_id", a.Chunk.ID, "worker_id", a.Worker, "err", err)
	}
	c.log.Info("copy dropped", "chunk_id", a.Chunk.ID, "worker_id", a.Worker, "reason", a.Reason)
	return nil
}

// updateReplicas changes the replica list of a chunk of a committed object.
// It returns errStale if the object is gone or no longer committed.
func (c *Controller) updateReplicas(ch repair.Chunk, fn func([]string) []string) error {
	_, err := c.meta.Update(ch.ObjectID, func(o *meta.Object) error {
		if o.State != meta.Committed || ch.Index >= len(o.Chunks) || o.Chunks[ch.Index].ID != ch.ID {
			return errStale
		}
		cur := o.Chunks[ch.Index].Replicas
		next := fn(slices.Clone(cur))
		if slices.Equal(cur, next) {
			return errUnchanged
		}
		o.Chunks[ch.Index].Replicas = next
		return nil
	})
	switch {
	case errors.Is(err, errUnchanged):
		return nil
	case errors.Is(err, meta.ErrNotFound):
		return errStale
	}
	return err
}

func appendNew(list []string, id string) []string {
	if slices.Contains(list, id) {
		return list
	}
	return append(list, id)
}

// StartRepair runs repair passes in the background every RepairInterval
// until Close. The first pass waits RepairDelay, so that after a controller
// restart every running worker has registered before a missing one is
// treated as lost.
func (c *Controller) StartRepair() {
	c.background(func(ctx context.Context) {
		select {
		case <-ctx.Done():
			return
		case <-time.After(c.cfg.RepairDelay):
		}
		t := time.NewTicker(c.cfg.RepairInterval)
		defer t.Stop()
		for {
			if _, _, err := c.RepairOnce(ctx); err != nil && ctx.Err() == nil {
				c.log.Error("repair pass failed", "err", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
		}
	})
}

// replicationStatus handles GET /internal/replication. It checks the current
// state afresh (metadata and worker inventories) without changing anything.
func (c *Controller) replicationStatus(w http.ResponseWriter, r *http.Request) {
	v, err := c.snapshot(r.Context())
	if err != nil {
		c.log.Error("replication status", "err", err)
		api.WriteError(w, http.StatusInternalServerError, api.CodeInternal, "could not read metadata")
		return
	}
	plan := repair.Make(v.chunks, v.workers, c.cfg.Replicas)
	out := api.ReplicationStatus{
		ReplicationFactor: c.cfg.Replicas,
		CheckedAt:         c.now().UTC(),
		Objects:           v.objects,
		Chunks:            len(plan.Chunks),
		Healthy:           plan.Count(repair.Healthy),
		AtRisk:            plan.Count(repair.AtRisk),
		UnderReplicated:   plan.Count(repair.UnderReplicated),
		OverReplicated:    plan.Count(repair.OverReplicated),
		Unavailable:       plan.Count(repair.Unavailable),
		PendingActions:    len(plan.Actions),
		Workers:           []api.ReplicationWorker{},
		Problems:          []api.ChunkReplication{},
	}
	out.Maintained = out.Healthy+out.OverReplicated == out.Chunks

	copies := make(map[string]int)
	unknown := make(map[string]bool)
	for _, cr := range plan.Chunks {
		for _, id := range cr.Copies {
			copies[id]++
		}
		for _, id := range cr.Down {
			unknown[id] = true
		}
		if cr.Status != repair.Healthy && len(out.Problems) < 100 {
			out.Problems = append(out.Problems, api.ChunkReplication{
				ObjectID: cr.Chunk.ObjectID, ChunkID: cr.Chunk.ID, Status: string(cr.Status),
				Copies: nonNil(cr.Copies), Down: cr.Down, Bad: cr.Bad,
			})
		}
	}
	for _, m := range v.members {
		delete(unknown, m.ID)
		rw := api.ReplicationWorker{WorkerID: m.ID, State: m.State.String(), Rejoining: m.Rejoining, Copies: copies[m.ID]}
		if err := v.invErr[m.ID]; err != nil {
			rw.InventoryError = err.Error()
		}
		out.Workers = append(out.Workers, rw)
	}
	// Workers named in the metadata that have not registered since the
	// controller started.
	for _, id := range sortedKeys(unknown) {
		out.Workers = append(out.Workers, api.ReplicationWorker{WorkerID: id, State: membership.Unknown.String()})
	}

	c.repairMu.Lock()
	if c.lastRepair != nil {
		last := *c.lastRepair
		out.LastRepair = &last
	}
	c.repairMu.Unlock()
	api.WriteJSON(w, http.StatusOK, out)
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
