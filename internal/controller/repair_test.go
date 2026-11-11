package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Dd1235/objstore/internal/api"
	"github.com/Dd1235/objstore/internal/membership"
	"github.com/Dd1235/objstore/internal/meta"
	"github.com/Dd1235/objstore/internal/repair"
	"github.com/Dd1235/objstore/internal/worker/store"
)

// advance moves the clock forward in one-second steps. Workers that are up
// heartbeat at every step, and the controller applies liveness timeouts.
func (cl *cluster) advance(d time.Duration) {
	cl.t.Helper()
	for end := cl.clk.now().Add(d); cl.clk.now().Before(end); {
		cl.clk.advance(time.Second)
		cl.mu.Lock()
		up := []string{}
		for id := range cl.workers {
			if !cl.down[id] {
				up = append(up, id)
			}
		}
		cl.mu.Unlock()
		cl.c.mu.Lock()
		for _, id := range up {
			if _, err := cl.c.members.Heartbeat(cl.clk.now(), id, membership.Stats{}); err != nil {
				cl.t.Fatal(err)
			}
		}
		cl.c.mu.Unlock()
		cl.c.Tick()
	}
}

func (cl *cluster) repair() api.RepairSummary {
	cl.t.Helper()
	_, sum, err := cl.c.RepairOnce(context.Background())
	if err != nil {
		cl.t.Fatal(err)
	}
	return sum
}

func (cl *cluster) status() api.ReplicationStatus {
	cl.t.Helper()
	rec := cl.do("GET", "/internal/replication", "")
	if rec.Code != http.StatusOK {
		cl.t.Fatalf("replication status: %d %s", rec.Code, rec.Body)
	}
	return decode[api.ReplicationStatus](cl.t, rec)
}

func (cl *cluster) object(id string) meta.Object {
	cl.t.Helper()
	o, err := cl.c.meta.Get(id)
	if err != nil {
		cl.t.Fatal(err)
	}
	return o
}

// holding returns the chunks of o that list worker as a replica.
func holding(o meta.Object, worker string) []meta.Chunk {
	var out []meta.Chunk
	for _, ch := range o.Chunks {
		if slices.Contains(ch.Replicas, worker) {
			out = append(out, ch)
		}
	}
	return out
}

// busiest returns the worker holding the most chunks of o.
func busiest(o meta.Object) string {
	best, n := "", -1
	for _, id := range []string{"worker1", "worker2", "worker3", "worker4"} {
		if k := len(holding(o, id)); k > n {
			best, n = id, k
		}
	}
	return best
}

func (cl *cluster) download(id string) []byte {
	cl.t.Helper()
	rec := cl.do("GET", "/api/v1/objects/"+id, "")
	if rec.Code != http.StatusOK {
		cl.t.Fatalf("download: %d %s", rec.Code, rec.Body)
	}
	return rec.Body.Bytes()
}

// checkExactCopies asserts that every chunk of o is recorded on exactly n
// distinct workers, each of which holds a valid copy, and that no other
// worker holds one.
func (cl *cluster) checkExactCopies(o meta.Object, n int) {
	cl.t.Helper()
	for _, ch := range o.Chunks {
		if len(ch.Replicas) != n || len(slices.Compact(slices.Sorted(slices.Values(ch.Replicas)))) != n {
			cl.t.Errorf("chunk %s recorded on %v, want %d distinct workers", ch.ID, ch.Replicas, n)
		}
		for id, st := range cl.stores {
			_, err := st.Verify(ch.ID)
			has := err == nil
			if want := slices.Contains(ch.Replicas, id); has != want {
				cl.t.Errorf("chunk %s on %s: valid copy present = %v, recorded = %v (err %v)", ch.ID, id, has, want, err)
			}
		}
	}
}

// TestRepairWorkerFailureAndReturn walks through the worker failure demo:
// a worker stops, becomes suspect (nothing copied), then dead (its chunks
// are copied elsewhere), then returns (surplus copies are trimmed, C1).
func TestRepairWorkerFailureAndReturn(t *testing.T) {
	cl := newCluster(t, 4)
	data := randomData(6*testChunk+100, 7)
	id := cl.upload("video.mp4", data, "").ObjectID
	if s := cl.status(); !s.Maintained || s.Healthy != 7 || s.Chunks != 7 {
		t.Fatalf("before failure: %+v", s)
	}

	o := cl.object(id)
	victim := busiest(o)
	affected := len(holding(o, victim))
	cl.setDown(victim, true)

	// Suspect: copies still count during the grace period; nothing moves.
	cl.advance(5 * time.Second)
	s := cl.status()
	if s.Maintained || s.AtRisk != affected || s.UnderReplicated != 0 {
		t.Fatalf("suspect: %+v", s)
	}
	if sum := cl.repair(); sum.Copied+sum.Dropped+sum.Adopted != 0 {
		t.Fatalf("repair acted during the grace period: %+v", sum)
	}

	// Dead: the affected chunks are under-replicated until repair runs.
	cl.advance(20 * time.Second)
	s = cl.status()
	if s.Maintained || s.UnderReplicated != affected || s.PendingActions != affected {
		t.Fatalf("dead: %+v", s)
	}
	if sum := cl.repair(); sum.Copied != affected || sum.Failed != 0 {
		t.Fatalf("repair: %+v, want %d copies", sum, affected)
	}
	cl.wait()
	s = cl.status()
	if !s.Maintained || s.Healthy != 7 || s.PendingActions != 0 {
		t.Fatalf("after repair: %+v", s)
	}
	if !bytes.Equal(cl.download(id), data) {
		t.Fatal("download differs after repair")
	}
	for _, ch := range holding(cl.object(id), victim) {
		if len(ch.Replicas) != 3 {
			t.Errorf("chunk %s replicas %v: want the dead worker plus two live copies", ch.ID, ch.Replicas)
		}
	}

	// The worker returns with its old copies: rejoining, then alive, then
	// the surplus copies are trimmed.
	cl.setDown(victim, false)
	cl.advance(2 * time.Second)
	if s := cl.status(); !s.Maintained || s.OverReplicated != 0 {
		t.Fatalf("rejoining worker's copies should not count yet: %+v", s)
	}
	cl.advance(10 * time.Second)
	if w, _ := cl.c.members.Get(victim); w.State != membership.Alive {
		t.Fatalf("%s is %s, want alive", victim, w.State)
	}
	if s := cl.status(); s.OverReplicated != affected {
		t.Fatalf("returned: %+v", s)
	}
	if sum := cl.repair(); sum.Dropped != affected || sum.Failed != 0 {
		t.Fatalf("trim: %+v", sum)
	}
	s = cl.status()
	if !s.Maintained || s.Healthy != 7 {
		t.Fatalf("after trim: %+v", s)
	}
	cl.checkExactCopies(cl.object(id), 2)
	if !bytes.Equal(cl.download(id), data) {
		t.Fatal("download differs after trim")
	}
	if sum := cl.repair(); sum != (api.RepairSummary{At: sum.At}) {
		t.Fatalf("steady state should need no repair: %+v", sum)
	}
}

func TestRepairWorkerReturnsEmpty(t *testing.T) {
	cl := newCluster(t, 4)
	id := cl.upload("a.bin", randomData(3*testChunk, 2), "").ObjectID
	o := cl.object(id)
	victim := busiest(o)
	cl.setDown(victim, true)
	cl.advance(25 * time.Second)
	cl.repair()

	// The worker comes back with an empty disk: its recorded copies are
	// missing, so they are dropped from the metadata.
	for _, ch := range holding(o, victim) {
		if err := cl.stores[victim].Delete(ch.ID); err != nil {
			t.Fatal(err)
		}
	}
	cl.setDown(victim, false)
	cl.advance(12 * time.Second)
	sum := cl.repair()
	if sum.Dropped != len(holding(o, victim)) || sum.Copied != 0 {
		t.Fatalf("repair: %+v", sum)
	}
	cl.checkExactCopies(cl.object(id), 2)
}

func TestRepairRewritesCorruptCopy(t *testing.T) {
	cl := newCluster(t, 4)
	data := randomData(2*testChunk, 3)
	id := cl.upload("a.bin", data, "").ObjectID
	ch := cl.object(id).Chunks[1]
	w := ch.Replicas[0]
	corruptFile(t, cl.stores[w].Path(ch.ID))
	// The scrubber finds it; the worker's inventory then reports it corrupt.
	if _, bad, err := store.Scrub(context.Background(), cl.stores[w]); err != nil || len(bad) != 1 {
		t.Fatalf("scrub: bad=%v err=%v", bad, err)
	}
	if s := cl.status(); s.UnderReplicated != 1 || len(s.Problems) != 1 || !slices.Equal(s.Problems[0].Bad, []string{w}) {
		t.Fatalf("status: %+v", s)
	}
	if sum := cl.repair(); sum.Copied != 1 {
		t.Fatalf("repair: %+v", sum)
	}
	if _, err := cl.stores[w].Verify(ch.ID); err != nil {
		t.Fatalf("copy on %s not rewritten: %v", w, err)
	}
	if got := cl.object(id).Chunks[1].Replicas; !slices.Equal(got, ch.Replicas) {
		t.Errorf("replicas changed from %v to %v by an in-place rewrite", ch.Replicas, got)
	}
	cl.checkExactCopies(cl.object(id), 2)
}

func TestRepairRestoresMissingCopy(t *testing.T) {
	cl := newCluster(t, 4)
	id := cl.upload("a.bin", randomData(testChunk, 4), "").ObjectID
	ch := cl.object(id).Chunks[0]
	if err := cl.stores[ch.Replicas[1]].Delete(ch.ID); err != nil {
		t.Fatal(err)
	}
	if sum := cl.repair(); sum.Copied != 1 {
		t.Fatalf("repair: %+v", sum)
	}
	cl.checkExactCopies(cl.object(id), 2)
}

func TestRepairAdoptsUnrecordedCopy(t *testing.T) {
	cl := newCluster(t, 4)
	data := randomData(testChunk, 5)
	id := cl.upload("a.bin", data, "").ObjectID
	ch := cl.object(id).Chunks[0]
	// A third worker already has a valid copy that the metadata does not
	// record (as if a repair's metadata update had been lost).
	var spare string
	for _, w := range []string{"worker1", "worker2", "worker3", "worker4"} {
		if !slices.Contains(ch.Replicas, w) {
			spare = w
			break
		}
	}
	if _, err := cl.stores[spare].Put(ch.ID, data, sha256.Sum256(data)); err != nil {
		t.Fatal(err)
	}
	cl.setDown(ch.Replicas[1], true)
	cl.advance(25 * time.Second)
	if sum := cl.repair(); sum.Adopted != 1 || sum.Copied != 0 {
		t.Fatalf("repair: %+v", sum)
	}
	if got := cl.object(id).Chunks[0].Replicas; !slices.Contains(got, spare) {
		t.Errorf("replicas %v do not include adopted copy on %s", got, spare)
	}
}

func TestRepairNoReadableCopy(t *testing.T) {
	cl := newCluster(t, 4)
	id := cl.upload("a.bin", randomData(testChunk, 6), "").ObjectID
	ch := cl.object(id).Chunks[0]
	cl.setDown(ch.Replicas[0], true)
	cl.setDown(ch.Replicas[1], true)
	cl.advance(25 * time.Second)
	s := cl.status()
	if s.Unavailable != 1 || s.Maintained {
		t.Fatalf("status: %+v", s)
	}
	if sum := cl.repair(); sum.Copied != 0 || sum.Failed != 0 {
		t.Fatalf("repair: %+v", sum)
	}
}

func TestRepairCopyOfDeletedObjectIsRemoved(t *testing.T) {
	cl := newCluster(t, 4)
	data := randomData(testChunk, 8)
	id := cl.upload("a.bin", data, "").ObjectID
	o := cl.object(id)
	ch := o.Chunks[0]
	var spare string
	for _, w := range []string{"worker1", "worker2", "worker3", "worker4"} {
		if !slices.Contains(ch.Replicas, w) {
			spare = w
			break
		}
	}
	// The object is deleted after the plan was made: the new copy must not
	// stay behind.
	if _, err := cl.c.meta.Update(id, func(o *meta.Object) error { o.State = meta.Deleted; return nil }); err != nil {
		t.Fatal(err)
	}
	a := repair.Action{
		Kind:    repair.CopyTo,
		Chunk:   repair.Chunk{ObjectID: id, Index: 0, ID: ch.ID, Size: ch.Size, SHA256: ch.SHA256, Replicas: ch.Replicas},
		Worker:  spare,
		Sources: ch.Replicas,
	}
	addrs := map[string]string{}
	for w := range cl.workers {
		addrs[w] = "http://" + w + ":8080"
	}
	if err := cl.c.repairCopy(context.Background(), a, addrs, &verifiedChunk{}); !errors.Is(err, errStale) {
		t.Fatalf("repairCopy = %v, want errStale", err)
	}
	if _, err := cl.stores[spare].Verify(ch.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("copy on %s still present: %v", spare, err)
	}
}

func TestRepairLoopStartsAfterDelay(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cl := newCluster(t, 4)
		cl.c.StartRepair()
		time.Sleep(cl.c.cfg.RepairDelay - time.Second)
		synctest.Wait()
		if s := cl.status(); s.LastRepair != nil {
			t.Fatalf("repair ran before the start delay: %+v", s.LastRepair)
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if s := cl.status(); s.LastRepair == nil {
			t.Fatal("no repair pass after the start delay")
		}
	})
}
