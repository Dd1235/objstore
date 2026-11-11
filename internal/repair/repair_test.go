package repair

import (
	"reflect"
	"slices"
	"testing"

	"github.com/Dd1235/objstore/internal/placement"
)

const sum = "aa11"

func chunk(replicas ...string) Chunk {
	return Chunk{ObjectID: "obj", Index: 0, ID: "obj_000000", Size: 10, SHA256: sum, Replicas: replicas}
}

func good() Copy    { return Copy{Size: 10, SHA256: sum} }
func corrupt() Copy { return Copy{Size: 10, SHA256: sum, Corrupt: true} }

// alive returns an alive worker whose inventory holds the given copies of
// the test chunk (none: the chunk is missing).
func alive(id string, copies ...Copy) Worker {
	inv := map[string]Copy{}
	if len(copies) > 0 {
		inv["obj_000000"] = copies[0]
	}
	return Worker{ID: id, Counts: true, Placeable: true, Readable: true, Inventory: inv}
}

func suspect(id string) Worker { return Worker{ID: id, Counts: true, Readable: true} }
func dead(id string) Worker    { return Worker{ID: id} }
func rejoining(id string) Worker {
	return Worker{ID: id, Readable: true, Inventory: map[string]Copy{"obj_000000": good()}}
}

type act struct {
	Kind    Kind
	Worker  string
	Sources []string
}

func acts(p Plan) []act {
	var out []act
	for _, a := range p.Actions {
		out = append(out, act{a.Kind, a.Worker, a.Sources})
	}
	return out
}

func check(t *testing.T, p Plan, status Status, want []act) {
	t.Helper()
	if len(p.Chunks) != 1 {
		t.Fatalf("got %d chunk reports, want 1", len(p.Chunks))
	}
	if got := p.Chunks[0].Status; got != status {
		t.Errorf("status = %s, want %s", got, status)
	}
	if got := acts(p); !reflect.DeepEqual(got, want) {
		t.Errorf("actions = %+v, want %+v", got, want)
	}
}

// ranked returns the workers in rendezvous order for the test chunk.
func ranked(ids ...string) []string { return placement.Rank("obj_000000", ids) }

func TestHealthy(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), alive("w2", good()), alive("w3"), alive("w4")}, 2)
	check(t, p, Healthy, nil)
	if c := p.Chunks[0].Copies; !slices.Equal(c, []string{"w1", "w2"}) {
		t.Errorf("copies = %v", c)
	}
}

func TestSuspectCopyIsAtRiskButNotCopied(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), suspect("w2"), alive("w3"), alive("w4")}, 2)
	check(t, p, AtRisk, nil)
}

func TestDeadCopyIsReplaced(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), dead("w2"), alive("w3"), alive("w4")}, 2)
	target := ranked("w3", "w4")[0]
	check(t, p, UnderReplicated, []act{{CopyTo, target, []string{"w1"}}})
	if d := p.Chunks[0].Down; !slices.Equal(d, []string{"w2"}) {
		t.Errorf("down = %v, want [w2]", d)
	}
}

func TestUnknownWorkerCountsAsDown(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "gone")}, []Worker{alive("w1", good()), alive("w3"), alive("w4")}, 2)
	target := ranked("w3", "w4")[0]
	check(t, p, UnderReplicated, []act{{CopyTo, target, []string{"w1"}}})
}

func TestRejoiningWorkerIsLastSource(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), rejoining("w2"), alive("w3"), alive("w4")}, 2)
	target := ranked("w3", "w4")[0]
	check(t, p, UnderReplicated, []act{{CopyTo, target, []string{"w1", "w2"}}})
}

func TestOnlyRejoiningCopyLeft(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{dead("w1"), rejoining("w2"), alive("w3"), alive("w4")}, 2)
	r := ranked("w3", "w4")
	check(t, p, UnderReplicated, []act{{CopyTo, r[0], []string{"w2"}}, {CopyTo, r[1], []string{"w2"}}})
}

func TestNoReadableCopy(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{dead("w1"), dead("w2"), alive("w3"), alive("w4")}, 2)
	check(t, p, Unavailable, nil)
}

func TestCorruptCopyRewrittenInPlace(t *testing.T) {
	// w2's copy is corrupt; it is rewritten even if another worker ranks higher.
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), alive("w2", corrupt()), alive("w3"), alive("w4")}, 2)
	check(t, p, UnderReplicated, []act{{CopyTo, "w2", []string{"w1"}}})
	if b := p.Chunks[0].Bad; !slices.Equal(b, []string{"w2"}) {
		t.Errorf("bad = %v, want [w2]", b)
	}
}

func TestWrongChecksumIsBad(t *testing.T) {
	other := Copy{Size: 10, SHA256: "bb22"}
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), alive("w2", other), alive("w3"), alive("w4")}, 2)
	check(t, p, UnderReplicated, []act{{CopyTo, "w2", []string{"w1"}}})
}

func TestMissingCopyDroppedWhenEnoughRemain(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w2", "w3")}, []Worker{alive("w1", good()), alive("w2", good()), alive("w3"), alive("w4")}, 2)
	check(t, p, Healthy, []act{{Drop, "w3", nil}})
}

func TestSurplusTrimmedByRank(t *testing.T) {
	// C1: w2 returned after its chunk was copied to w3.
	p := Make([]Chunk{chunk("w1", "w2", "w3")}, []Worker{alive("w1", good()), alive("w2", good()), alive("w3", good()), alive("w4")}, 2)
	drop := ranked("w1", "w2", "w3")[2]
	check(t, p, OverReplicated, []act{{Drop, drop, nil}})
}

func TestSurplusKeptUntilSteady(t *testing.T) {
	w3 := suspect("w3")
	w3.Inventory = map[string]Copy{"obj_000000": good()}
	p := Make([]Chunk{chunk("w1", "w2", "w3")}, []Worker{alive("w1", good()), alive("w2", good()), w3, alive("w4")}, 2)
	check(t, p, OverReplicated, nil)

	unknown := alive("w3")
	unknown.Inventory = nil
	p = Make([]Chunk{chunk("w1", "w2", "w3")}, []Worker{alive("w1", good()), alive("w2", good()), unknown, alive("w4")}, 2)
	check(t, p, OverReplicated, nil)
}

func TestUnknownInventoryPresumedValid(t *testing.T) {
	w2 := alive("w2")
	w2.Inventory = nil
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), w2, alive("w3"), alive("w4")}, 2)
	check(t, p, Healthy, nil)
}

func TestUnrecordedValidCopyAdopted(t *testing.T) {
	// w4 holds a valid copy the metadata does not know about (for example a
	// repair whose metadata update was lost). It is adopted instead of copying.
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), dead("w2"), alive("w3"), alive("w4", good())}, 2)
	check(t, p, UnderReplicated, []act{{Adopt, "w4", nil}})
}

func TestUnrecordedCorruptCopyNotAdopted(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), dead("w2"), alive("w3"), alive("w4", corrupt())}, 2)
	target := ranked("w3", "w4")[0]
	check(t, p, UnderReplicated, []act{{CopyTo, target, []string{"w1"}}})
}

func TestTooFewWorkers(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w2")}, []Worker{alive("w1", good()), dead("w2"), dead("w3"), dead("w4")}, 2)
	check(t, p, UnderReplicated, nil)
}

func TestDuplicateReplicasIgnored(t *testing.T) {
	p := Make([]Chunk{chunk("w1", "w1", "w2")}, []Worker{alive("w1", good()), alive("w2", good()), alive("w3")}, 2)
	check(t, p, Healthy, nil)
}

func TestDeterministicAcrossWorkerOrder(t *testing.T) {
	chunks := []Chunk{chunk("w1", "w2"), {ObjectID: "obj", Index: 1, ID: "obj_000001", Size: 10, SHA256: sum, Replicas: []string{"w2", "w3"}}}
	workers := []Worker{alive("w1", good()), dead("w2"), alive("w3"), alive("w4"), alive("w5")}
	a := Make(chunks, workers, 2)
	rev := slices.Clone(workers)
	slices.Reverse(rev)
	b := Make(chunks, rev, 2)
	if !reflect.DeepEqual(a, b) {
		t.Errorf("plan depends on worker order:\n%+v\n%+v", a, b)
	}
	if a.Count(UnderReplicated) != 1 || a.Count(Unavailable) != 1 {
		t.Errorf("counts: under=%d unavailable=%d", a.Count(UnderReplicated), a.Count(Unavailable))
	}
}
