package placement

import (
	"fmt"
	"slices"
	"testing"
)

var workers = []string{"worker1", "worker2", "worker3", "worker4"}

func TestRankIsPermutationAndDeterministic(t *testing.T) {
	in := slices.Clone(workers)
	a := Rank("obj_000001", in)
	b := Rank("obj_000001", []string{"worker4", "worker2", "worker1", "worker3"})
	if !slices.Equal(a, b) {
		t.Fatalf("input order changed result: %v vs %v", a, b)
	}
	if !slices.Equal(in, workers) {
		t.Fatal("input slice modified")
	}
	sorted := slices.Sorted(slices.Values(a))
	if !slices.Equal(sorted, workers) {
		t.Fatalf("not a permutation: %v", a)
	}
	if len(Rank("x", nil)) != 0 {
		t.Fatal("empty input should give empty output")
	}
}

func TestEvenSpread(t *testing.T) {
	const chunks = 20000
	count := map[string]int{}
	pairs := map[string]int{}
	for i := range chunks {
		top := Rank(fmt.Sprintf("obj_%06d", i), workers)[:2]
		count[top[0]]++
		count[top[1]]++
		pairs[top[0]+"+"+top[1]]++
	}
	want := chunks * 2 / len(workers)
	for _, w := range workers {
		if c := count[w]; c < want*9/10 || c > want*11/10 {
			t.Errorf("%s holds %d copies, want about %d", w, c, want)
		}
	}
	if len(pairs) != 12 { // every ordered pair of distinct workers is used
		t.Errorf("only %d distinct ordered pairs used", len(pairs))
	}
}

func TestRemovingWorkerMovesOnlyItsChunks(t *testing.T) {
	without := []string{"worker1", "worker3", "worker4"}
	for i := range 5000 {
		id := fmt.Sprintf("obj_%06d", i)
		before := Rank(id, workers)[:2]
		after := Rank(id, without)[:2]
		if !slices.Contains(before, "worker2") && !slices.Equal(before, after) {
			t.Fatalf("%s moved from %v to %v though worker2 did not hold it", id, before, after)
		}
		// The surviving copy stays where it was.
		for _, w := range before {
			if w != "worker2" && !slices.Contains(after, w) {
				t.Fatalf("%s: surviving copy on %s moved (%v -> %v)", id, w, before, after)
			}
		}
	}
}
