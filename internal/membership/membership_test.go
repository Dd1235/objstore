package membership

import (
	"errors"
	"testing"
	"time"
)

var (
	t0  = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	cfg = Config{SuspectAfter: 3 * time.Second, DeadAfter: 20 * time.Second, RejoinAfter: 10 * time.Second}
)

func at(d time.Duration) time.Time { return t0.Add(d) }

func newTable(t *testing.T) *Table {
	t.Helper()
	tb, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return tb
}

func state(t *testing.T, tb *Table, id string) Worker {
	t.Helper()
	w, ok := tb.Get(id)
	if !ok {
		t.Fatalf("worker %s missing", id)
	}
	return w
}

func wantStates(t *testing.T, trs []Transition, want ...State) {
	t.Helper()
	if len(trs) != len(want) {
		t.Fatalf("got transitions %v, want targets %v", trs, want)
	}
	for i, tr := range trs {
		if tr.To != want[i] {
			t.Fatalf("transition %d = %v, want -> %v", i, tr, want[i])
		}
	}
}

// beat sends a heartbeat every second in [from, to) and ticks after each one.
func beat(t *testing.T, tb *Table, id string, from, to time.Duration) []Transition {
	t.Helper()
	var all []Transition
	for d := from; d < to; d += time.Second {
		trs, err := tb.Heartbeat(at(d), id, Stats{})
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, trs...)
		all = append(all, tb.Tick(at(d))...)
	}
	return all
}

func TestRegisterAndHeartbeat(t *testing.T) {
	tb := newTable(t)
	trs := tb.Register(t0, "w1", "http://w1:8080", Stats{ChunkCount: 2, UsedBytes: 10})
	wantStates(t, trs, Alive)
	if trs[0].From != Unknown {
		t.Fatalf("From = %v", trs[0].From)
	}
	w := state(t, tb, "w1")
	if w.State != Alive || w.Addr != "http://w1:8080" || w.ChunkCount != 2 || !w.CountsReplicas() || !w.Placeable() {
		t.Fatalf("worker = %+v", w)
	}

	trs, err := tb.Heartbeat(at(time.Second), "w1", Stats{ChunkCount: 5})
	if err != nil || len(trs) != 0 {
		t.Fatalf("heartbeat: %v %v", trs, err)
	}
	if w := state(t, tb, "w1"); w.ChunkCount != 5 || !w.LastHeartbeat.Equal(at(time.Second)) {
		t.Fatalf("stats not updated: %+v", w)
	}
}

func TestHeartbeatUnknownWorker(t *testing.T) {
	tb := newTable(t)
	if _, err := tb.Heartbeat(t0, "ghost", Stats{}); !errors.Is(err, ErrUnknownWorker) {
		t.Fatalf("got %v, want ErrUnknownWorker", err)
	}
}

func TestBriefPauseOnlySuspects(t *testing.T) {
	tb := newTable(t)
	tb.Register(t0, "w1", "a", Stats{})

	wantStates(t, tb.Tick(at(2999*time.Millisecond)))
	wantStates(t, tb.Tick(at(3*time.Second)), Suspect)

	w := state(t, tb, "w1")
	if !w.CountsReplicas() || w.Placeable() || !w.Readable() {
		t.Fatalf("suspect worker flags wrong: %+v", w)
	}

	// Silent for 19 s: still suspect, never dead, so no repair.
	wantStates(t, tb.Tick(at(19*time.Second)))
	trs, _ := tb.Heartbeat(at(19*time.Second), "w1", Stats{})
	wantStates(t, trs, Alive)
}

func TestDeathAfterGracePeriod(t *testing.T) {
	tb := newTable(t)
	tb.Register(t0, "w1", "a", Stats{})
	// One late tick crosses both thresholds.
	trs := tb.Tick(at(25 * time.Second))
	wantStates(t, trs, Suspect, Dead)
	w := state(t, tb, "w1")
	if w.CountsReplicas() || w.Readable() || w.Placeable() {
		t.Fatalf("dead worker flags wrong: %+v", w)
	}
}

func TestRejoinNeedsSteadyHeartbeats(t *testing.T) {
	tb := newTable(t)
	tb.Register(t0, "w1", "a", Stats{})
	tb.Tick(at(30 * time.Second))

	// docker start: the worker registers again.
	trs := tb.Register(at(40*time.Second), "w1", "b", Stats{})
	wantStates(t, trs, Suspect)
	w := state(t, tb, "w1")
	if !w.Rejoining || w.CountsReplicas() || w.Placeable() || w.Addr != "b" {
		t.Fatalf("rejoining worker = %+v", w)
	}

	// Steady heartbeats for just under RejoinAfter: still not trusted.
	wantStates(t, beat(t, tb, "w1", 41*time.Second, 50*time.Second))
	// The heartbeat at 50 s completes the window.
	wantStates(t, beat(t, tb, "w1", 50*time.Second, 51*time.Second), Alive)
	if w := state(t, tb, "w1"); w.Rejoining || !w.CountsReplicas() {
		t.Fatalf("after rejoin = %+v", w)
	}
}

func TestRejoinAbandonedOnSilence(t *testing.T) {
	tb := newTable(t)
	tb.Register(t0, "w1", "a", Stats{})
	tb.Tick(at(30 * time.Second))
	tb.Heartbeat(at(40*time.Second), "w1", Stats{})

	trs := tb.Tick(at(43 * time.Second))
	wantStates(t, trs, Dead)
	if w := state(t, tb, "w1"); w.Rejoining {
		t.Fatalf("dead worker still rejoining: %+v", w)
	}
}

func TestRejoinWindowRestartsAfterGap(t *testing.T) {
	tb := newTable(t)
	tb.Register(t0, "w1", "a", Stats{})
	tb.Tick(at(30 * time.Second))
	beat(t, tb, "w1", 40*time.Second, 45*time.Second)

	// A 4 s gap with no Tick in between: the window must start again.
	trs, _ := tb.Heartbeat(at(48*time.Second), "w1", Stats{})
	wantStates(t, trs)
	wantStates(t, beat(t, tb, "w1", 49*time.Second, 58*time.Second))
	wantStates(t, beat(t, tb, "w1", 58*time.Second, 59*time.Second), Alive)
}

func TestFlappingNeverDies(t *testing.T) {
	tb := newTable(t)
	tb.Register(t0, "w1", "a", Stats{})
	var all []Transition
	now := time.Duration(0)
	for range 20 {
		now += 5 * time.Second // longer than SuspectAfter, shorter than DeadAfter
		all = append(all, tb.Tick(at(now))...)
		trs, _ := tb.Heartbeat(at(now), "w1", Stats{})
		all = append(all, trs...)
	}
	for _, tr := range all {
		if tr.To == Dead {
			t.Fatalf("flapping worker declared dead: %v", tr)
		}
	}
	if len(all) != 40 {
		t.Fatalf("got %d transitions, want 40 suspect/alive flips", len(all))
	}
}

func TestDeterministicOrder(t *testing.T) {
	tb := newTable(t)
	for _, id := range []string{"w3", "w1", "w4", "w2"} {
		tb.Register(t0, id, id, Stats{})
	}
	trs := tb.Tick(at(5 * time.Second))
	for i, want := range []string{"w1", "w2", "w3", "w4"} {
		if trs[i].WorkerID != want {
			t.Fatalf("Tick order = %v", trs)
		}
		if tb.List()[i].ID != want {
			t.Fatalf("List order wrong")
		}
	}
}

func TestConfigValidate(t *testing.T) {
	bad := []Config{
		{},
		{SuspectAfter: time.Second, DeadAfter: 0, RejoinAfter: time.Second},
		{SuspectAfter: 5 * time.Second, DeadAfter: time.Second, RejoinAfter: time.Second},
		{SuspectAfter: time.Second, DeadAfter: 2 * time.Second, RejoinAfter: -1},
	}
	for _, c := range bad {
		if _, err := New(c); err == nil {
			t.Errorf("New(%+v) succeeded", c)
		}
	}
	if err := DefaultConfig().Validate(); err != nil {
		t.Fatal(err)
	}
}
