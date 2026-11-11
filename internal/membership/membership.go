// Package membership tracks worker liveness on the controller.
//
// It is a pure state machine: callers pass the current time with every
// event, and it never starts goroutines or reads the clock. Workers move
// between three states:
//
//	alive   -> suspect  after SuspectAfter without a heartbeat
//	suspect -> alive    on the next heartbeat (a brief pause costs nothing)
//	suspect -> dead     after DeadAfter without a heartbeat (repair may start)
//	dead    -> suspect  on a heartbeat, marked as rejoining
//	suspect -> alive    once a rejoining worker has heartbeated steadily for RejoinAfter
//	suspect -> dead     if a rejoining worker misses SuspectAfter again
//
// The grace period before dead stops short outages from triggering
// re-replication, and the rejoin delay stops a flapping worker from bouncing
// straight back into use.
package membership

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// State is a worker's liveness state.
type State int

const (
	Unknown State = iota
	Alive
	Suspect
	Dead
)

func (s State) String() string {
	switch s {
	case Alive:
		return "alive"
	case Suspect:
		return "suspect"
	case Dead:
		return "dead"
	default:
		return "unknown"
	}
}

// Config holds the liveness timeouts.
type Config struct {
	SuspectAfter time.Duration // silence before alive -> suspect
	DeadAfter    time.Duration // silence before suspect -> dead
	RejoinAfter  time.Duration // steady heartbeats needed before a returning worker is alive
}

// DefaultConfig returns timeouts suited to the local Docker demo with
// one-second heartbeats.
func DefaultConfig() Config {
	return Config{SuspectAfter: 3 * time.Second, DeadAfter: 20 * time.Second, RejoinAfter: 10 * time.Second}
}

// Validate checks that the timeouts are positive and ordered.
func (c Config) Validate() error {
	if c.SuspectAfter <= 0 || c.DeadAfter <= 0 || c.RejoinAfter < 0 {
		return errors.New("membership: timeouts must be positive")
	}
	if c.DeadAfter < c.SuspectAfter {
		return errors.New("membership: DeadAfter must be at least SuspectAfter")
	}
	return nil
}

// Stats is the load a worker reports with each heartbeat.
type Stats struct {
	ChunkCount int
	UsedBytes  int64
}

// Worker is the controller's view of one worker.
type Worker struct {
	ID            string
	Addr          string
	State         State
	Rejoining     bool // returned after being dead and not yet trusted
	LastHeartbeat time.Time
	StateSince    time.Time
	RejoinStart   time.Time
	Stats
}

// CountsReplicas reports whether chunk copies on the worker count toward the
// replication factor. A suspect worker's copies still count (grace period);
// a dead or rejoining worker's do not.
func (w Worker) CountsReplicas() bool {
	return w.State == Alive || w.State == Suspect && !w.Rejoining
}

// Placeable reports whether new chunk copies may be placed on the worker.
func (w Worker) Placeable() bool { return w.State == Alive }

// Readable reports whether it is worth trying to read from the worker.
func (w Worker) Readable() bool { return w.State == Alive || w.State == Suspect }

// Transition records a state change.
type Transition struct {
	WorkerID string
	From, To State
	At       time.Time
	Reason   string
}

func (t Transition) String() string {
	return fmt.Sprintf("%s: %s -> %s (%s)", t.WorkerID, t.From, t.To, t.Reason)
}

// ErrUnknownWorker is returned for a heartbeat from a worker that has not
// registered, for example after a controller restart.
var ErrUnknownWorker = errors.New("unknown worker")

// Table holds the membership state of all workers. It is not safe for
// concurrent use; the caller serialises access.
type Table struct {
	cfg     Config
	workers map[string]*Worker
}

// New returns an empty table.
func New(cfg Config) (*Table, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Table{cfg: cfg, workers: make(map[string]*Worker)}, nil
}

// Config returns the table's timeouts.
func (t *Table) Config() Config { return t.cfg }

// Register records a worker announcing itself. A new worker starts alive; a
// known worker is treated as sending a heartbeat, so a dead worker that
// restarts must still pass the rejoin delay.
func (t *Table) Register(now time.Time, id, addr string, st Stats) []Transition {
	w, ok := t.workers[id]
	if !ok {
		w = &Worker{ID: id, Addr: addr, State: Alive, LastHeartbeat: now, StateSince: now, Stats: st}
		t.workers[id] = w
		return []Transition{{WorkerID: id, From: Unknown, To: Alive, At: now, Reason: "registered"}}
	}
	w.Addr = addr
	return t.heartbeat(now, w, st)
}

// Heartbeat records a heartbeat from a registered worker.
func (t *Table) Heartbeat(now time.Time, id string, st Stats) ([]Transition, error) {
	w, ok := t.workers[id]
	if !ok {
		return nil, ErrUnknownWorker
	}
	return t.heartbeat(now, w, st), nil
}

// Tick applies timeouts as of now. Transitions are returned in worker ID order.
func (t *Table) Tick(now time.Time) []Transition {
	var out []Transition
	for _, id := range t.ids() {
		out = append(out, t.evaluate(now, t.workers[id])...)
	}
	return out
}

// Get returns a copy of one worker's state.
func (t *Table) Get(id string) (Worker, bool) {
	w, ok := t.workers[id]
	if !ok {
		return Worker{}, false
	}
	return *w, true
}

// List returns copies of all workers sorted by ID.
func (t *Table) List() []Worker {
	out := make([]Worker, 0, len(t.workers))
	for _, id := range t.ids() {
		out = append(out, *t.workers[id])
	}
	return out
}

func (t *Table) ids() []string {
	ids := make([]string, 0, len(t.workers))
	for id := range t.workers {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, strings.Compare)
	return ids
}

func (t *Table) heartbeat(now time.Time, w *Worker, st Stats) []Transition {
	gap := now.Sub(w.LastHeartbeat)
	w.LastHeartbeat = now
	w.Stats = st

	var out []Transition
	switch w.State {
	case Suspect:
		if !w.Rejoining {
			out = append(out, t.set(w, Alive, now, "heartbeat resumed"))
		} else if gap >= t.cfg.SuspectAfter {
			w.RejoinStart = now // not steady: start the rejoin window again
		}
	case Dead:
		w.Rejoining = true
		w.RejoinStart = now
		out = append(out, t.set(w, Suspect, now, "heartbeat from dead worker, rejoining"))
	}
	return append(out, t.evaluate(now, w)...)
}

// evaluate applies time-based rules until the worker's state is stable.
func (t *Table) evaluate(now time.Time, w *Worker) []Transition {
	var out []Transition
	for {
		silent := now.Sub(w.LastHeartbeat)
		var next State
		var reason string
		switch {
		case w.State == Alive && silent >= t.cfg.SuspectAfter:
			next, reason = Suspect, "no heartbeat for "+silent.String()
		case w.State == Suspect && !w.Rejoining && silent >= t.cfg.DeadAfter:
			next, reason = Dead, "no heartbeat for "+silent.String()
		case w.State == Suspect && w.Rejoining && silent >= t.cfg.SuspectAfter:
			next, reason = Dead, "rejoin abandoned, no heartbeat for "+silent.String()
		case w.State == Suspect && w.Rejoining && now.Sub(w.RejoinStart) >= t.cfg.RejoinAfter:
			next, reason = Alive, "rejoined after "+now.Sub(w.RejoinStart).String()+" of steady heartbeats"
		default:
			return out
		}
		out = append(out, t.set(w, next, now, reason))
	}
}

func (t *Table) set(w *Worker, to State, now time.Time, reason string) Transition {
	tr := Transition{WorkerID: w.ID, From: w.State, To: to, At: now, Reason: reason}
	w.State = to
	w.StateSince = now
	if to == Alive || to == Dead {
		w.Rejoining = false
	}
	return tr
}
