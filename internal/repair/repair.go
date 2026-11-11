// Package repair decides what has to change so that every chunk ends up with
// exactly the required number of valid copies on distinct workers.
//
// Plan is a pure function: it takes the controller's chunk map, a snapshot of
// worker liveness and the chunk inventories reported by the workers, and
// returns a status for every chunk plus the actions that would restore the
// replication factor. It does no I/O and reads no clock, so it can be tested
// exhaustively and driven by a simulator; the controller executes the actions.
//
// Rules:
//   - A copy counts toward the replication factor only if its worker counts
//     (alive, or suspect within the grace period) and, when the worker's
//     inventory is known, the inventory lists the chunk with the right size
//     and checksum and not marked corrupt.
//   - Under-replicated chunks get new copies. A worker that already holds a
//     valid but unrecorded copy is adopted first, then a recorded worker whose
//     copy is corrupt or missing is rewritten in place, then the remaining
//     alive workers are used in rendezvous-hash order.
//   - Surplus copies (for example on a worker that returned after its chunks
//     were re-replicated, C1) are trimmed only in a steady state: every
//     inventory known and every copy on an alive worker. The copies kept are
//     the ones ranked highest by rendezvous hashing.
//   - Copies recorded on dead or unknown workers stay recorded: those workers
//     may come back with their data, and are reconciled when they do.
package repair

import (
	"slices"
	"sort"

	"github.com/Dd1235/objstore/internal/placement"
)

// Worker is the planner's view of one worker.
type Worker struct {
	ID        string
	Counts    bool // its copies count toward the replication factor
	Placeable bool // may receive new copies
	Readable  bool // worth trying to read from
	// Inventory lists the chunks the worker reported, by chunk ID. Nil means
	// the inventory is unknown (not fetched, or the fetch failed).
	Inventory map[string]Copy
}

// Copy is one entry of a worker's inventory.
type Copy struct {
	Size    int64
	SHA256  string // hex
	Corrupt bool
}

// Chunk is one chunk of a committed object, as recorded in the metadata.
type Chunk struct {
	ObjectID string
	Index    int
	ID       string
	Size     int64
	SHA256   string   // hex
	Replicas []string // workers recorded as holding a copy
}

// Status summarises a chunk's replication.
type Status string

const (
	// Healthy: the required copies, all on alive workers.
	Healthy Status = "healthy"
	// AtRisk: enough copies count, but some are on suspect workers that are
	// still inside their grace period; nothing is copied yet.
	AtRisk Status = "at_risk"
	// UnderReplicated: fewer copies count than required, and at least one
	// readable copy exists to copy from.
	UnderReplicated Status = "under_replicated"
	// OverReplicated: more valid copies than required.
	OverReplicated Status = "over_replicated"
	// Unavailable: no readable copy is known.
	Unavailable Status = "unavailable"
)

// Kind is the type of a repair action.
type Kind string

const (
	// CopyTo writes a copy to Worker, reading from the first of Sources that
	// returns data matching the chunk's checksum.
	CopyTo Kind = "copy"
	// Adopt records a valid copy that Worker already holds.
	Adopt Kind = "adopt"
	// Drop removes Worker from the chunk's replicas and deletes its copy.
	Drop Kind = "drop"
)

// Action is one step of a repair plan.
type Action struct {
	Kind    Kind
	Chunk   Chunk
	Worker  string
	Sources []string // CopyTo only: workers to read from, best first
	Reason  string
}

// ChunkReport is the replication state of one chunk.
type ChunkReport struct {
	Chunk  Chunk
	Status Status
	Copies []string // copies that count, in recorded order
	Down   []string // recorded copies on workers that do not count (dead, unknown, rejoining)
	Bad    []string // recorded copies a worker reports as corrupt, wrong or missing
}

// Plan is the result of planning one repair pass.
type Plan struct {
	Chunks  []ChunkReport
	Actions []Action
}

// Count returns how many chunks have status s.
func (p Plan) Count(s Status) int {
	n := 0
	for _, c := range p.Chunks {
		if c.Status == s {
			n++
		}
	}
	return n
}

// Make plans a repair pass for chunks given the workers' state. Chunks are
// processed in the order given; the result is deterministic.
func Make(chunks []Chunk, workers []Worker, replicas int) Plan {
	byID := make(map[string]Worker, len(workers))
	ids := make([]string, 0, len(workers))
	for _, w := range workers {
		byID[w.ID] = w
		ids = append(ids, w.ID)
	}
	sort.Strings(ids)

	var p Plan
	for _, ch := range chunks {
		rep, acts := planChunk(ch, byID, ids, replicas)
		p.Chunks = append(p.Chunks, rep)
		p.Actions = append(p.Actions, acts...)
	}
	return p
}

func (c Copy) matches(ch Chunk) bool {
	return !c.Corrupt && c.Size == ch.Size && c.SHA256 == ch.SHA256
}

func planChunk(ch Chunk, byID map[string]Worker, ids []string, replicas int) (ChunkReport, []Action) {
	rep := ChunkReport{Chunk: ch}
	var (
		valid    []string // counting copies confirmed by an inventory
		presumed []string // counting copies on workers whose inventory is unknown
		suspect  bool     // some counting copy is on a worker that is not alive
		sources  []string // readable copies worth trying, best first
		laterSrc []string
	)
	recorded := make(map[string]bool, len(ch.Replicas))
	for _, id := range ch.Replicas {
		if recorded[id] {
			continue
		}
		recorded[id] = true
		w, ok := byID[id]
		switch {
		case !ok || !w.Counts:
			rep.Down = append(rep.Down, id)
			if ok && w.Readable && !knownBad(w, ch) {
				laterSrc = append(laterSrc, id)
			}
		case w.Inventory == nil:
			presumed = append(presumed, id)
			rep.Copies = append(rep.Copies, id)
			sources = append(sources, id)
			suspect = suspect || !w.Placeable
		case knownBad(w, ch):
			rep.Bad = append(rep.Bad, id)
		default:
			valid = append(valid, id)
			rep.Copies = append(rep.Copies, id)
			suspect = suspect || !w.Placeable
		}
	}
	// Confirmed copies are the best sources, ahead of presumed ones.
	sources = append(slices.Clone(valid), sources...)

	// Valid copies on alive workers that the metadata does not record.
	var adoptable []string
	for _, id := range ids {
		w := byID[id]
		if recorded[id] || !w.Placeable || w.Inventory == nil {
			continue
		}
		if c, ok := w.Inventory[ch.ID]; ok && c.matches(ch) {
			adoptable = append(adoptable, id)
		}
	}
	sources = append(sources, adoptable...)
	sources = append(sources, laterSrc...)

	counting := len(rep.Copies)
	switch {
	case counting > replicas:
		rep.Status = OverReplicated
	case counting == replicas && suspect:
		rep.Status = AtRisk
	case counting == replicas:
		rep.Status = Healthy
	case len(sources) == 0:
		rep.Status = Unavailable
	default:
		rep.Status = UnderReplicated
	}

	var acts []Action
	if need := replicas - counting; need > 0 {
		acts = append(acts, addCopies(ch, byID, ids, recorded, rep.Copies, adoptable, sources, need)...)
		return rep, acts
	}

	// Enough copies count: forget recorded copies that are bad.
	for _, id := range rep.Bad {
		acts = append(acts, Action{Kind: Drop, Chunk: ch, Worker: id, Reason: "copy is corrupt or missing and enough valid copies exist"})
	}
	// Trim surplus only when every copy is confirmed and on an alive worker.
	if len(valid) > replicas && len(presumed) == 0 && !suspect {
		keep := placement.Rank(ch.ID, valid)[:replicas]
		for _, id := range valid {
			if !slices.Contains(keep, id) {
				acts = append(acts, Action{Kind: Drop, Chunk: ch, Worker: id, Reason: "surplus copy"})
			}
		}
	}
	return rep, acts
}

// knownBad reports whether w's inventory shows its copy of ch is unusable.
func knownBad(w Worker, ch Chunk) bool {
	if w.Inventory == nil {
		return false
	}
	c, ok := w.Inventory[ch.ID]
	return !ok || !c.matches(ch)
}

// addCopies picks up to need workers to receive a copy of ch.
func addCopies(ch Chunk, byID map[string]Worker, ids []string, recorded map[string]bool,
	counting, adoptable, sources []string, need int) []Action {
	var candidates []string
	for _, id := range ids {
		if byID[id].Placeable && !slices.Contains(counting, id) {
			candidates = append(candidates, id)
		}
	}
	ranked := placement.Rank(ch.ID, candidates)
	// Prefer adopting existing copies, then rewriting recorded copies in
	// place, then new workers; rendezvous order within each group.
	class := func(id string) int {
		switch {
		case slices.Contains(adoptable, id):
			return 0
		case recorded[id]:
			return 1
		default:
			return 2
		}
	}
	slices.SortStableFunc(ranked, func(a, b string) int { return class(a) - class(b) })

	var acts []Action
	for _, id := range ranked {
		if len(acts) == need {
			break
		}
		switch class(id) {
		case 0:
			acts = append(acts, Action{Kind: Adopt, Chunk: ch, Worker: id, Reason: "worker already holds a valid copy"})
		case 1:
			if len(sources) > 0 {
				acts = append(acts, Action{Kind: CopyTo, Chunk: ch, Worker: id, Sources: slices.Clone(sources), Reason: "recorded copy is corrupt or missing"})
			}
		default:
			if len(sources) > 0 {
				acts = append(acts, Action{Kind: CopyTo, Chunk: ch, Worker: id, Sources: slices.Clone(sources), Reason: "under-replicated"})
			}
		}
	}
	return acts
}
