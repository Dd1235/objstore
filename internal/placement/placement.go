// Package placement decides which workers hold a chunk.
//
// It uses rendezvous (highest random weight) hashing: every worker gets a
// score derived from the chunk ID and the worker ID, and the chunk goes to the
// highest-scoring workers. Placement is deterministic, spreads chunks evenly,
// and when a worker disappears only the chunks it held move.
package placement

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"strings"
)

// Rank returns workers ordered from most to least preferred for chunkID.
// The first n entries are the chunk's home; the rest are fallbacks in order.
// The input slice is not modified.
func Rank(chunkID string, workers []string) []string {
	type scored struct {
		id    string
		score uint64
	}
	s := make([]scored, len(workers))
	for i, w := range workers {
		s[i] = scored{w, score(chunkID, w)}
	}
	slices.SortFunc(s, func(a, b scored) int {
		if a.score != b.score {
			if a.score > b.score {
				return -1
			}
			return 1
		}
		return strings.Compare(a.id, b.id)
	})
	out := make([]string, len(s))
	for i := range s {
		out[i] = s[i].id
	}
	return out
}

func score(chunkID, workerID string) uint64 {
	h := sha256.New()
	h.Write([]byte(chunkID))
	h.Write([]byte{0})
	h.Write([]byte(workerID))
	var sum [sha256.Size]byte
	return binary.BigEndian.Uint64(h.Sum(sum[:0]))
}
