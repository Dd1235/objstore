// Package ids creates and parses object and chunk identifiers.
//
// Object IDs are random version 4 UUIDs. Chunk IDs are "<object id>_<index>"
// with the index zero-padded to six digits. Randomness is injected so that
// tests and simulations can use a seeded source.
package ids

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// MaxLen is the longest chunk ID a worker will accept.
const MaxLen = 128

// NewObjectID returns a version 4 UUID built from 16 bytes read from r.
// Pass crypto/rand.Reader in production.
func NewObjectID(r io.Reader) (string, error) {
	var b [16]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}
	b[6] = b[6]&0x0f | 0x40 // version 4
	b[8] = b[8]&0x3f | 0x80 // RFC 4122 variant

	var s [36]byte
	hex.Encode(s[0:8], b[0:4])
	s[8] = '-'
	hex.Encode(s[9:13], b[4:6])
	s[13] = '-'
	hex.Encode(s[14:18], b[6:8])
	s[18] = '-'
	hex.Encode(s[19:23], b[8:10])
	s[23] = '-'
	hex.Encode(s[24:], b[10:])
	return string(s[:]), nil
}

// ValidObjectID reports whether s looks like an ID produced by NewObjectID.
func ValidObjectID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
				return false
			}
		}
	}
	return true
}

// ChunkID returns the ID of the chunk at position index within an object.
func ChunkID(objectID string, index int) string {
	return fmt.Sprintf("%s_%06d", objectID, index)
}

// ParseChunkID splits a chunk ID into its object ID and chunk index.
func ParseChunkID(id string) (objectID string, index int, err error) {
	i := strings.LastIndexByte(id, '_')
	if i <= 0 || i == len(id)-1 {
		return "", 0, errors.New("chunk id must have the form <object id>_<index>")
	}
	n, err := strconv.Atoi(id[i+1:])
	if err != nil || n < 0 {
		return "", 0, fmt.Errorf("bad chunk index in %q", id)
	}
	return id[:i], n, nil
}

// ValidChunkID reports whether id is safe to use as a file name on a worker:
// 1 to MaxLen characters from [A-Za-z0-9_.-], not starting with a dot.
func ValidChunkID(id string) bool {
	if len(id) == 0 || len(id) > MaxLen || id[0] == '.' {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		ok := 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9' ||
			c == '_' || c == '-' || c == '.'
		if !ok {
			return false
		}
	}
	return true
}
