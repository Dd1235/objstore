package worker

import (
	"context"
	"crypto/sha256"
	"log/slog"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Dd1235/objstore/internal/worker/store"
)

func TestScrubberFlagsCorruption(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		st, err := store.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		data := []byte("scrub me")
		if _, err := st.Put("c", data, sha256.Sum256(data)); err != nil {
			t.Fatal(err)
		}
		raw, _ := os.ReadFile(st.Path("c"))
		raw[len(raw)-1] ^= 0xff
		os.WriteFile(st.Path("c"), raw, 0o644)

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		go func() {
			RunScrubber(ctx, st, time.Minute, slog.New(slog.DiscardHandler))
			close(done)
		}()

		time.Sleep(59 * time.Second)
		synctest.Wait()
		if st.List()[0].Corrupt {
			t.Fatal("flagged before the first scrub")
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if !st.List()[0].Corrupt {
			t.Fatal("scrub did not flag the corrupt chunk")
		}
		cancel()
		<-done
	})
}
