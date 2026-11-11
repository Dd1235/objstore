package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/Dd1235/objstore/internal/worker/store"
)

// RunScrubber re-verifies every stored chunk once per interval until ctx is
// cancelled. Corrupt chunks are flagged in the store, so they show up in the
// worker's inventory and are never served. A non-positive interval disables
// scrubbing.
func RunScrubber(ctx context.Context, s store.Store, interval time.Duration, log *slog.Logger) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		checked, corrupt, err := store.Scrub(ctx, s)
		switch {
		case err != nil && ctx.Err() == nil:
			log.Error("scrub failed", "err", err, "checked", checked)
		case len(corrupt) > 0:
			log.Warn("scrub found corrupt chunks", "checked", checked, "corrupt", corrupt)
		default:
			log.Debug("scrub clean", "checked", checked)
		}
	}
}
