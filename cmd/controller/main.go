// Command controller runs the controller node.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Dd1235/objstore/internal/controller"
	"github.com/Dd1235/objstore/internal/envflag"
	"github.com/Dd1235/objstore/internal/meta"
	"github.com/Dd1235/objstore/internal/workerclient"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		os.Exit(1)
	}
}

func run() error {
	def := controller.DefaultConfig()
	cfg := def
	listen := flag.String("listen", envflag.String("LISTEN_ADDR", ":8080"), "listen address (env LISTEN_ADDR)")
	dataDir := flag.String("data", envflag.String("DATA_DIR", "/data"), "directory for the metadata database (env DATA_DIR)")
	flag.DurationVar(&cfg.WorkerTimeout, "worker-timeout", envflag.Duration("WORKER_TIMEOUT", def.WorkerTimeout), "limit on each call to a worker (env WORKER_TIMEOUT)")
	flag.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", envflag.Duration("HEARTBEAT_INTERVAL", def.HeartbeatInterval), "worker heartbeat interval (env HEARTBEAT_INTERVAL)")
	flag.DurationVar(&cfg.TickInterval, "tick-interval", envflag.Duration("TICK_INTERVAL", def.TickInterval), "how often liveness timeouts are checked (env TICK_INTERVAL)")
	flag.DurationVar(&cfg.Membership.SuspectAfter, "suspect-after", envflag.Duration("SUSPECT_AFTER", def.Membership.SuspectAfter), "silence before a worker is suspect (env SUSPECT_AFTER)")
	flag.DurationVar(&cfg.Membership.DeadAfter, "dead-after", envflag.Duration("DEAD_AFTER", def.Membership.DeadAfter), "silence before a worker is dead and repair may start (env DEAD_AFTER)")
	flag.DurationVar(&cfg.Membership.RejoinAfter, "rejoin-after", envflag.Duration("REJOIN_AFTER", def.Membership.RejoinAfter), "steady heartbeats needed before a returning worker is alive (env REJOIN_AFTER)")
	flag.DurationVar(&cfg.RepairInterval, "repair-interval", envflag.Duration("REPAIR_INTERVAL", def.RepairInterval), "time between repair passes (env REPAIR_INTERVAL)")
	flag.DurationVar(&cfg.RepairDelay, "repair-delay", envflag.Duration("REPAIR_DELAY", def.RepairDelay), "wait after start before the first repair pass (env REPAIR_DELAY)")
	logLevel := flag.String("log-level", envflag.String("LOG_LEVEL", "info"), "debug, info, warn or error (env LOG_LEVEL)")
	flag.Parse()

	log := envflag.Logger(*logLevel)
	if err := os.MkdirAll(*dataDir, 0o755); err != nil {
		return err
	}
	metaPath := filepath.Join(*dataDir, "meta.db")
	store, err := meta.OpenBolt(metaPath)
	if err != nil {
		return err
	}
	defer store.Close()
	log.Info("metadata store opened", "path", metaPath)

	c, err := controller.New(cfg, controller.Deps{
		Meta:   store,
		Chunks: &workerclient.Client{HTTP: &http.Client{}, MaxChunk: int64(cfg.ChunkSize)},
		Log:    log,
	})
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go c.RunTicker(ctx.Done())
	c.StartRepair()

	srv := &http.Server{Addr: *listen, Handler: c.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", *listen,
			"suspect_after", cfg.Membership.SuspectAfter, "dead_after", cfg.Membership.DeadAfter,
			"rejoin_after", cfg.Membership.RejoinAfter, "repair_interval", cfg.RepairInterval, "repair_delay", cfg.RepairDelay)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}
