// Command worker runs a storage worker.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Dd1235/objstore/internal/envflag"
	"github.com/Dd1235/objstore/internal/worker"
	"github.com/Dd1235/objstore/internal/worker/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		id            = flag.String("id", envflag.String("WORKER_ID", ""), "worker ID (env WORKER_ID)")
		listen        = flag.String("listen", envflag.String("LISTEN_ADDR", ":8080"), "listen address (env LISTEN_ADDR)")
		addr          = flag.String("addr", envflag.String("WORKER_ADDR", ""), "URL the controller uses to reach this worker; default http://<id>:<port> (env WORKER_ADDR)")
		dataDir       = flag.String("data", envflag.String("DATA_DIR", "/data"), "data directory (env DATA_DIR)")
		controllerURL = flag.String("controller", envflag.String("CONTROLLER_URL", "http://controller:8080"), "controller base URL (env CONTROLLER_URL)")
		scrubEvery    = flag.Duration("scrub-interval", envflag.Duration("SCRUB_INTERVAL", 5*time.Minute), "time between full scrubs, 0 to disable (env SCRUB_INTERVAL)")
		logLevel      = flag.String("log-level", envflag.String("LOG_LEVEL", "info"), "debug, info, warn or error (env LOG_LEVEL)")
	)
	flag.Parse()
	if *id == "" {
		return errors.New("worker ID is required (-id or WORKER_ID)")
	}
	if *addr == "" {
		_, port, err := net.SplitHostPort(*listen)
		if err != nil {
			return fmt.Errorf("listen address: %w", err)
		}
		*addr = "http://" + *id + ":" + port
	}
	log := envflag.Logger(*logLevel).With("worker_id", *id)

	st, err := store.Open(*dataDir)
	if err != nil {
		return fmt.Errorf("open store: %w", err)
	}
	count, used := st.Stats()
	log.Info("store opened", "dir", *dataDir, "chunks", count, "bytes", used)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:              *listen,
		Handler:           (&worker.Server{ID: *id, Store: st, Log: log}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	agent := &worker.Agent{
		WorkerID:      *id,
		Addr:          *addr,
		ControllerURL: *controllerURL,
		Stats:         st,
		Client:        &http.Client{Timeout: 2 * time.Second},
		Log:           log,
	}
	go agent.Run(ctx)
	go worker.RunScrubber(ctx, st, *scrubEvery, log)

	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", *listen, "advertised", *addr)
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
