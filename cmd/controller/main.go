// Command controller runs the controller node.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Dd1235/objstore/internal/controller"
	"github.com/Dd1235/objstore/internal/envflag"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "controller:", err)
		os.Exit(1)
	}
}

func run() error {
	def := controller.DefaultConfig()
	var cfg controller.Config
	listen := flag.String("listen", envflag.String("LISTEN_ADDR", ":8080"), "listen address (env LISTEN_ADDR)")
	flag.DurationVar(&cfg.HeartbeatInterval, "heartbeat-interval", envflag.Duration("HEARTBEAT_INTERVAL", def.HeartbeatInterval), "worker heartbeat interval (env HEARTBEAT_INTERVAL)")
	flag.DurationVar(&cfg.TickInterval, "tick-interval", envflag.Duration("TICK_INTERVAL", def.TickInterval), "how often liveness timeouts are checked (env TICK_INTERVAL)")
	flag.DurationVar(&cfg.Membership.SuspectAfter, "suspect-after", envflag.Duration("SUSPECT_AFTER", def.Membership.SuspectAfter), "silence before a worker is suspect (env SUSPECT_AFTER)")
	flag.DurationVar(&cfg.Membership.DeadAfter, "dead-after", envflag.Duration("DEAD_AFTER", def.Membership.DeadAfter), "silence before a worker is dead and repair may start (env DEAD_AFTER)")
	flag.DurationVar(&cfg.Membership.RejoinAfter, "rejoin-after", envflag.Duration("REJOIN_AFTER", def.Membership.RejoinAfter), "steady heartbeats needed before a returning worker is alive (env REJOIN_AFTER)")
	logLevel := flag.String("log-level", envflag.String("LOG_LEVEL", "info"), "debug, info, warn or error (env LOG_LEVEL)")
	flag.Parse()

	log := envflag.Logger(*logLevel)
	c, err := controller.New(cfg, log)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go c.RunTicker(ctx.Done())

	srv := &http.Server{Addr: *listen, Handler: c.Handler(), ReadHeaderTimeout: 5 * time.Second}
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", *listen,
			"suspect_after", cfg.Membership.SuspectAfter, "dead_after", cfg.Membership.DeadAfter,
			"rejoin_after", cfg.Membership.RejoinAfter)
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
