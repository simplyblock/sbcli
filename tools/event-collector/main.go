package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	level := slog.LevelInfo
	switch strings.ToUpper(os.Getenv("SIMPLYBLOCK_LOG_LEVEL")) {
	case "DEBUG":
		level = slog.LevelDebug
	case "WARNING", "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(log)

	clusterFile := os.Getenv("FDB_CLUSTER_FILE")
	if clusterFile == "" {
		clusterFile = "/etc/foundationdb/fdb.cluster"
	}
	store, err := newStore(clusterFile)
	if err != nil {
		log.Error("cannot open the store", "err", err)
		os.Exit(1)
	}
	tlsCfg := TLSConfigFromEnv()
	client, err := tlsCfg.HTTPClient()
	if err != nil {
		log.Error("cannot set up TLS", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Info("two-node event collector started", "tls", tlsCfg.Mode)
	(&Supervisor{Store: store, TLS: tlsCfg, Client: client, Interval: 5 * time.Second, Log: log}).Run(ctx)
	log.Info("two-node event collector stopped")
}
