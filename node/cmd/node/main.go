// Command node runs a storage node: it stores chunk files on local disk and
// serves StorageNodeService over gRPC.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"google.golang.org/grpc"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
	"github.com/Ayomide1x/object-store/node/internal/chunkstore"
	"github.com/Ayomide1x/object-store/node/internal/server"
)

// config is read from environment variables, documented in docker-compose.yml.
type config struct {
	nodeID     string
	listenAddr string
	dataDir    string
	maxReads   int
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log); err != nil {
		log.Error("node exited", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	log = log.With("node_id", cfg.nodeID)

	store, err := chunkstore.New(cfg.dataDir)
	if err != nil {
		return err
	}
	// Before listening: no write is in flight, so every temp file belongs to a
	// write that died with a previous process.
	removed, err := store.CleanTemps(ctx)
	if err != nil {
		return err
	}
	for _, name := range removed {
		log.Warn("removed stray temp file from interrupted write",
			"chunk_id", chunkstore.ChunkIDFromTempName(name), "file", name)
	}

	lis, err := net.Listen("tcp", cfg.listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.listenAddr, err)
	}

	srv := grpc.NewServer()
	objectstorev1.RegisterStorageNodeServiceServer(srv, server.New(cfg.nodeID, store, log, cfg.maxReads))

	serveErr := make(chan error, 1)
	go func() {
		log.Info("node listening", "addr", cfg.listenAddr, "data_dir", cfg.dataDir, "max_concurrent_reads", cfg.maxReads)
		serveErr <- srv.Serve(lis)
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
		srv.GracefulStop()
		return nil
	}
}

func loadConfig() (config, error) {
	cfg := config{
		nodeID:     os.Getenv("NODE_ID"),
		listenAddr: os.Getenv("NODE_LISTEN_ADDR"),
		dataDir:    os.Getenv("NODE_DATA_DIR"),
	}
	if cfg.nodeID == "" {
		return config{}, fmt.Errorf("NODE_ID is required")
	}
	if cfg.listenAddr == "" {
		return config{}, fmt.Errorf("NODE_LISTEN_ADDR is required")
	}
	if cfg.dataDir == "" {
		return config{}, fmt.Errorf("NODE_DATA_DIR is required")
	}
	cfg.maxReads = 32
	if v := os.Getenv("NODE_MAX_CONCURRENT_READS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			return config{}, fmt.Errorf("NODE_MAX_CONCURRENT_READS must be a positive integer, got %q", v)
		}
		cfg.maxReads = n
	}
	return cfg, nil
}
