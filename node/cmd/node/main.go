// Command node runs a storage node: it stores chunk files on local disk and
// serves StorageNodeService over gRPC.
//
//	node          run the node
//	node health   exit 0 if the node on NODE_LISTEN_ADDR reports SERVING, else 1
//	              (the image is distroless, so this is the Compose healthcheck)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

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
	if len(os.Args) > 1 && os.Args[1] == "health" {
		if err := checkHealth(); err != nil {
			fmt.Fprintln(os.Stderr, "unhealthy:", err)
			os.Exit(1)
		}
		return
	}

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
	// SERVING only now: temp cleanup is done and the listener is open.
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)

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

// checkHealth asks the local node's gRPC health service for its status.
func checkHealth() error {
	addr := os.Getenv("NODE_LISTEN_ADDR")
	if addr == "" {
		return fmt.Errorf("NODE_LISTEN_ADDR is required")
	}
	// ":9101" listens on all interfaces; probe it on loopback.
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("health check %s: %w", addr, err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return fmt.Errorf("status %s", resp.GetStatus())
	}
	return nil
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
