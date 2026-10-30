// Command gateway is the HTTP front door: a stateless client of the
// coordinator (metadata) and the storage nodes (bytes).
//
//	gateway          run the gateway
//	gateway health   exit 0 if the gateway on GATEWAY_LISTEN_ADDR answers
//	                 /healthz, else 1 (the image is distroless, so this is the
//	                 Compose healthcheck)
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Ayomide1x/object-store/gateway/internal/httpapi"
	"github.com/Ayomide1x/object-store/gateway/internal/webui"
	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// shutdownGrace is how long in-flight requests get to finish on SIGTERM. It
// is under Docker's default 10 s stop timeout; a transfer still running then
// is cut off, and an upload cut off never commits.
const shutdownGrace = 8 * time.Second

// config is read from environment variables, documented in docker-compose.yml.
type config struct {
	listenAddr         string
	coordinatorAddr    string
	coordinatorTimeout time.Duration
	api                httpapi.Config
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "health" {
		if err := checkHealth(os.Getenv("GATEWAY_LISTEN_ADDR"), os.Getenv("GATEWAY_ALLOWED_HOSTS")); err != nil {
			fmt.Fprintln(os.Stderr, "unhealthy:", err)
			os.Exit(1)
		}
		return
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log); err != nil {
		log.Error("gateway exited", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	cfg, err := loadConfig()
	if err != nil {
		return err
	}
	// One connection to the coordinator, shared by all requests. It holds no
	// request state; grpc-go reconnects it if the coordinator restarts.
	conn, err := grpc.NewClient(cfg.coordinatorAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(httpapi.CoordinatorDeadline(cfg.coordinatorTimeout)))
	if err != nil {
		return fmt.Errorf("dial %s: %w", cfg.coordinatorAddr, err)
	}
	defer conn.Close()

	if !webui.Built() {
		log.Warn("web UI not built into this binary: / answers 503 (run make web, then rebuild)")
	}
	api := httpapi.New(objectstorev1.NewCoordinatorServiceClient(conn), cfg.api, log)
	srv := &http.Server{
		Addr:    cfg.listenAddr,
		Handler: api.Handler(),
		// Against clients that open a connection and send headers slowly.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    64 << 10,
		// No ReadTimeout or WriteTimeout: they bound a whole request, and any
		// fixed value either cuts off a large transfer or bounds nothing.
		// Handlers set per-chunk deadlines (StallTimeout) instead.
		ErrorLog: slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	serveErr := make(chan error, 1)
	go func() {
		log.Info("gateway listening", "addr", cfg.listenAddr, "coordinator", cfg.coordinatorAddr,
			"max_concurrent_uploads", cfg.api.MaxUploads, "max_concurrent_downloads", cfg.api.MaxDownloads,
			"stall_timeout", cfg.api.StallTimeout.String(), "allowed_hosts", cfg.api.AllowedHosts)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		return fmt.Errorf("serve: %w", err)
	case <-ctx.Done():
		log.Info("shutting down")
		sctx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		if err := srv.Shutdown(sctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("shutdown: %w", err)
		}
		return nil
	}
}

// checkHealth asks the local gateway's /healthz. It says only that the
// process is serving HTTP, not that the coordinator is reachable: a
// coordinator outage must not get healthy gateways restarted.
//
// It connects to the listen address but sends the first allowed host as
// Host, so the probe passes the Host check like any browser request; there
// is no exemption for /healthz.
func checkHealth(listenAddr, allowedHosts string) error {
	if listenAddr == "" {
		return fmt.Errorf("GATEWAY_LISTEN_ADDR is required")
	}
	if strings.HasPrefix(listenAddr, ":") {
		listenAddr = "127.0.0.1" + listenAddr
	}
	hosts, err := httpapi.ParseAllowedHosts(allowedHosts)
	if err != nil {
		return fmt.Errorf("GATEWAY_ALLOWED_HOSTS: %w", err)
	}
	req, err := http.NewRequest(http.MethodGet, "http://"+listenAddr+"/healthz", nil)
	if err != nil {
		return err
	}
	req.Host = hosts[0]
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %s", resp.Status)
	}
	return nil
}

func loadConfig() (config, error) {
	cfg := config{
		listenAddr:      os.Getenv("GATEWAY_LISTEN_ADDR"),
		coordinatorAddr: os.Getenv("GATEWAY_COORDINATOR"),
	}
	if cfg.listenAddr == "" {
		return config{}, fmt.Errorf("GATEWAY_LISTEN_ADDR is required")
	}
	if cfg.coordinatorAddr == "" {
		return config{}, fmt.Errorf("GATEWAY_COORDINATOR is required")
	}
	var err error
	if cfg.api.AllowedHosts, err = httpapi.ParseAllowedHosts(os.Getenv("GATEWAY_ALLOWED_HOSTS")); err != nil {
		return config{}, fmt.Errorf("GATEWAY_ALLOWED_HOSTS: %w", err)
	}
	cfg.api.UI = webui.Handler()
	if cfg.api.MaxUploads, err = intEnv("GATEWAY_MAX_CONCURRENT_UPLOADS", 8); err != nil {
		return config{}, err
	}
	if cfg.api.MaxDownloads, err = intEnv("GATEWAY_MAX_CONCURRENT_DOWNLOADS", 16); err != nil {
		return config{}, err
	}
	if cfg.api.StallTimeout, err = durationEnv("GATEWAY_STALL_TIMEOUT", 30*time.Second); err != nil {
		return config{}, err
	}
	if cfg.api.ChunkWriteTimeout, err = durationEnv("GATEWAY_CHUNK_WRITE_TIMEOUT", 10*time.Second); err != nil {
		return config{}, err
	}
	if cfg.api.ChunkReadTimeout, err = durationEnv("GATEWAY_CHUNK_READ_TIMEOUT", 10*time.Second); err != nil {
		return config{}, err
	}
	if cfg.coordinatorTimeout, err = durationEnv("GATEWAY_COORDINATOR_TIMEOUT", 10*time.Second); err != nil {
		return config{}, err
	}
	return cfg, nil
}

// intEnv reads a positive integer from env, or returns def.
func intEnv(env string, def int) (int, error) {
	v := os.Getenv(env)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", env, v)
	}
	return n, nil
}

// durationEnv reads a positive Go duration such as "10s" from env, or returns def.
func durationEnv(env string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(env)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration such as 10s, got %q", env, v)
	}
	return d, nil
}
