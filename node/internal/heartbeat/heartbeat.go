// Package heartbeat tells the coordinator, on an interval, that this node is
// alive. It runs in its own goroutine and never touches the serving path.
package heartbeat

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	mathrand "math/rand/v2"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// failureLogEvery bounds how often repeated failures are logged.
const failureLogEvery = 30 * time.Second

// Config describes one node's heartbeat loop.
type Config struct {
	NodeID     string
	InstanceID string // random per process; see NewInstanceID
	Interval   time.Duration
	Client     objectstorev1.CoordinatorServiceClient
	Log        *slog.Logger
}

// Dial connects to the coordinator for heartbeats.
//
// The passthrough resolver hands the address to the dialer unchanged, so every
// reconnect looks the name up afresh (Go's resolver keeps no cache): a
// coordinator restarted with a new IP costs at most one failed beat.
//
// gRPC's own reconnect backoff would grow to two minutes while the coordinator
// is down, and the node would then stay silent long after it came back, going
// falsely DEAD. Capping it at one interval keeps reconnects as frequent as
// beats, matching the loop's no-backoff rule.
func Dial(addr string, interval time.Duration) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: interval, Multiplier: 1, Jitter: 0.1, MaxDelay: interval},
			MinConnectTimeout: interval,
		}))
	if err != nil {
		return nil, fmt.Errorf("dial coordinator %s: %w", addr, err)
	}
	return conn, nil
}

// NewInstanceID returns a random ID for this process. It lets the coordinator
// tell a restart apart from two processes claiming one node ID.
func NewInstanceID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b) // crypto/rand.Read never fails
	return hex.EncodeToString(b)
}

// Run beats immediately, then every Interval (plus or minus 10% jitter), until
// ctx is cancelled. Each beat is bounded by Interval, so a hung coordinator
// costs at most one beat in flight; failures never stop the loop and never
// back off, because the coordinator needs beats promptly when it returns.
func Run(ctx context.Context, cfg Config) {
	log := cfg.Log.With("node_id", cfg.NodeID, "instance_id", cfg.InstanceID)
	var failing bool
	var lastFailureLog time.Time
	for {
		err := beat(ctx, cfg)
		switch {
		case err == nil && failing:
			log.Info("heartbeats reaching the coordinator again")
			failing = false
		case err != nil && ctx.Err() == nil:
			if !failing || time.Since(lastFailureLog) >= failureLogEvery {
				log.Warn("heartbeat failed; still serving, will keep trying", "error", err)
				lastFailureLog = time.Now()
			}
			failing = true
		}

		t := time.NewTimer(jitter(cfg.Interval))
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

func beat(ctx context.Context, cfg Config) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.Interval)
	defer cancel()
	_, err := cfg.Client.Heartbeat(ctx, &objectstorev1.HeartbeatRequest{NodeId: cfg.NodeID, InstanceId: cfg.InstanceID})
	if err != nil {
		return fmt.Errorf("heartbeat: %w", err)
	}
	return nil
}

// jitter spreads beats by up to 10% either way, so nodes started together
// don't stay in step.
func jitter(d time.Duration) time.Duration {
	spread := int64(d) / 10
	if spread <= 0 {
		return d
	}
	return d + time.Duration(mathrand.Int64N(2*spread+1)-spread)
}
