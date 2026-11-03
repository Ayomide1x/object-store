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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

const (
	// failureLogEvery bounds how often repeated failures are logged.
	failureLogEvery = 30 * time.Second
	// keepaliveTime and keepaliveTimeout: ping after 10 s without reading
	// anything (grpc-go's floor), and drop the connection if the ack takes
	// over 3 s. The coordinator permits pings every 5 s, half this rate.
	//
	// On Linux grpc-go also sets TCP_USER_TIMEOUT to keepaliveTimeout, so a
	// connection whose sent data stays unacknowledged that long is closed by
	// the kernel. 3 s is longer than a beat's timeout (the 2 s interval), so a
	// beat on a half-open connection times out first and the loop's redial
	// runs, on Linux as on macOS (which has no TCP_USER_TIMEOUT). A shorter
	// timeout would fail the beat UNAVAILABLE first, skip the redial, and
	// leave reconnecting to the next beat, an interval later.
	keepaliveTime    = 10 * time.Second
	keepaliveTimeout = 3 * time.Second
)

// Config describes one node's heartbeat loop.
type Config struct {
	NodeID     string
	InstanceID string // random per process; see NewInstanceID
	Interval   time.Duration
	Addr       string // the coordinator's
	Log        *slog.Logger
}

// Loop is one node's heartbeat loop and its connection to the coordinator.
type Loop struct {
	cfg  Config
	conn *grpc.ClientConn // Run's alone once it starts; replaced on redial
}

// New returns cfg's loop. The connection is made by the first beat.
func New(cfg Config) (*Loop, error) {
	conn, err := Dial(cfg.Addr, cfg.Interval)
	if err != nil {
		return nil, err
	}
	return &Loop{cfg: cfg, conn: conn}, nil
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
//
// Keepalive pings, with or without a beat in flight, close a half-open
// connection within 13 s, under DEAD's 15 s. The loop's redial (Run) gets
// there first; keepalive is the backstop.
func Dial(addr string, interval time.Duration) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient("passthrough:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{BaseDelay: interval, Multiplier: 1, Jitter: 0.1, MaxDelay: interval},
			MinConnectTimeout: interval,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: keepaliveTime, Timeout: keepaliveTimeout, PermitWithoutStream: true,
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
// ctx is cancelled, then closes the connection. Each beat is bounded by
// Interval, so a hung coordinator costs at most one beat in flight; failures
// never stop the loop and never back off, because the coordinator needs beats
// promptly when it returns.
//
// A beat that times out drops its connection: the loop dials a new one and
// beats again at once. A beat has a whole interval to cross a LAN, so a
// timeout most likely means a half-open connection (the coordinator's end, or
// the path, gone without closing anything), which would fail every beat until
// TCP gives up, long after DEAD. The next beat on the new connection lands
// within one beat timeout plus one interval (and its jitter) of the last one
// that did: 4.2 s at a 2 s interval, under SUSPECT's 6 s. Against a hung
// coordinator the redial costs one dial per beat.
func (l *Loop) Run(ctx context.Context) {
	cfg := l.cfg
	log := cfg.Log.With("node_id", cfg.NodeID, "instance_id", cfg.InstanceID)
	defer func() { l.conn.Close() }() // the connection at return, not at defer
	var failing bool
	var lastFailureLog time.Time
	for {
		err := beat(ctx, cfg, l.conn)
		timedOut := status.Code(err) == codes.DeadlineExceeded && ctx.Err() == nil
		switch {
		case err == nil && failing:
			log.Info("heartbeats reaching the coordinator again")
			failing = false
		case err != nil && ctx.Err() == nil:
			if !failing || time.Since(lastFailureLog) >= failureLogEvery {
				log.Warn("heartbeat failed; still serving, will keep trying", "error", err, "redial", timedOut)
				lastFailureLog = time.Now()
			}
			failing = true
		}
		if timedOut && l.redial(log) {
			continue // beat again at once, on the new connection
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

// redial replaces the connection with a new one. If dialling fails (it can
// fail only on a malformed address), the old connection stays.
func (l *Loop) redial(log *slog.Logger) bool {
	conn, err := Dial(l.cfg.Addr, l.cfg.Interval)
	if err != nil {
		log.Warn("heartbeat redial failed; keeping the old connection", "error", err)
		return false
	}
	l.conn.Close()
	l.conn = conn
	return true
}

func beat(ctx context.Context, cfg Config, conn *grpc.ClientConn) error {
	ctx, cancel := context.WithTimeout(ctx, cfg.Interval)
	defer cancel()
	_, err := objectstorev1.NewCoordinatorServiceClient(conn).Heartbeat(ctx,
		&objectstorev1.HeartbeatRequest{NodeId: cfg.NodeID, InstanceId: cfg.InstanceID})
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
