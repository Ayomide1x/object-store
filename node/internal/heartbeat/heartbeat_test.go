package heartbeat

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
	"github.com/Ayomide1x/object-store/internal/halfopen"
)

const interval = 50 * time.Millisecond

type mode int

const (
	accept mode = iota
	hang
	reject
)

// fakeCoordinator counts heartbeats and how many are in flight at once.
type fakeCoordinator struct {
	objectstorev1.UnimplementedCoordinatorServiceServer
	mode        mode
	beats       atomic.Int32
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	first       atomic.Int64 // unix nanos of the first beat

	mu     sync.Mutex
	landed []landing // every beat that reached the coordinator
}

// landing is one beat as the coordinator saw it: when, and on which connection.
type landing struct {
	at   time.Time
	peer string
}

func (f *fakeCoordinator) landings() []landing {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]landing(nil), f.landed...)
}

func (f *fakeCoordinator) Heartbeat(ctx context.Context, req *objectstorev1.HeartbeatRequest) (*objectstorev1.HeartbeatResponse, error) {
	f.beats.Add(1)
	f.first.CompareAndSwap(0, time.Now().UnixNano())
	if p, ok := peer.FromContext(ctx); ok {
		f.mu.Lock()
		f.landed = append(f.landed, landing{at: time.Now(), peer: p.Addr.String()})
		f.mu.Unlock()
	}
	n := f.inFlight.Add(1)
	defer f.inFlight.Add(-1)
	for {
		m := f.maxInFlight.Load()
		if n <= m || f.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	switch f.mode {
	case hang:
		<-ctx.Done()
		return nil, ctx.Err()
	case reject:
		return nil, status.Error(codes.NotFound, "node9 is not in COORDINATOR_NODES")
	}
	return &objectstorev1.HeartbeatResponse{Liveness: objectstorev1.NodeLiveness_NODE_LIVENESS_ALIVE}, nil
}

// serve runs f on lis until the test ends.
func serve(t *testing.T, lis net.Listener, f *fakeCoordinator) {
	t.Helper()
	srv := grpc.NewServer()
	objectstorev1.RegisterCoordinatorServiceServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
}

// start runs the loop against addr in the background, dialling the way the
// node does (so the reconnect backoff under test is the real one), and
// returns a function that cancels it and fails the test unless Run returns
// within limit.
func start(t *testing.T, addr string, interval time.Duration) func(limit time.Duration) {
	t.Helper()
	l, err := New(Config{NodeID: "node1", InstanceID: "test", Interval: interval, Addr: addr,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		l.Run(ctx)
	}()
	stop := func(limit time.Duration) {
		cancel()
		select {
		case <-done:
		case <-time.After(limit):
			t.Fatalf("Run did not return within %s of cancellation", limit)
		}
	}
	// Bounded, so a loop that ignores cancellation fails the test instead of
	// hanging the run.
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * interval):
		}
	})
	return stop
}

func TestBeatsImmediatelyThenOnTheInterval(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeCoordinator{}
	serve(t, lis, f)
	began := time.Now()
	stop := start(t, lis.Addr().String(), interval)
	time.Sleep(10 * interval)
	stop(interval)

	if first := time.Unix(0, f.first.Load()).Sub(began); first > interval/2 {
		t.Errorf("first beat after %s, want immediately", first)
	}
	// 10 intervals: the immediate beat plus about 10 more, give or take jitter.
	if n := f.beats.Load(); n < 8 || n > 13 {
		t.Errorf("%d beats in 10 intervals, want about 11", n)
	}
}

// The loop keeps going while nothing is listening, and its beats arrive
// promptly once the coordinator is back, even after a long outage: gRPC's
// default reconnect backoff would have grown past several intervals by then.
func TestSurvivesTheCoordinatorBeingDown(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close() // down
	f := &fakeCoordinator{}
	stop := start(t, addr, interval)

	time.Sleep(40 * interval)          // 2 s down
	lis, err = net.Listen("tcp", addr) // back on the same address
	if err != nil {
		t.Skipf("could not reuse %s: %v", addr, err)
	}
	serve(t, lis, f)
	back := time.Now()
	deadline := back.Add(40 * interval)
	for f.beats.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(interval / 10)
	}
	firstAfter := time.Since(back)
	time.Sleep(3 * interval)
	stop(interval)
	if f.beats.Load() < 3 {
		t.Fatalf("%d beats after the coordinator came back, want at least 3", f.beats.Load())
	}
	if firstAfter > 4*interval {
		t.Errorf("first beat %s after the coordinator came back, want within a few intervals (reconnect backoff not capped?)", firstAfter)
	}
}

// Each beat is bounded, so a hung coordinator neither stops the loop nor
// piles up calls.
func TestAHungCoordinatorCostsOneBeatAtATime(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeCoordinator{mode: hang}
	serve(t, lis, f)
	stop := start(t, lis.Addr().String(), interval)
	time.Sleep(10 * interval)
	stop(2 * interval)

	if n := f.beats.Load(); n < 4 {
		t.Errorf("%d beats attempted against a hung coordinator in 10 intervals, want at least 4", n)
	}
	if m := f.maxInFlight.Load(); m != 1 {
		t.Errorf("max beats in flight = %d, want 1", m)
	}
}

func TestRejectedBeatsAreRetried(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeCoordinator{mode: reject}
	serve(t, lis, f)
	stop := start(t, lis.Addr().String(), interval)
	time.Sleep(5 * interval)
	stop(interval)
	if n := f.beats.Load(); n < 3 {
		t.Errorf("%d beats after rejections, want the loop to keep going", n)
	}
}

func TestStopsPromptlyOnCancellation(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	serve(t, lis, &fakeCoordinator{mode: hang}) // even mid-beat
	stop := start(t, lis.Addr().String(), interval)
	time.Sleep(interval / 2)
	stop(interval) // returns within one beat's timeout
}

// halfOpen serves a fake coordinator behind a proxy whose connections can be
// made half-open.
func halfOpen(t *testing.T) (*fakeCoordinator, *halfopen.Proxy) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeCoordinator{}
	serve(t, lis, f)
	p, err := halfopen.Start(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return f, p
}

// A half-open heartbeat connection (the coordinator's end gone, nothing
// closed) fails every beat on it. The loop drops it after one timed-out beat
// and beats at once on a new connection, so beats resume within one beat
// timeout plus one interval of the freeze, and the coordinator's longest
// silence stays under SUSPECT (3 intervals at the demo's 2 s and 6 s), let
// alone DEAD (7.5). Keepalive is on, but needs 11 s: this is the redial.
func TestBeatsResumeOnANewConnectionAfterAHalfOpenConnection(t *testing.T) {
	const iv = 200 * time.Millisecond
	f, proxy := halfOpen(t)
	stop := start(t, proxy.Addr(), iv)
	for deadline := time.Now().Add(20 * iv); len(f.landings()) < 3; {
		if time.Now().After(deadline) {
			t.Fatal("no beats before the freeze")
		}
		time.Sleep(iv / 10)
	}

	proxy.Freeze()
	frozenAt := time.Now()
	frozenPeer := f.landings()[len(f.landings())-1].peer
	var resumed time.Duration
	for deadline := frozenAt.Add(15 * iv); resumed == 0 && time.Now().Before(deadline); time.Sleep(iv / 20) {
		for _, b := range f.landings() {
			if b.peer != frozenPeer {
				resumed = b.at.Sub(frozenAt)
				break
			}
		}
	}
	time.Sleep(3 * iv)
	stop(2 * iv)

	if resumed == 0 {
		t.Fatalf("no beat on a new connection within %s of the freeze (the redial missing?)", 15*iv)
	}
	// One beat timeout (iv) plus one interval and its jitter, plus 50 ms for the dial and scheduling.
	if bound := iv + iv + iv/10 + 50*time.Millisecond; resumed > bound {
		t.Errorf("beats resumed %s after the freeze, want within %s", resumed, bound)
	}
	var maxGap time.Duration
	ls := f.landings()
	for i := 1; i < len(ls); i++ {
		maxGap = max(maxGap, ls[i].at.Sub(ls[i-1].at))
	}
	if suspect := 3 * iv; maxGap >= suspect {
		t.Errorf("longest silence %s, want under %s (SUSPECT, scaled; DEAD is %s)", maxGap, suspect, iv*15/2)
	}
	if n := proxy.Accepted(); n != 2 {
		t.Errorf("%d connections, want 2 (one frozen, one new)", n)
	}
}

// Keepalive is the backstop: with no beat in flight, a connection from Dial
// notices it is half-open within keepaliveTime plus keepaliveTimeout and
// leaves READY, so the next beat reconnects.
func TestDialedConnectionNoticesAHalfOpenConnection(t *testing.T) {
	t.Parallel()
	_, proxy := halfOpen(t)
	conn, err := Dial(proxy.Addr(), interval)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := objectstorev1.NewCoordinatorServiceClient(conn).Heartbeat(ctx, &objectstorev1.HeartbeatRequest{NodeId: "node1"}); err != nil {
		t.Fatal(err)
	}

	proxy.Freeze()
	bound := keepaliveTime + keepaliveTimeout + time.Second
	for deadline := time.Now().Add(bound); conn.GetState() == connectivity.Ready; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("still READY %s after the connection went half-open (no keepalive?)", bound)
		}
	}
}
