package heartbeat

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
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
}

func (f *fakeCoordinator) Heartbeat(ctx context.Context, req *objectstorev1.HeartbeatRequest) (*objectstorev1.HeartbeatResponse, error) {
	f.beats.Add(1)
	f.first.CompareAndSwap(0, time.Now().UnixNano())
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

// client dials the way the node does, so the reconnect backoff under test is the real one.
func client(t *testing.T, addr string) objectstorev1.CoordinatorServiceClient {
	t.Helper()
	conn, err := Dial(addr, interval)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return objectstorev1.NewCoordinatorServiceClient(conn)
}

// start runs the loop in the background and returns a function that cancels
// it and fails the test unless Run returns within limit.
func start(t *testing.T, c objectstorev1.CoordinatorServiceClient) func(limit time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		Run(ctx, Config{NodeID: "node1", InstanceID: "test", Interval: interval, Client: c,
			Log: slog.New(slog.NewTextHandler(io.Discard, nil))})
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
	stop := start(t, client(t, lis.Addr().String()))
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
	stop := start(t, client(t, addr))

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
	stop := start(t, client(t, lis.Addr().String()))
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
	stop := start(t, client(t, lis.Addr().String()))
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
	stop := start(t, client(t, lis.Addr().String()))
	time.Sleep(interval / 2)
	stop(interval) // returns within one beat's timeout
}
