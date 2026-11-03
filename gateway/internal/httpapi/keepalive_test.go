package httpapi

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/Ayomide1x/object-store/internal/halfopen"
)

// A half-open connection to the coordinator (its end, or the path, gone
// without closing anything) times out every call on it. DialCoordinator's
// keepalive closes it within coordinatorKeepaliveTime plus
// coordinatorKeepaliveTimeout, and the next call reconnects. Any gRPC service
// will do as the far end: the connection is what's under test.
func TestCoordinatorConnectionRecoversFromAHalfOpenConnection(t *testing.T) {
	t.Parallel()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	proxy, err := halfopen.Start(lis.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { proxy.Close() })

	const callTimeout = 500 * time.Millisecond
	conn, err := DialCoordinator(proxy.Addr(), callTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	check := func() error {
		_, err := healthpb.NewHealthClient(conn).Check(context.Background(), &healthpb.HealthCheckRequest{})
		return err
	}
	if err := check(); err != nil {
		t.Fatal(err)
	}

	proxy.Freeze()
	frozenAt := time.Now()
	if err := check(); status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("a call on the half-open connection: %v, want DeadlineExceeded", err)
	}
	bound := coordinatorKeepaliveTime + coordinatorKeepaliveTimeout + time.Second + callTimeout
	for check() != nil {
		if time.Since(frozenAt) > bound {
			t.Fatalf("no call succeeded within %s of the connection going half-open (no keepalive?)", bound)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if n := proxy.Accepted(); n != 2 {
		t.Errorf("%d connections, want 2 (one half-open, one new)", n)
	}
}
