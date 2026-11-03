package server

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"

	"github.com/Ayomide1x/object-store/internal/halfopen"
)

// The coordinator's channels ping every 10 s, with or without calls
// (NodeClient.KEEPALIVE_TIME). A node's server must permit that: under gRPC's
// default policy (a ping every 5 min at most, none without calls) the third
// ping, at about 30 s, gets "too many pings" and the connection is closed.
// So a client pinging at that rate while idle must keep its one connection.
func TestCoordinatorPingRateIsPermitted(t *testing.T) {
	t.Parallel()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(GRPCOptions()...)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	counter, err := halfopen.Start(lis.Addr().String()) // never frozen: it counts connections
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { counter.Close() })

	conn, err := grpc.NewClient(counter.Addr(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time: 10 * time.Second, Timeout: 2 * time.Second, PermitWithoutStream: true, // the coordinator's
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	check := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{}); err != nil {
			t.Fatalf("health check: %v", err)
		}
	}

	check()
	time.Sleep(36 * time.Second) // idle: pings at about 10, 20 and 30 s
	check()
	if n := counter.Accepted(); n != 1 {
		t.Errorf("%d connections after 36 s of pings at the coordinator's rate, want 1 (closed for too many pings?)", n)
	}
}
