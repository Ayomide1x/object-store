package tracing

import (
	"context"
	"net"
	"testing"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// A method passed as untraced makes no server span; others do, joined to
// the caller's trace.
func TestServerOptionsSkipUntracedMethods(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(ServerOptions("svc", "/grpc.health.v1.Health/Check")...)
	healthpb.RegisterHealthServer(srv, health.NewServer())
	go srv.Serve(lis)
	defer srv.Stop()

	conn, err := grpc.NewClient(lis.Addr().String(),
		append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, DialOptions("caller")...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := healthpb.NewHealthClient(conn)

	if _, err := client.Check(context.Background(), &healthpb.HealthCheckRequest{}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.List(context.Background(), &healthpb.HealthListRequest{}); err != nil {
		t.Fatal(err)
	}

	served := map[string]bool{}
	var clientTrace uint64
	for _, s := range mt.FinishedSpans() {
		switch s.Tag(ext.SpanKind) {
		case ext.SpanKindServer:
			served[s.Tag(ext.ResourceName).(string)] = true
			if clientTrace != 0 && s.TraceID() != clientTrace {
				t.Errorf("server span in trace %d, client's is %d", s.TraceID(), clientTrace)
			}
		case ext.SpanKindClient:
			if s.Tag(ext.ResourceName) == "/grpc.health.v1.Health/List" {
				clientTrace = s.TraceID()
			}
		}
	}
	if served["/grpc.health.v1.Health/Check"] {
		t.Error("untraced method Check made a server span")
	}
	if !served["/grpc.health.v1.Health/List"] {
		t.Errorf("traced method List made no server span (served: %v)", served)
	}
}
