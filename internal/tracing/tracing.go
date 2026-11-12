// Package tracing connects the Go services to Datadog APM. It starts
// Datadog's tracer and returns the hooks that make spans: gRPC interceptors
// for the gateway's and nodes' calls, a wrapper for the gateway's HTTP
// handler, and a log handler that stamps each log line with its trace.
//
// Tracing is off unless DD_TRACE_ENABLED is "true", which only
// docker-compose.prod.yml sets. Off, the hooks still run but make no spans:
// dd-trace-go's global tracer is a no-op until Start starts it.
package tracing

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strings"

	grpctrace "github.com/DataDog/dd-trace-go/contrib/google.golang.org/grpc/v2"
	slogtrace "github.com/DataDog/dd-trace-go/contrib/log/slog/v2"
	httptrace "github.com/DataDog/dd-trace-go/contrib/net/http/v2"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"google.golang.org/grpc"
)

// Service names, as they appear in Datadog. The four nodes share one service
// and are told apart by the node_id tag.
const (
	ServiceGateway = "objectstore-gateway"
	ServiceNode    = "objectstore-node"
)

// Enabled reports whether DD_TRACE_ENABLED asks for tracing.
func Enabled() bool { return os.Getenv("DD_TRACE_ENABLED") == "true" }

// Start starts the tracer for service when Enabled, with tags on every span,
// and returns the function that flushes and stops it. A tracer that fails to
// start is logged and skipped: tracing never stops a service from serving.
//
// The tracer is process-wide: dd-trace-go keeps it in a package variable,
// the one exception to "no package-level mutable state" (DECISIONS.md,
// "Datadog's own tracers, not OpenTelemetry").
func Start(service string, log *slog.Logger, tags map[string]string) (stop func()) {
	if !Enabled() {
		return func() {}
	}
	opts := []tracer.StartOption{tracer.WithService(service), tracer.WithLogger(tracerLog{log})}
	for k, v := range tags {
		opts = append(opts, tracer.WithGlobalTag(k, v))
	}
	if err := tracer.Start(opts...); err != nil {
		log.Warn("tracing not started", "error", err)
		return func() {}
	}
	log.Info("tracing started", "service", service)
	return tracer.Stop
}

// tracerLog sends the tracer's own messages to the service's JSON log on
// stdout. Left to itself it writes them to stderr, which Datadog files as
// errors whatever they say.
type tracerLog struct{ log *slog.Logger }

func (l tracerLog) Log(msg string) {
	level := slog.LevelInfo
	switch {
	case strings.Contains(msg, " ERROR: "):
		level = slog.LevelError
	case strings.Contains(msg, " WARN: "):
		level = slog.LevelWarn
	}
	l.log.Log(context.Background(), level, "datadog tracer", "message", msg)
}

// LogHandler wraps h so that a line logged with a context (InfoContext and
// the rest) carries that context's trace and span IDs, which Datadog uses to
// link the line to its trace.
func LogHandler(h slog.Handler) slog.Handler { return slogtrace.WrapHandler(h) }

// grpcOptions makes one span per call, never one per streamed message: a
// 4 MiB chunk is several 1 MiB frames, and a span per frame would bury the
// call that carries them.
func grpcOptions(service string, untraced []string) []grpctrace.Option {
	return []grpctrace.Option{
		grpctrace.WithService(service),
		grpctrace.WithStreamMessages(false),
		grpctrace.WithUntracedMethods(untraced...),
	}
}

// DialOptions trace every call made on a connection, and send the trace's
// context in the call's metadata so the server's span joins the same trace.
func DialOptions(service string) []grpc.DialOption {
	o := grpcOptions(service, nil)
	return []grpc.DialOption{
		grpc.WithChainUnaryInterceptor(grpctrace.UnaryClientInterceptor(o...)),
		grpc.WithChainStreamInterceptor(grpctrace.StreamClientInterceptor(o...)),
	}
}

// ServerOptions trace every call a server receives, except the full method
// names in untraced (such as "/grpc.health.v1.Health/Check").
func ServerOptions(service string, untraced ...string) []grpc.ServerOption {
	o := grpcOptions(service, untraced)
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(grpctrace.UnaryServerInterceptor(o...)),
		grpc.ChainStreamInterceptor(grpctrace.StreamServerInterceptor(o...)),
	}
}

// HTTPHandler traces each request to h. route names the span's resource from
// the request, so it can be the route ("PUT /objects/{key}"), not the URL: a
// resource per object key would make every object its own row in Datadog.
// GET /healthz, polled by Docker, is not traced.
func HTTPHandler(h http.Handler, service string, route func(*http.Request) string) http.Handler {
	return httptrace.WrapHandler(h, service, "",
		httptrace.WithResourceNamer(route),
		httptrace.WithIgnoreRequest(func(r *http.Request) bool { return r.URL.Path == "/healthz" }))
}
