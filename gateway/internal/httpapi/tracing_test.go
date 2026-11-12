package httpapi

import (
	"bytes"
	"net/http"
	"strings"
	"testing"

	"github.com/DataDog/dd-trace-go/v2/ddtrace/ext"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/mocktracer"

	"github.com/Ayomide1x/object-store/internal/tracing"
)

// One upload is one trace: the gateway's HTTP span, its calls to the
// coordinator and every replica write, and the servers' spans for those
// calls, all under the same trace ID. The key never names a span.
func TestUploadIsOneTrace(t *testing.T) {
	mt := mocktracer.Start()
	defer mt.Stop()
	c := newCluster(t, options{chunkSize: 1 << 20})

	const key = "private-name.bin"
	resp := putObject(t, c, key, bytes.Repeat([]byte("x"), 2<<20)) // two chunks
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PUT: status %d", resp.StatusCode)
	}
	c.waitHandled(t, 1) // the HTTP span finishes as the handler returns

	spans := mt.FinishedSpans()
	var root *mocktracer.Span
	for _, s := range spans {
		if s.Tag(ext.ServiceName) == tracing.ServiceGateway && s.OperationName() == "http.request" {
			root = s
		}
	}
	if root == nil {
		t.Fatalf("no HTTP span among %d spans", len(spans))
	}
	if got := root.Tag(ext.ResourceName); got != "PUT /objects/{key}" {
		t.Errorf("HTTP span resource = %q, want the route", got)
	}

	// Server-side spans per service and method.
	served := map[string]int{}
	for _, s := range spans {
		if s.TraceID() != root.TraceID() {
			t.Errorf("span %s (%v) is in trace %d, not the upload's %d",
				s.OperationName(), s.Tag(ext.ResourceName), s.TraceID(), root.TraceID())
		}
		if res, _ := s.Tag(ext.ResourceName).(string); strings.Contains(res, key) {
			t.Errorf("span %s has the key in its resource %q", s.OperationName(), res)
		}
		if s.Tag(ext.SpanKind) == ext.SpanKindServer && s != root {
			served[s.Tag(ext.ServiceName).(string)+" "+s.Tag(ext.ResourceName).(string)]++
		}
	}
	want := map[string]int{
		testCoordinatorService + " /objectstore.v1.CoordinatorService/InitiateUpload": 1,
		testCoordinatorService + " /objectstore.v1.CoordinatorService/CompleteUpload": 1,
		testNodeService + " /objectstore.v1.StorageNodeService/WriteChunk":            6, // 2 chunks x 3 replicas
	}
	for k, n := range want {
		if served[k] != n {
			t.Errorf("%s: %d server spans, want %d (all: %v)", k, served[k], n, served)
		}
	}
}
