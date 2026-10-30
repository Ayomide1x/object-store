package download

import (
	"bytes"
	"crypto/sha256"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// readTimeout is short so the hanging cases run quickly.
const readTimeout = 300 * time.Millisecond

const frameSize = 1 << 20

type behaviour int

const (
	ok               behaviour = iota
	unavailable                // UNAVAILABLE before any data
	closed                     // nothing listening
	hang                       // never answers until the client gives up
	exhausted                  // RESOURCE_EXHAUSTED
	partialThenBreak           // two frames, then UNAVAILABLE
	notFound                   // NOT_FOUND
	dataLoss                   // DATA_LOSS
	wrongBytes                 // serves other bytes of the right size, cleanly
	shortStream                // serves too few bytes, then ends cleanly
	invalidArgument            // INVALID_ARGUMENT
	internal                   // INTERNAL: a code not listed in the classification
)

// fakeNode serves every requested chunk from data with one fixed behaviour.
type fakeNode struct {
	objectstorev1.UnimplementedStorageNodeServiceServer
	behaviour behaviour
	data      []byte
	reads     atomic.Int32
}

func (f *fakeNode) ReadChunk(_ *objectstorev1.ReadChunkRequest, stream objectstorev1.StorageNodeService_ReadChunkServer) error {
	f.reads.Add(1)
	send := func(b []byte) error {
		for off := 0; off < len(b); off += frameSize {
			if err := stream.Send(&objectstorev1.ReadChunkResponse{Data: b[off:min(off+frameSize, len(b))]}); err != nil {
				return err
			}
		}
		return nil
	}
	switch f.behaviour {
	case unavailable:
		return status.Error(codes.Unavailable, "fake: unavailable")
	case hang:
		<-stream.Context().Done()
		return stream.Context().Err()
	case exhausted:
		return status.Error(codes.ResourceExhausted, "fake: busy")
	case partialThenBreak:
		if err := send(f.data[:2*frameSize]); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "fake: connection lost mid-chunk")
	case notFound:
		return status.Error(codes.NotFound, "fake: no such chunk")
	case dataLoss:
		return status.Error(codes.DataLoss, "fake: chunk failed verification")
	case wrongBytes:
		other := bytes.Clone(f.data)
		other[len(other)/2] ^= 0xff
		return send(other)
	case shortStream:
		return send(f.data[:len(f.data)-1])
	case invalidArgument:
		return status.Error(codes.InvalidArgument, "fake: bad chunk id")
	case internal:
		return status.Error(codes.Internal, "fake: disk read error")
	}
	return send(f.data)
}

// startNodes starts one fake node per behaviour, all serving data, named a, b, c, ...
func startNodes(t *testing.T, data []byte, bs ...behaviour) ([]*fakeNode, []Replica) {
	t.Helper()
	var nodes []*fakeNode
	var replicas []Replica
	for i, b := range bs {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		f := &fakeNode{behaviour: b, data: data}
		if b == closed {
			lis.Close()
		} else {
			srv := grpc.NewServer()
			objectstorev1.RegisterStorageNodeServiceServer(srv, f)
			go srv.Serve(lis)
			t.Cleanup(srv.Stop)
		}
		nodes = append(nodes, f)
		replicas = append(replicas, Replica{NodeID: string(rune('a' + i)), Address: lis.Addr().String()})
	}
	return nodes, replicas
}

// withinOrFail fails the test if fn takes longer than limit, so a missing
// deadline shows up as a failure rather than a hung test run.
func withinOrFail(t *testing.T, limit time.Duration, fn func()) time.Duration {
	t.Helper()
	done := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
		return time.Since(start)
	case <-time.After(limit):
		t.Fatalf("did not return within %s: a hung replica is not bounded by the read deadline", limit)
		return 0
	}
}

func newDownloader(order Order) *Downloader {
	return New(readTimeout, order, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func testData() []byte {
	b := bytes.Repeat([]byte("failover "), 300_000) // 2.7 MB: three frames
	return b[:2*frameSize+500_000]
}

func chunkOf(data []byte, replicas []Replica) Chunk {
	sum := sha256.Sum256(data)
	return Chunk{ID: "c0", Size: int64(len(data)), SHA256: sum[:], Replicas: replicas}
}

func reportsOf(d *Downloader) []string {
	var out []string
	for _, r := range d.Reports() {
		out = append(out, r.NodeID+":"+r.Reason.String())
	}
	return out
}

func TestFailoverClassification(t *testing.T) {
	data := testData()
	tests := []struct {
		name        string
		first       behaviour
		wantReports []string
		slow        bool // the first replica runs into the read deadline
	}{
		{"unavailable", unavailable, nil, false},
		{"connection refused", closed, nil, false},
		{"hung past the deadline", hang, nil, true},
		{"resource exhausted", exhausted, nil, false},
		{"stream breaks mid-chunk", partialThenBreak, nil, false},
		{"internal (unlisted code)", internal, nil, false},
		{"not found", notFound, []string{"a:REASON_NOT_FOUND"}, false},
		{"data loss", dataLoss, []string{"a:REASON_DATA_LOSS"}, false},
		{"wrong bytes", wrongBytes, []string{"a:REASON_CHECKSUM_MISMATCH"}, false},
		{"short stream", shortStream, []string{"a:REASON_CHECKSUM_MISMATCH"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes, replicas := startNodes(t, data, tt.first, ok)
			d := newDownloader(Listed)
			path := filepath.Join(t.TempDir(), "out")

			var err error
			elapsed := withinOrFail(t, 5*time.Second, func() {
				err = d.ToFile(t.Context(), []Chunk{chunkOf(data, replicas)}, int64(len(data)), path)
			})
			if err != nil {
				t.Fatalf("ToFile: %v", err)
			}
			got, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("output differs from the source (%d bytes, want %d): partial bytes leaked from the failed replica",
					len(got), len(data))
			}
			// A closed port refuses the connection, so the call never reaches the fake.
			wantFirst := int32(1)
			if tt.first == closed {
				wantFirst = 0
			}
			if nodes[0].reads.Load() != wantFirst || nodes[1].reads.Load() != 1 {
				t.Errorf("reads = %d, %d; want %d, 1 (one failed attempt, then failover)",
					nodes[0].reads.Load(), nodes[1].reads.Load(), wantFirst)
			}
			if r := reportsOf(d); !slices.Equal(r, tt.wantReports) {
				t.Errorf("reports = %v, want %v", r, tt.wantReports)
			}
			if tt.slow && (elapsed < readTimeout || elapsed > readTimeout+time.Second) {
				t.Errorf("took %s, want about the %s deadline", elapsed, readTimeout)
			}
		})
	}
}

// INVALID_ARGUMENT means the request is malformed: the get fails at once and
// the next replica is never asked.
func TestInvalidArgumentDoesNotFailOver(t *testing.T) {
	data := testData()
	nodes, replicas := startNodes(t, data, invalidArgument, ok)
	d := newDownloader(Listed)
	path := filepath.Join(t.TempDir(), "out")

	err := d.ToFile(t.Context(), []Chunk{chunkOf(data, replicas)}, int64(len(data)), path)
	if status.Code(errorsUnwrapToStatus(err)) != codes.InvalidArgument {
		t.Fatalf("err = %v, want InvalidArgument", err)
	}
	if got := nodes[1].reads.Load(); got != 0 {
		t.Errorf("second replica was asked %d times, want 0", got)
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Error("an output file was left behind")
	}
	if r := reportsOf(d); len(r) != 0 {
		t.Errorf("reports = %v, want none", r)
	}
}

func TestAllReplicasFail(t *testing.T) {
	data := testData()
	_, replicas := startNodes(t, data, dataLoss, unavailable, wrongBytes)
	d := newDownloader(Listed)
	path := filepath.Join(t.TempDir(), "out")

	err := d.ToFile(t.Context(), []Chunk{chunkOf(data, replicas)}, int64(len(data)), path)
	if err == nil {
		t.Fatal("ToFile succeeded with every replica failing")
	}
	for _, want := range []string{"all 3 replicas failed", "a: ", "b: ", "c: ", "DataLoss", "Unavailable", "sha256"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if _, statErr := os.Stat(path); statErr == nil {
		t.Error("an output file was left behind")
	}
	if entries, _ := os.ReadDir(filepath.Dir(path)); len(entries) != 0 {
		t.Errorf("temp files left behind: %d entries", len(entries))
	}
	want := []string{"a:REASON_DATA_LOSS", "c:REASON_CHECKSUM_MISMATCH"}
	if r := reportsOf(d); !slices.Equal(r, want) {
		t.Errorf("reports = %v, want %v", r, want)
	}
}

// A hung node is demoted after its first miss, so it costs one deadline per
// get, not one per chunk.
func TestHungReplicaDemotedForRestOfGet(t *testing.T) {
	data := []byte("small chunk")
	nodes, replicas := startNodes(t, data, hang, ok)
	d := newDownloader(Listed)
	var chunks []Chunk
	for i := range 5 {
		c := chunkOf(data, replicas)
		c.ID, c.Index = "c"+string(rune('0'+i)), int32(i)
		chunks = append(chunks, c)
	}
	whole := bytes.Repeat(data, 5)
	path := filepath.Join(t.TempDir(), "out")

	elapsed := withinOrFail(t, 10*time.Second, func() {
		if err := d.ToFile(t.Context(), chunks, int64(len(whole)), path); err != nil {
			t.Errorf("ToFile: %v", err)
		}
	})
	if got := nodes[0].reads.Load(); got != 1 {
		t.Errorf("hung node was asked %d times, want 1 (tried last after its first miss)", got)
	}
	if limit := 2 * readTimeout; elapsed > limit {
		t.Errorf("5 chunks took %s, want under %s", elapsed, limit)
	}
	if got, _ := os.ReadFile(path); !bytes.Equal(got, whole) {
		t.Error("output differs from the source")
	}
}

// A demoted node is still tried when every other replica fails.
func TestDemotedReplicaStillTriedLast(t *testing.T) {
	data := []byte("last resort")
	// b was demoted earlier in this get; a now fails, so b must still be tried.
	_, replicas := startNodes(t, data, unavailable, ok)
	d := newDownloader(Listed)
	d.demoted["b"] = true
	c := chunkOf(data, replicas)
	got, err := d.ReadChunk(t.Context(), c)
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadChunk = %q, %v; want the data from the demoted replica", got, err)
	}
}

func TestOrderIsInjected(t *testing.T) {
	data := []byte("ordered")
	nodes, replicas := startNodes(t, data, ok, ok)
	reversed := func(r []Replica) []Replica { slices.Reverse(r); return r }
	d := newDownloader(reversed)
	if _, err := d.ReadChunk(t.Context(), chunkOf(data, replicas)); err != nil {
		t.Fatal(err)
	}
	if nodes[0].reads.Load() != 0 || nodes[1].reads.Load() != 1 {
		t.Errorf("reads = %d, %d; want the injected order (second replica first)", nodes[0].reads.Load(), nodes[1].reads.Load())
	}
}

// errorsUnwrapToStatus finds the gRPC status error inside a wrapped error.
func errorsUnwrapToStatus(err error) error {
	for e := err; e != nil; {
		if _, ok := status.FromError(e); ok {
			return e
		}
		u, ok := e.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		e = u.Unwrap()
	}
	return err
}

// A replica the coordinator lists as DEAD is read only after every live one,
// even when it comes first in the coordinator's order; it is never dropped.
func TestDeadReplicasAreReadLast(t *testing.T) {
	data := []byte("liveness ordered")
	nodes, replicas := startNodes(t, data, ok, ok, ok)
	replicas[0].Liveness = objectstorev1.NodeLiveness_NODE_LIVENESS_DEAD
	replicas[1].Liveness = objectstorev1.NodeLiveness_NODE_LIVENESS_SUSPECT
	replicas[2].Liveness = objectstorev1.NodeLiveness_NODE_LIVENESS_ALIVE
	d := newDownloader(Listed)
	if _, err := d.ReadChunk(t.Context(), chunkOf(data, replicas)); err != nil {
		t.Fatal(err)
	}
	if nodes[0].reads.Load() != 0 || nodes[1].reads.Load() != 0 || nodes[2].reads.Load() != 1 {
		t.Errorf("reads = %d, %d, %d; want only the ALIVE replica read", nodes[0].reads.Load(), nodes[1].reads.Load(), nodes[2].reads.Load())
	}

	// With every live replica failing, the DEAD one is still tried.
	nodes2, replicas2 := startNodes(t, data, ok, unavailable, unavailable)
	replicas2[0].Liveness = objectstorev1.NodeLiveness_NODE_LIVENESS_DEAD
	d2 := newDownloader(Listed)
	got, err := d2.ReadChunk(t.Context(), chunkOf(data, replicas2))
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("ReadChunk = %q, %v; want the data from the DEAD-listed replica", got, err)
	}
	if nodes2[0].reads.Load() != 1 {
		t.Errorf("DEAD-listed replica read %d times, want 1 (last resort)", nodes2[0].reads.Load())
	}
}
