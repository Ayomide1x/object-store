package upload

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// writeTimeout is short so the hanging cases run quickly.
const writeTimeout = 300 * time.Millisecond

type behaviour int

const (
	ok        behaviour = iota // verifies the trailer and acknowledges with its hash
	fail                       // returns UNAVAILABLE at once
	hang                       // never answers until the client gives up
	wrongHash                  // acknowledges with a different hash
	closed                     // nothing listening: connection refused
)

// fakeNode is an in-process StorageNodeService with one fixed behaviour.
type fakeNode struct {
	objectstorev1.UnimplementedStorageNodeServiceServer
	behaviour behaviour
	writes    atomic.Int32 // WriteChunk calls received
}

func (f *fakeNode) WriteChunk(stream objectstorev1.StorageNodeService_WriteChunkServer) error {
	f.writes.Add(1)
	switch f.behaviour {
	case fail:
		return status.Error(codes.Unavailable, "fake node failing")
	case hang:
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	h := sha256.New()
	var n int64
	var trailer []byte
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch fr := req.GetFrame().(type) {
		case *objectstorev1.WriteChunkRequest_Data:
			h.Write(fr.Data)
			n += int64(len(fr.Data))
		case *objectstorev1.WriteChunkRequest_Trailer:
			trailer = fr.Trailer.GetSha256()
		}
	}
	sum := h.Sum(nil)
	if !bytes.Equal(sum, trailer) {
		return status.Error(codes.InvalidArgument, "checksum mismatch")
	}
	if f.behaviour == wrongHash {
		sum = make([]byte, 32)
	}
	return stream.SendAndClose(&objectstorev1.WriteChunkResponse{BytesWritten: n, Sha256: sum})
}

// startNodes starts one fake node per behaviour and returns them with their targets.
func startNodes(t *testing.T, bs ...behaviour) ([]*fakeNode, []Target) {
	t.Helper()
	var nodes []*fakeNode
	var targets []Target
	for i, b := range bs {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := lis.Addr().String()
		f := &fakeNode{behaviour: b}
		if b == closed {
			lis.Close() // the port now refuses connections
		} else {
			srv := grpc.NewServer()
			objectstorev1.RegisterStorageNodeServiceServer(srv, f)
			go srv.Serve(lis)
			t.Cleanup(srv.Stop)
		}
		nodes = append(nodes, f)
		targets = append(targets, Target{NodeID: string(rune('a' + i)), Address: addr})
	}
	return nodes, targets
}

// withinOrFail runs fn and fails the test if it takes longer than limit, so a
// missing deadline shows up as a failure rather than a hung test run.
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
		t.Fatalf("did not return within %s: a hung replica is not bounded by the write deadline", limit)
		return 0
	}
}

func newUploader() *Uploader {
	return New("obj", writeTimeout, slog.New(slog.NewTextHandler(io.Discard, nil)))
}

func TestWriteChunkQuorum(t *testing.T) {
	data := bytes.Repeat([]byte("replicate me "), 200_000) // 2.6 MB: several frames
	want := sha256.Sum256(data)

	tests := []struct {
		name       string
		behaviours []behaviour
		wantAcked  int  // 0 means the chunk must fail
		slow       bool // a replica is expected to run into the deadline
	}{
		{"all three acknowledge", []behaviour{ok, ok, ok}, 3, false},
		{"one failing", []behaviour{ok, fail, ok}, 2, false},
		{"one refusing connections", []behaviour{closed, ok, ok}, 2, false},
		{"one hanging past the deadline", []behaviour{ok, ok, hang}, 2, true},
		{"one acknowledging the wrong hash", []behaviour{ok, wrongHash, ok}, 2, false},
		{"two failing", []behaviour{fail, ok, fail}, 0, false},
		{"one failing, one hanging", []behaviour{ok, hang, fail}, 0, true},
		{"all three failing", []behaviour{fail, closed, fail}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, targets := startNodes(t, tt.behaviours...)
			u := newUploader()
			chunk := Chunk{ID: "c0", Size: int64(len(data)), Targets: targets}

			var sum []byte
			var acked []string
			var err error
			elapsed := withinOrFail(t, 5*time.Second, func() {
				sum, acked, err = u.WriteChunk(t.Context(), chunk, bytes.NewReader(data))
			})

			if tt.wantAcked == 0 {
				if err == nil {
					t.Fatalf("WriteChunk succeeded with acks %v, want a quorum failure", acked)
				}
				if !strings.Contains(err.Error(), "need 2") {
					t.Errorf("error %q does not explain the quorum", err)
				}
			} else {
				if err != nil {
					t.Fatalf("WriteChunk: %v", err)
				}
				if len(acked) != tt.wantAcked {
					t.Errorf("acked = %v, want %d nodes", acked, tt.wantAcked)
				}
				if !bytes.Equal(sum, want[:]) {
					t.Errorf("sum = %x, want %x", sum, want)
				}
			}
			if tt.slow && (elapsed < writeTimeout || elapsed > writeTimeout+time.Second) {
				t.Errorf("took %s, want about the %s deadline", elapsed, writeTimeout)
			}
		})
	}
}

// A node that fails once is not tried again for the rest of the upload, so a
// hung node costs one deadline per upload, not one per chunk.
func TestFailedNodeSkippedForRestOfUpload(t *testing.T) {
	nodes, targets := startNodes(t, ok, ok, hang)
	u := newUploader()
	data := []byte("small chunk")
	const chunks = 5

	elapsed := withinOrFail(t, 10*time.Second, func() {
		for i := range chunks {
			c := Chunk{ID: "c" + string(rune('0'+i)), Index: int32(i), Size: int64(len(data)), Targets: targets}
			if _, acked, err := u.WriteChunk(t.Context(), c, bytes.NewReader(data)); err != nil || len(acked) != 2 {
				t.Errorf("chunk %d: acked %v, err %v; want 2 acks", i, acked, err)
			}
		}
	})

	if got := nodes[2].writes.Load(); got != 1 {
		t.Errorf("hung node received %d writes, want 1 (skipped after its first miss)", got)
	}
	if limit := 2 * writeTimeout; elapsed > limit {
		t.Errorf("%d chunks took %s, want under %s (one deadline, not one per chunk)", chunks, elapsed, limit)
	}
}
