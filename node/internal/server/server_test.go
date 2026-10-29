package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
	"github.com/Ayomide1x/object-store/node/internal/chunkstore"
)

const (
	testChunkID  = "0b6f0d8e-7a64-4a8e-9b3a-2a1c7f0e5d11"
	otherChunkID = "7d1e2f3a-4b5c-4d6e-8f70-112233445566"
)

// testNode is a Server on an in-memory listener.
type testNode struct {
	client objectstorev1.StorageNodeServiceClient
	srv    *Server
	dir    string
}

func startServer(t *testing.T, maxReads int) testNode {
	t.Helper()
	dir := t.TempDir()
	store, err := chunkstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New("test-node", store, log, maxReads)

	lis := bufconn.Listen(16 << 20)
	gs := grpc.NewServer()
	objectstorev1.RegisterStorageNodeServiceServer(gs, srv)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return testNode{client: objectstorev1.NewStorageNodeServiceClient(conn), srv: srv, dir: dir}
}

func (n testNode) finalPath(id string) string { return filepath.Join(n.dir, "chunks", id) }

func (n testNode) tempCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(n.dir, "tmp"))
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

func header(size int64) *objectstorev1.WriteChunkRequest {
	return &objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Header{
		Header: &objectstorev1.WriteChunkHeader{ChunkId: testChunkID, SizeBytes: size},
	}}
}

func data(b []byte) *objectstorev1.WriteChunkRequest {
	return &objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Data{Data: b}}
}

func trailer(sum []byte) *objectstorev1.WriteChunkRequest {
	return &objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Trailer{
		Trailer: &objectstorev1.WriteChunkTrailer{Sha256: sum},
	}}
}

func sha(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}

// write sends frames and returns the response and the RPC's final error.
func write(ctx context.Context, c objectstorev1.StorageNodeServiceClient, frames []*objectstorev1.WriteChunkRequest) (*objectstorev1.WriteChunkResponse, error) {
	stream, err := c.WriteChunk(ctx)
	if err != nil {
		return nil, err
	}
	for _, f := range frames {
		// A Send error means the server already failed the RPC; the real
		// status comes from CloseAndRecv.
		if err := stream.Send(f); err != nil {
			break
		}
	}
	return stream.CloseAndRecv()
}

// writeValid stores b as chunk id through the RPC.
func writeValid(t *testing.T, n testNode, id string, b []byte) {
	t.Helper()
	frames := []*objectstorev1.WriteChunkRequest{
		{Frame: &objectstorev1.WriteChunkRequest_Header{Header: &objectstorev1.WriteChunkHeader{ChunkId: id, SizeBytes: int64(len(b))}}},
	}
	for off := 0; off < len(b); off += MaxFrameSize {
		frames = append(frames, data(b[off:min(off+MaxFrameSize, len(b))]))
	}
	frames = append(frames, trailer(sha(b)))
	if _, err := write(t.Context(), n.client, frames); err != nil {
		t.Fatalf("write %s: %v", id, err)
	}
}

// read returns the frames received before the stream ended, and its error.
func read(ctx context.Context, c objectstorev1.StorageNodeServiceClient, id string) ([][]byte, error) {
	stream, err := c.ReadChunk(ctx, &objectstorev1.ReadChunkRequest{ChunkId: id})
	if err != nil {
		return nil, err
	}
	var frames [][]byte
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return frames, nil
		}
		if err != nil {
			return frames, err
		}
		frames = append(frames, resp.GetData())
	}
}

func TestWriteChunkFraming(t *testing.T) {
	oneMiB := bytes.Repeat([]byte{'a'}, MaxFrameSize)
	abc := []byte("abc")

	type frames = []*objectstorev1.WriteChunkRequest
	tests := []struct {
		name     string
		frames   frames
		wantCode codes.Code
	}{
		{"zero bytes", frames{header(0), trailer(sha(nil))}, codes.OK},
		{"one frame", frames{header(3), data(abc), trailer(sha(abc))}, codes.OK},
		{"full chunk in 1 MiB frames", frames{header(MaxChunkSize), data(oneMiB), data(oneMiB), data(oneMiB), data(oneMiB),
			trailer(sha(bytes.Repeat([]byte{'a'}, MaxChunkSize)))}, codes.OK},
		{"no frames", nil, codes.InvalidArgument},
		{"data before header", frames{data(abc), header(3), trailer(sha(abc))}, codes.InvalidArgument},
		{"header repeated", frames{header(3), header(3), data(abc), trailer(sha(abc))}, codes.InvalidArgument},
		{"empty frame", frames{header(3), {}, trailer(sha(abc))}, codes.InvalidArgument},
		{"too few bytes", frames{header(4), data(abc), trailer(sha(abc))}, codes.InvalidArgument},
		{"too many bytes", frames{header(2), data(abc), trailer(sha(abc))}, codes.InvalidArgument},
		{"size over 4 MiB", frames{header(MaxChunkSize + 1)}, codes.InvalidArgument},
		{"frame over 1 MiB", frames{header(MaxFrameSize + 1), data(append(oneMiB, 'a'))}, codes.InvalidArgument},
		{"missing trailer", frames{header(3), data(abc)}, codes.InvalidArgument},
		{"trailer repeated", frames{header(3), data(abc), trailer(sha(abc)), trailer(sha(abc))}, codes.InvalidArgument},
		{"data after trailer", frames{header(3), data(abc), trailer(sha(abc)), data(abc)}, codes.InvalidArgument},
		{"digest 31 bytes", frames{header(3), data(abc), trailer(sha(abc)[:31])}, codes.InvalidArgument},
		{"digest missing", frames{header(3), data(abc), trailer(nil)}, codes.InvalidArgument},
		{"checksum mismatch", frames{header(3), data(abc), trailer(sha([]byte("abd")))}, codes.InvalidArgument},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := startServer(t, 4)
			_, err := write(t.Context(), n.client, tt.frames)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			_, statErr := os.Stat(n.finalPath(testChunkID))
			if exists, want := statErr == nil, tt.wantCode == codes.OK; exists != want {
				t.Errorf("final chunk file exists = %v, want %v", exists, want)
			}
			if c := n.tempCount(t); c != 0 {
				t.Errorf("tmp has %d entries after the write, want 0", c)
			}
		})
	}
}

func TestWriteChunkStoresHeaderAndReturnsHash(t *testing.T) {
	n := startServer(t, 4)
	b := []byte("hello chunk")
	resp, err := write(t.Context(), n.client, []*objectstorev1.WriteChunkRequest{header(int64(len(b))), data(b), trailer(sha(b))})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(resp.GetSha256(), sha(b)) || resp.GetBytesWritten() != int64(len(b)) {
		t.Errorf("response = %x/%d, want %x/%d", resp.GetSha256(), resp.GetBytesWritten(), sha(b), len(b))
	}
	file, err := os.ReadFile(n.finalPath(testChunkID))
	if err != nil {
		t.Fatal(err)
	}
	idBytes := []byte{0x0b, 0x6f, 0x0d, 0x8e, 0x7a, 0x64, 0x4a, 0x8e, 0x9b, 0x3a, 0x2a, 0x1c, 0x7f, 0x0e, 0x5d, 0x11}
	want := append(append(append([]byte("OSCHUNK1"), idBytes...), sha(b)...), b...)
	if !bytes.Equal(file, want) {
		t.Errorf("chunk file =\n  %x\nwant\n  %x", file, want)
	}
}

// The fast path rejects a write whose chunk already exists before any data.
func TestWriteChunkExistingChunkRejectedUpFront(t *testing.T) {
	n := startServer(t, 4)
	writeValid(t, n, testChunkID, []byte("old"))
	_, err := write(t.Context(), n.client, []*objectstorev1.WriteChunkRequest{header(3), data([]byte("new")), trailer(sha([]byte("new")))})
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Fatalf("code = %v, want AlreadyExists", got)
	}
	got, err := read(t.Context(), n.client, testChunkID)
	if err != nil || len(got) != 1 || string(got[0]) != "old" {
		t.Errorf("read after rejected overwrite = %q, %v; want [old]", got, err)
	}
}

// TestWriteChunkNeverReplacesAtFinalStep plants a file under the final name
// after the server has passed its up-front existence check, so only the
// final link step stands between the write and the planted file.
func TestWriteChunkNeverReplacesAtFinalStep(t *testing.T) {
	n := startServer(t, 4)
	ctx := t.Context()
	stream, err := n.client.WriteChunk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b := []byte("new data")
	for _, f := range []*objectstorev1.WriteChunkRequest{header(int64(len(b))), data(b)} {
		if err := stream.Send(f); err != nil {
			t.Fatal(err)
		}
	}
	// The temp file appears only after the existence check passed.
	deadline := time.Now().Add(5 * time.Second)
	for n.tempCount(t) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("server never created a temp file")
		}
		time.Sleep(time.Millisecond)
	}
	planted := []byte("planted: must survive")
	if err := os.WriteFile(n.finalPath(testChunkID), planted, 0o644); err != nil {
		t.Fatal(err)
	}

	if err := stream.Send(trailer(sha(b))); err != nil {
		t.Fatal(err)
	}
	_, err = stream.CloseAndRecv()
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Fatalf("code = %v, want AlreadyExists (err: %v)", got, err)
	}
	got, err := os.ReadFile(n.finalPath(testChunkID))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, planted) {
		t.Errorf("planted file was replaced: now %q", got)
	}
	if c := n.tempCount(t); c != 0 {
		t.Errorf("tmp has %d entries, want 0", c)
	}
}

func TestReadChunkFrames(t *testing.T) {
	tests := []struct {
		name       string
		size       int
		wantFrames int
	}{
		{"empty", 0, 0},
		{"small", 10, 1},
		{"exactly one frame", MaxFrameSize, 1},
		{"full chunk", MaxChunkSize, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := startServer(t, 4)
			want := bytes.Repeat([]byte{'z'}, tt.size)
			writeValid(t, n, testChunkID, want)

			frames, err := read(t.Context(), n.client, testChunkID)
			if err != nil {
				t.Fatal(err)
			}
			for _, f := range frames {
				if len(f) > MaxFrameSize {
					t.Fatalf("frame of %d bytes exceeds %d", len(f), MaxFrameSize)
				}
			}
			if got := bytes.Join(frames, nil); !bytes.Equal(got, want) {
				t.Errorf("read %d bytes, want %d identical bytes", len(got), len(want))
			}
			if len(frames) != tt.wantFrames {
				t.Errorf("frames = %d, want %d", len(frames), tt.wantFrames)
			}
		})
	}
}

// TestReadChunkCorruptSendsNothing damages a stored chunk in each way the
// header guards against. Every case must fail with DATA_LOSS before a single
// frame is sent (invariant 5).
func TestReadChunkCorruptSendsNothing(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789"), 300_000) // 3 MB: several frames
	tests := []struct {
		name   string
		damage func(t *testing.T, n testNode) // leaves a damaged file at testChunkID
	}{
		{"data byte flipped", func(t *testing.T, n testNode) {
			flip(t, n.finalPath(testChunkID), chunkstore.HeaderSize+2_500_000)
		}},
		{"stored hash byte flipped", func(t *testing.T, n testNode) { flip(t, n.finalPath(testChunkID), 30) }},
		{"magic damaged", func(t *testing.T, n testNode) { flip(t, n.finalPath(testChunkID), 0) }},
		{"truncated below header", func(t *testing.T, n testNode) {
			if err := os.Truncate(n.finalPath(testChunkID), chunkstore.HeaderSize-1); err != nil {
				t.Fatal(err)
			}
		}},
		{"truncated data", func(t *testing.T, n testNode) {
			if err := os.Truncate(n.finalPath(testChunkID), chunkstore.HeaderSize+100); err != nil {
				t.Fatal(err)
			}
		}},
		{"valid chunk file under another chunk's name", func(t *testing.T, n testNode) {
			// A misplaced file: intact, but it is not testChunkID's chunk.
			writeValid(t, n, otherChunkID, payload)
			if err := os.Remove(n.finalPath(testChunkID)); err != nil {
				t.Fatal(err)
			}
			b, err := os.ReadFile(n.finalPath(otherChunkID))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(n.finalPath(testChunkID), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := startServer(t, 4)
			writeValid(t, n, testChunkID, payload)
			tt.damage(t, n)

			frames, err := read(t.Context(), n.client, testChunkID)
			if got := status.Code(err); got != codes.DataLoss {
				t.Fatalf("code = %v, want DataLoss (err: %v)", got, err)
			}
			if len(frames) != 0 {
				t.Errorf("received %d frames before the error, want 0", len(frames))
			}
		})
	}
}

func TestReadChunkMissing(t *testing.T) {
	n := startServer(t, 4)
	_, err := read(t.Context(), n.client, testChunkID)
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", got)
	}
}

func TestReadChunkConcurrencyLimit(t *testing.T) {
	n := startServer(t, 2)
	writeValid(t, n, testChunkID, []byte("x"))

	// Occupy every slot, as two in-flight reads would.
	n.srv.readSlots <- struct{}{}
	n.srv.readSlots <- struct{}{}
	_, err := read(t.Context(), n.client, testChunkID)
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("code with all slots taken = %v, want ResourceExhausted", got)
	}

	<-n.srv.readSlots
	if _, err := read(t.Context(), n.client, testChunkID); err != nil {
		t.Fatalf("read after a slot freed: %v", err)
	}
	if got := len(n.srv.readSlots); got != 1 {
		t.Errorf("slots in use after read = %d, want 1 (the read must release its slot)", got)
	}
}

// flip inverts one byte of a file in place.
func flip(t *testing.T, path string, offset int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, offset); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, offset); err != nil {
		t.Fatal(err)
	}
}

// TestStatChunks covers each entry state in one batch, including a misplaced
// file: an intact chunk under another chunk's name must not count as a copy.
func TestStatChunks(t *testing.T) {
	const (
		missingID   = "11111111-2222-4333-8444-555555555555"
		corruptID   = "22222222-3333-4444-8555-666666666666"
		misplacedID = "33333333-4444-4555-8666-777777777777"
	)
	n := startServer(t, 4)
	good := []byte("stat me")
	writeValid(t, n, testChunkID, good)
	writeValid(t, n, corruptID, []byte("about to lose my magic"))
	flip(t, n.finalPath(corruptID), 0)
	writeValid(t, n, otherChunkID, []byte("I belong to otherChunkID"))
	b, err := os.ReadFile(n.finalPath(otherChunkID))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(n.finalPath(misplacedID), b, 0o644); err != nil {
		t.Fatal(err)
	}

	ids := []string{testChunkID, missingID, corruptID, misplacedID}
	resp, err := n.client.StatChunks(t.Context(), &objectstorev1.StatChunksRequest{ChunkIds: ids})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetNodeId() != "test-node" {
		t.Errorf("response node_id = %q, want the node's own ID %q", resp.GetNodeId(), "test-node")
	}
	want := []struct {
		id    string
		state objectstorev1.ChunkStat_State
	}{
		{testChunkID, objectstorev1.ChunkStat_STATE_FOUND},
		{missingID, objectstorev1.ChunkStat_STATE_MISSING},
		{corruptID, objectstorev1.ChunkStat_STATE_BAD_HEADER},
		{misplacedID, objectstorev1.ChunkStat_STATE_BAD_HEADER},
	}
	if len(resp.GetStats()) != len(want) {
		t.Fatalf("got %d entries, want %d", len(resp.GetStats()), len(want))
	}
	for i, w := range want {
		got := resp.GetStats()[i]
		if got.GetChunkId() != w.id || got.GetState() != w.state {
			t.Errorf("entry %d = %s %s, want %s %s", i, got.GetChunkId(), got.GetState(), w.id, w.state)
		}
	}
	found := resp.GetStats()[0]
	if found.GetSizeBytes() != int64(len(good)) || !bytes.Equal(found.GetSha256(), sha(good)) {
		t.Errorf("found entry = %d bytes %x, want %d bytes %x", found.GetSizeBytes(), found.GetSha256(), len(good), sha(good))
	}

	_, err = n.client.StatChunks(t.Context(), &objectstorev1.StatChunksRequest{ChunkIds: []string{"../etc/passwd"}})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("invalid chunk id: code = %v, want InvalidArgument", got)
	}
}

func TestVerifyChunk(t *testing.T) {
	good := []byte("verify me please")
	tests := []struct {
		name      string
		damage    func(t *testing.T, n testNode) // applied after testChunkID holds good
		expected  []byte
		wantState objectstorev1.VerifyChunkResponse_State
	}{
		{"healthy", func(*testing.T, testNode) {}, sha(good), objectstorev1.VerifyChunkResponse_STATE_HEALTHY},
		{"data byte flipped", func(t *testing.T, n testNode) {
			flip(t, n.finalPath(testChunkID), chunkstore.HeaderSize+3)
		}, sha(good), objectstorev1.VerifyChunkResponse_STATE_CORRUPT},
		{"bad header", func(t *testing.T, n testNode) { flip(t, n.finalPath(testChunkID), 0) },
			sha(good), objectstorev1.VerifyChunkResponse_STATE_CORRUPT},
		{"misplaced file", func(t *testing.T, n testNode) {
			writeValid(t, n, otherChunkID, good)
			b, err := os.ReadFile(n.finalPath(otherChunkID))
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(n.finalPath(testChunkID), b, 0o644); err != nil {
				t.Fatal(err)
			}
		}, sha(good), objectstorev1.VerifyChunkResponse_STATE_CORRUPT},
		{"missing", func(t *testing.T, n testNode) {
			if err := os.Remove(n.finalPath(testChunkID)); err != nil {
				t.Fatal(err)
			}
		}, sha(good), objectstorev1.VerifyChunkResponse_STATE_MISSING},
		// Internally consistent, but not the chunk the coordinator recorded.
		{"stored hash differs from expected", func(*testing.T, testNode) {}, sha([]byte("something else")),
			objectstorev1.VerifyChunkResponse_STATE_CORRUPT},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := startServer(t, 4)
			writeValid(t, n, testChunkID, good)
			tt.damage(t, n)

			resp, err := n.client.VerifyChunk(t.Context(), &objectstorev1.VerifyChunkRequest{ChunkId: testChunkID, ExpectedSha256: tt.expected})
			if err != nil {
				t.Fatal(err)
			}
			if resp.GetState() != tt.wantState {
				t.Errorf("state = %s (%s), want %s", resp.GetState(), resp.GetDetail(), tt.wantState)
			}
			if resp.GetNodeId() != "test-node" || resp.GetChunkId() != testChunkID {
				t.Errorf("response identifies %s/%s, want test-node/%s", resp.GetNodeId(), resp.GetChunkId(), testChunkID)
			}
		})
	}
}

func TestVerifyChunkCountsAgainstReadLimit(t *testing.T) {
	n := startServer(t, 1)
	writeValid(t, n, testChunkID, []byte("x"))
	n.srv.readSlots <- struct{}{}
	_, err := n.client.VerifyChunk(t.Context(), &objectstorev1.VerifyChunkRequest{ChunkId: testChunkID, ExpectedSha256: sha([]byte("x"))})
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("code with the only slot taken = %v, want ResourceExhausted", got)
	}
}

// startTCPNode runs a real node server on a TCP port, for PushChunk targets:
// the push dials the target by address.
func startTCPNode(t *testing.T, nodeID string) (testNode, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := chunkstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(nodeID, store, slog.New(slog.NewTextHandler(io.Discard, nil)), 4)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	objectstorev1.RegisterStorageNodeServiceServer(gs, srv)
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return testNode{client: objectstorev1.NewStorageNodeServiceClient(conn), srv: srv, dir: dir}, lis.Addr().String()
}

func TestPushChunk(t *testing.T) {
	payload := bytes.Repeat([]byte("push me "), 400_000) // 3.2 MB: several frames
	tests := []struct {
		name        string
		setup       func(t *testing.T, source, target testNode) // after source holds payload
		expected    []byte
		wantCode    codes.Code
		wantOutcome objectstorev1.PushChunkResponse_Outcome
	}{
		{"copies a verified chunk", func(*testing.T, testNode, testNode) {}, sha(payload),
			codes.OK, objectstorev1.PushChunkResponse_OUTCOME_COPIED},
		{"source corrupt", func(t *testing.T, s, _ testNode) { flip(t, s.finalPath(testChunkID), chunkstore.HeaderSize+10) },
			sha(payload), codes.DataLoss, 0},
		{"source is not the recorded chunk", func(*testing.T, testNode, testNode) {}, sha([]byte("other")),
			codes.DataLoss, 0},
		{"source missing", func(t *testing.T, s, _ testNode) {
			if err := os.Remove(s.finalPath(testChunkID)); err != nil {
				t.Fatal(err)
			}
		}, sha(payload), codes.NotFound, 0},
		{"target already has a file", func(t *testing.T, _, tg testNode) { writeValid(t, tg, testChunkID, []byte("old")) },
			sha(payload), codes.OK, objectstorev1.PushChunkResponse_OUTCOME_TARGET_HAS_CHUNK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			source := startServer(t, 4)
			writeValid(t, source, testChunkID, payload)
			target, targetAddr := startTCPNode(t, "target-node")
			tt.setup(t, source, target)
			before, _ := os.ReadFile(target.finalPath(testChunkID))

			resp, err := source.client.PushChunk(t.Context(), &objectstorev1.PushChunkRequest{
				ChunkId: testChunkID, ExpectedSha256: tt.expected, TargetNodeId: "target-node", TargetAddress: targetAddr,
			})
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			after, _ := os.ReadFile(target.finalPath(testChunkID))
			if tt.wantCode != codes.OK {
				if !bytes.Equal(before, after) {
					t.Error("the target's file changed although the source refused to copy")
				}
				return
			}
			if resp.GetNodeId() != "test-node" || resp.GetChunkId() != testChunkID {
				t.Errorf("response identifies %s/%s, want the source test-node/%s", resp.GetNodeId(), resp.GetChunkId(), testChunkID)
			}
			if resp.GetOutcome() != tt.wantOutcome {
				t.Fatalf("outcome = %s (%s), want %s", resp.GetOutcome(), resp.GetDetail(), tt.wantOutcome)
			}
			if tt.wantOutcome == objectstorev1.PushChunkResponse_OUTCOME_TARGET_HAS_CHUNK {
				if !bytes.Equal(before, after) {
					t.Error("the target's existing file was replaced")
				}
				return
			}
			// The copy is verified on the target: its own read checks header, ID and hash.
			frames, err := read(t.Context(), target.client, testChunkID)
			if err != nil || !bytes.Equal(bytes.Join(frames, nil), payload) {
				t.Fatalf("target read = %d bytes, %v; want the verified payload", len(bytes.Join(frames, nil)), err)
			}
		})
	}
}

func TestPushChunkTargetUnreachable(t *testing.T) {
	source := startServer(t, 4)
	writeValid(t, source, testChunkID, []byte("going nowhere"))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := lis.Addr().String()
	lis.Close()
	resp, err := source.client.PushChunk(t.Context(), &objectstorev1.PushChunkRequest{
		ChunkId: testChunkID, ExpectedSha256: sha([]byte("going nowhere")), TargetNodeId: "gone", TargetAddress: addr,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetOutcome() != objectstorev1.PushChunkResponse_OUTCOME_TARGET_FAILED {
		t.Errorf("outcome = %s, want TARGET_FAILED", resp.GetOutcome())
	}
}

func TestPushChunkHoldsAReadSlot(t *testing.T) {
	source := startServer(t, 1)
	writeValid(t, source, testChunkID, []byte("x"))
	source.srv.readSlots <- struct{}{}
	_, err := source.client.PushChunk(t.Context(), &objectstorev1.PushChunkRequest{
		ChunkId: testChunkID, ExpectedSha256: sha([]byte("x")), TargetNodeId: "t", TargetAddress: "127.0.0.1:1",
	})
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Fatalf("code with the only slot taken = %v, want ResourceExhausted", got)
	}
}
