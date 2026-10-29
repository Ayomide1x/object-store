package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
	"github.com/Ayomide1x/object-store/node/internal/chunkstore"
)

const testChunkID = "0b6f0d8e-7a64-4a8e-9b3a-2a1c7f0e5d11"

// startServer runs a Server on an in-memory listener and returns a client
// for it plus the chunk directory.
func startServer(t *testing.T) (objectstorev1.StorageNodeServiceClient, string) {
	t.Helper()
	dir := t.TempDir()
	store, err := chunkstore.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	lis := bufconn.Listen(16 << 20)
	srv := grpc.NewServer()
	objectstorev1.RegisterStorageNodeServiceServer(srv, New("test-node", store, log))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return objectstorev1.NewStorageNodeServiceClient(conn), dir
}

func header(size int64) *objectstorev1.WriteChunkRequest {
	return &objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Header{
		Header: &objectstorev1.WriteChunkHeader{ChunkId: testChunkID, SizeBytes: size},
	}}
}

func data(b []byte) *objectstorev1.WriteChunkRequest {
	return &objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Data{Data: b}}
}

// write sends frames and returns the RPC's final error.
func write(ctx context.Context, c objectstorev1.StorageNodeServiceClient, frames []*objectstorev1.WriteChunkRequest) error {
	stream, err := c.WriteChunk(ctx)
	if err != nil {
		return err
	}
	for _, f := range frames {
		// A Send error means the server already failed the RPC; the real
		// status comes from CloseAndRecv.
		if err := stream.Send(f); err != nil {
			break
		}
	}
	_, err = stream.CloseAndRecv()
	return err
}

func TestWriteChunkFraming(t *testing.T) {
	oneMiB := bytes.Repeat([]byte{'a'}, MaxFrameSize)

	tests := []struct {
		name     string
		frames   []*objectstorev1.WriteChunkRequest
		wantCode codes.Code
		wantFile bool
	}{
		{"zero bytes", []*objectstorev1.WriteChunkRequest{header(0)}, codes.OK, true},
		{"one frame", []*objectstorev1.WriteChunkRequest{header(3), data([]byte("abc"))}, codes.OK, true},
		{"full chunk in 1 MiB frames", []*objectstorev1.WriteChunkRequest{header(MaxChunkSize), data(oneMiB), data(oneMiB), data(oneMiB), data(oneMiB)}, codes.OK, true},
		{"no frames", nil, codes.InvalidArgument, false},
		{"data before header", []*objectstorev1.WriteChunkRequest{data([]byte("abc")), header(3)}, codes.InvalidArgument, false},
		{"header repeated", []*objectstorev1.WriteChunkRequest{header(3), header(3), data([]byte("abc"))}, codes.InvalidArgument, false},
		{"empty frame", []*objectstorev1.WriteChunkRequest{header(3), {}}, codes.InvalidArgument, false},
		{"too few bytes", []*objectstorev1.WriteChunkRequest{header(4), data([]byte("abc"))}, codes.InvalidArgument, false},
		{"too many bytes", []*objectstorev1.WriteChunkRequest{header(2), data([]byte("abc"))}, codes.InvalidArgument, false},
		{"size over 4 MiB", []*objectstorev1.WriteChunkRequest{header(MaxChunkSize + 1)}, codes.InvalidArgument, false},
		{"frame over 1 MiB", []*objectstorev1.WriteChunkRequest{header(MaxFrameSize + 1), data(append(oneMiB, 'a'))}, codes.InvalidArgument, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, dir := startServer(t)
			err := write(t.Context(), client, tt.frames)
			if got := status.Code(err); got != tt.wantCode {
				t.Fatalf("code = %v, want %v (err: %v)", got, tt.wantCode, err)
			}
			_, statErr := os.Stat(filepath.Join(dir, testChunkID))
			if exists := statErr == nil; exists != tt.wantFile {
				t.Errorf("chunk file exists = %v, want %v", exists, tt.wantFile)
			}
		})
	}
}

func TestWriteChunkNeverOverwrites(t *testing.T) {
	client, dir := startServer(t)
	ctx := t.Context()

	if err := write(ctx, client, []*objectstorev1.WriteChunkRequest{header(3), data([]byte("old"))}); err != nil {
		t.Fatalf("first write: %v", err)
	}
	err := write(ctx, client, []*objectstorev1.WriteChunkRequest{header(3), data([]byte("new"))})
	if got := status.Code(err); got != codes.AlreadyExists {
		t.Fatalf("second write code = %v, want AlreadyExists", got)
	}
	got, err := os.ReadFile(filepath.Join(dir, testChunkID))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "old" {
		t.Errorf("chunk content = %q, want %q", got, "old")
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
			client, dir := startServer(t)
			want := bytes.Repeat([]byte{'z'}, tt.size)
			if err := os.WriteFile(filepath.Join(dir, testChunkID), want, 0o644); err != nil {
				t.Fatal(err)
			}

			stream, err := client.ReadChunk(t.Context(), &objectstorev1.ReadChunkRequest{ChunkId: testChunkID})
			if err != nil {
				t.Fatal(err)
			}
			var got []byte
			frames := 0
			for {
				resp, err := stream.Recv()
				if errors.Is(err, io.EOF) {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if len(resp.GetData()) > MaxFrameSize {
					t.Fatalf("frame of %d bytes exceeds %d", len(resp.GetData()), MaxFrameSize)
				}
				got = append(got, resp.GetData()...)
				frames++
			}
			if !bytes.Equal(got, want) {
				t.Errorf("read %d bytes, want %d identical bytes", len(got), len(want))
			}
			if frames != tt.wantFrames {
				t.Errorf("frames = %d, want %d", frames, tt.wantFrames)
			}
		})
	}
}

func TestReadChunkMissing(t *testing.T) {
	client, _ := startServer(t)
	stream, err := client.ReadChunk(t.Context(), &objectstorev1.ReadChunkRequest{ChunkId: testChunkID})
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if got := status.Code(err); got != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", got)
	}
}
