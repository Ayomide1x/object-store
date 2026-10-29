package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// get downloads an object into path. Bytes go to a temp file in the same
// directory, which is renamed over path only after every chunk arrived with
// the expected size, so a failed get never leaves a truncated file at path.
func get(ctx context.Context, log *slog.Logger, coordAddr, key, path string) error {
	conn, err := dial(coordAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	obj, err := objectstorev1.NewCoordinatorServiceClient(conn).GetObject(ctx, &objectstorev1.GetObjectRequest{Key: key})
	if err != nil {
		return fmt.Errorf("get object %q: %w", key, err)
	}
	log = log.With("object_id", obj.GetObjectId())

	tmp, err := os.CreateTemp(filepath.Dir(path), ".osctl-get-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	// Cleanup on failure. After a successful rename, Remove fails harmlessly.
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	var total int64
	for _, chunk := range obj.GetChunks() {
		if len(chunk.GetReplicas()) == 0 {
			return fmt.Errorf("chunk %s has no replicas", chunk.GetChunkId())
		}
		replica := chunk.GetReplicas()[0]
		n, err := readChunk(ctx, replica.GetAddress(), chunk.GetChunkId(), tmp)
		if err != nil {
			return fmt.Errorf("read chunk %s from %s: %w", chunk.GetChunkId(), replica.GetNodeId(), err)
		}
		if n != chunk.GetSizeBytes() {
			return fmt.Errorf("chunk %s: got %d bytes, expected %d", chunk.GetChunkId(), n, chunk.GetSizeBytes())
		}
		log.Info("chunk read", "chunk_id", chunk.GetChunkId(), "node_id", replica.GetNodeId(), "bytes", n)
		total += n
	}
	if total != obj.GetSizeBytes() {
		return fmt.Errorf("object %q: got %d bytes, expected %d", key, total, obj.GetSizeBytes())
	}

	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("rename to %s: %w", path, err)
	}
	log.Info("download complete", "key", key, "bytes", total)
	return nil
}

// readChunk streams one chunk from a node into w and returns the bytes read.
func readChunk(ctx context.Context, addr, chunkID string, w io.Writer) (int64, error) {
	conn, err := dial(addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	stream, err := objectstorev1.NewStorageNodeServiceClient(conn).ReadChunk(ctx, &objectstorev1.ReadChunkRequest{ChunkId: chunkID})
	if err != nil {
		return 0, fmt.Errorf("open stream: %w", err)
	}
	var n int64
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return n, nil
		}
		if err != nil {
			return n, err
		}
		written, err := w.Write(resp.GetData())
		n += int64(written)
		if err != nil {
			return n, fmt.Errorf("write output: %w", err)
		}
	}
}
