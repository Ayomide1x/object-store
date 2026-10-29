package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Ayomide1x/object-store/client/internal/upload"
	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

const defaultChunkWriteTimeout = 10 * time.Second

// put uploads a file: initiate (the coordinator returns the full chunk plan),
// write each chunk to all of its placed nodes in parallel, then complete. The
// coordinator decides what was stored by asking the nodes; the client reports
// only each chunk's SHA-256. Chunks go one after another; each replica reads
// its own view of the file, one frame at a time, hashing as it streams.
func put(ctx context.Context, log *slog.Logger, coordAddr, path, key string) error {
	timeout, err := chunkWriteTimeout()
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}
	size := info.Size()

	conn, err := dial(coordAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	coord := objectstorev1.NewCoordinatorServiceClient(conn)

	initResp, err := coord.InitiateUpload(ctx, &objectstorev1.InitiateUploadRequest{Key: key, SizeBytes: size})
	if err != nil {
		return fmt.Errorf("initiate upload of %q: %w", key, err)
	}
	objectID := initResp.GetObjectId()
	log = log.With("object_id", objectID)
	if err := checkPlan(initResp.GetChunks(), size); err != nil {
		return fmt.Errorf("initiate upload of %q: %w", key, err)
	}

	up := upload.New(objectID, timeout, log)
	var completed []*objectstorev1.CompletedChunk
	var offset int64
	for _, c := range initResp.GetChunks() {
		chunk := upload.Chunk{ID: c.GetChunkId(), Index: c.GetIndex(), Size: c.GetSizeBytes(), Offset: offset}
		for _, n := range c.GetNodes() {
			chunk.Targets = append(chunk.Targets, upload.Target{NodeID: n.GetNodeId(), Address: n.GetAddress()})
		}
		sum, acked, err := up.WriteChunk(ctx, chunk, f)
		if err != nil {
			// Fewer than 2 replicas: the object can't commit. It stays PENDING
			// and the key keeps its previous version.
			return fmt.Errorf("upload of %q abandoned: %w", key, err)
		}
		log.Info("chunk written", "chunk_id", c.GetChunkId(), "index", c.GetIndex(), "replicas", len(acked),
			"nodes", acked, "sha256", fmt.Sprintf("%x", sum))
		completed = append(completed, &objectstorev1.CompletedChunk{
			ChunkId:   c.GetChunkId(),
			Index:     c.GetIndex(),
			SizeBytes: c.GetSizeBytes(),
			Sha256:    sum,
		})
		offset += c.GetSizeBytes()
	}

	_, err = coord.CompleteUpload(ctx, &objectstorev1.CompleteUploadRequest{
		ObjectId:  objectID,
		Key:       key,
		SizeBytes: size,
		Chunks:    completed,
	})
	if err != nil {
		return fmt.Errorf("complete upload of %q: %w", key, err)
	}
	log.Info("upload complete", "key", key, "bytes", size, "chunks", len(completed))
	return nil
}

// chunkWriteTimeout reads OSCTL_CHUNK_WRITE_TIMEOUT, a Go duration such as
// "10s", which bounds each replica write.
func chunkWriteTimeout() (time.Duration, error) {
	v := os.Getenv("OSCTL_CHUNK_WRITE_TIMEOUT")
	if v == "" {
		return defaultChunkWriteTimeout, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("OSCTL_CHUNK_WRITE_TIMEOUT must be a positive duration such as 10s, got %q", v)
	}
	return d, nil
}

// checkPlan rejects a chunk plan that does not cover exactly size bytes with
// indexes 0..n-1 in order, each placed on at least one node.
func checkPlan(chunks []*objectstorev1.ChunkPlacement, size int64) error {
	var total int64
	for i, c := range chunks {
		if int(c.GetIndex()) != i {
			return fmt.Errorf("chunk plan out of order: position %d has index %d", i, c.GetIndex())
		}
		if len(c.GetNodes()) == 0 {
			return fmt.Errorf("chunk %d (%s) has no nodes", i, c.GetChunkId())
		}
		total += c.GetSizeBytes()
	}
	if total != size {
		return fmt.Errorf("chunk plan covers %d bytes, file is %d", total, size)
	}
	return nil
}
