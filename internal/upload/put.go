package upload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// MaxObjectSize is the largest object the coordinator accepts; it enforces
// the same limit at initiate. Callers check it first only to fail early.
const MaxObjectSize = 5 << 30

// MaxChunkSize is the coordinator's chunk size: no planned chunk is larger.
const MaxChunkSize = 4 << 20

// Source supplies the bytes of each chunk, in order, for its replica writes.
type Source interface {
	// Chunk returns where to read the chunk that covers [offset, offset+size)
	// of the object: a ReaderAt and the offset of the chunk within it. Every
	// replica write reads its own view of that range.
	Chunk(offset, size int64) (io.ReaderAt, int64, error)
}

// FileSource reads chunks straight from a file: no copy in memory, each
// replica reading its own range of the file. osctl uses it.
type FileSource struct{ File io.ReaderAt }

func (s FileSource) Chunk(offset, _ int64) (io.ReaderAt, int64, error) { return s.File, offset, nil }

// ErrSource matches (errors.Is) a Put error caused by the source failing to
// supply a chunk, such as a client that went away mid-body.
var ErrSource = errors.New("source failed")

// StreamSource reads a one-pass stream, one chunk at a time, into a buffer
// reused for every chunk: all replicas of a chunk read the same buffer, and
// the next chunk is read only after this one is written. Memory is one chunk
// (at most MaxChunkSize) per upload, whatever the object's size.
type StreamSource struct {
	r   io.Reader
	buf []byte
}

// NewStreamSource returns a StreamSource for an object of size bytes from r.
// Its buffer holds the largest chunk such an object can have.
func NewStreamSource(r io.Reader, size int64) *StreamSource {
	return &StreamSource{r: r, buf: make([]byte, min(size, MaxChunkSize))}
}

func (s *StreamSource) Chunk(_, size int64) (io.ReaderAt, int64, error) {
	if size > int64(len(s.buf)) {
		return nil, 0, fmt.Errorf("chunk of %d bytes exceeds the %d-byte buffer", size, len(s.buf))
	}
	if _, err := io.ReadFull(s.r, s.buf[:size]); err != nil {
		return nil, 0, err
	}
	return bytes.NewReader(s.buf[:size]), 0, nil
}

// Options are the per-upload settings.
type Options struct {
	// ChunkWriteTimeout bounds each replica write, connection included.
	ChunkWriteTimeout time.Duration
	// NodeAddresses says which node addresses the coordinator should return.
	NodeAddresses objectstorev1.NodeAddressSet
}

// Put uploads size bytes from src under key and returns the new object's ID:
// initiate (the coordinator returns the full chunk plan), write each chunk to
// all of its placed nodes in parallel, then complete. The coordinator decides
// what was stored by asking the nodes; the client reports only each chunk's
// SHA-256. Chunks go one after another. If any step fails the object is never
// completed: it stays PENDING, the key keeps its previous version, and garbage
// collection removes what was written.
//
// Errors wrap the coordinator's gRPC status where there is one, ErrQuorum when
// a chunk got too few replicas, and ErrSource when src failed.
func Put(ctx context.Context, coord objectstorev1.CoordinatorServiceClient, log *slog.Logger,
	key string, size int64, src Source, opts Options) (string, error) {
	initResp, err := coord.InitiateUpload(ctx, &objectstorev1.InitiateUploadRequest{
		Key: key, SizeBytes: size, NodeAddresses: opts.NodeAddresses,
	})
	if err != nil {
		return "", fmt.Errorf("initiate upload of %q: %w", key, err)
	}
	objectID := initResp.GetObjectId()
	log = log.With("object_id", objectID)
	if err := checkPlan(initResp.GetChunks(), size); err != nil {
		return "", fmt.Errorf("initiate upload of %q: %w", key, err)
	}

	up := New(objectID, opts.ChunkWriteTimeout, log)
	var completed []*objectstorev1.CompletedChunk
	var offset int64
	for _, c := range initResp.GetChunks() {
		chunk := Chunk{ID: c.GetChunkId(), Index: c.GetIndex(), Size: c.GetSizeBytes()}
		for _, n := range c.GetNodes() {
			chunk.Targets = append(chunk.Targets, Target{NodeID: n.GetNodeId(), Address: n.GetAddress()})
		}
		r, at, err := src.Chunk(offset, c.GetSizeBytes())
		if err != nil {
			return "", fmt.Errorf("upload of %q abandoned: read chunk %d: %w: %w", key, c.GetIndex(), ErrSource, err)
		}
		chunk.Offset = at
		sum, acked, err := up.WriteChunk(ctx, chunk, r)
		if err != nil {
			// Fewer than 2 replicas: the object can't commit. It stays PENDING
			// and the key keeps its previous version.
			return "", fmt.Errorf("upload of %q abandoned: %w", key, err)
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
		return "", fmt.Errorf("complete upload of %q: %w", key, err)
	}
	log.Info("upload complete", "key", key, "bytes", size, "chunks", len(completed))
	return objectID, nil
}

// checkPlan rejects a chunk plan that does not cover exactly size bytes with
// indexes 0..n-1 in order, each placed on at least one node and none larger
// than MaxChunkSize.
func checkPlan(chunks []*objectstorev1.ChunkPlacement, size int64) error {
	var total int64
	for i, c := range chunks {
		if int(c.GetIndex()) != i {
			return fmt.Errorf("chunk plan out of order: position %d has index %d", i, c.GetIndex())
		}
		if len(c.GetNodes()) == 0 {
			return fmt.Errorf("chunk %d (%s) has no nodes", i, c.GetChunkId())
		}
		if c.GetSizeBytes() < 0 || c.GetSizeBytes() > MaxChunkSize {
			return fmt.Errorf("chunk %d (%s) has size %d, outside [0, %d]", i, c.GetChunkId(), c.GetSizeBytes(), MaxChunkSize)
		}
		total += c.GetSizeBytes()
	}
	if total != size {
		return fmt.Errorf("chunk plan covers %d bytes, file is %d", total, size)
	}
	return nil
}
