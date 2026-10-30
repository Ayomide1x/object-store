// Package upload writes each chunk of an object to all of its placed nodes in
// parallel (client fan-out) and enforces the write quorum. Put runs a whole
// upload: initiate, every chunk, complete. osctl and the gateway share it.
package upload

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Ayomide1x/object-store/internal/chunkwrite"
)

// Quorum is the number of replicas that must acknowledge a chunk for the
// upload to continue. The coordinator enforces the same rule at commit.
const Quorum = 2

// ErrQuorum matches (errors.Is) the error WriteChunk returns when fewer than
// Quorum replicas acknowledged: the cluster could not take the chunk now.
var ErrQuorum = errors.New("write quorum not reached")

// quorumError carries WriteChunk's message unchanged and matches ErrQuorum.
type quorumError struct{ msg string }

func (e quorumError) Error() string        { return e.msg }
func (e quorumError) Is(target error) bool { return target == ErrQuorum }

// Target is one node a chunk is placed on.
type Target struct {
	NodeID  string
	Address string
}

// Chunk is one chunk of the upload plan.
type Chunk struct {
	ID      string
	Index   int32
	Size    int64
	Offset  int64 // where the chunk starts in the source
	Targets []Target
}

// Uploader writes the chunks of one object. It remembers nodes that failed
// during this upload and does not try them again: without that, a hung node
// would cost the full write deadline on every chunk placed on it. This is
// local to one upload, not cluster liveness.
type Uploader struct {
	objectID string
	timeout  time.Duration
	log      *slog.Logger
	// skipped maps a node ID to the error that took it out of this upload.
	skipped map[string]error
}

// New returns an Uploader for one object. timeout bounds each replica write,
// connection included.
func New(objectID string, timeout time.Duration, log *slog.Logger) *Uploader {
	return &Uploader{objectID: objectID, timeout: timeout, log: log, skipped: map[string]error{}}
}

type replicaResult struct {
	nodeID string
	sum    []byte
	err    error
}

// WriteChunk writes c to all its targets in parallel, each reading its own
// view of src, and returns the SHA-256 and the IDs of the nodes that
// acknowledged it. It returns an error if fewer than Quorum acknowledged; the
// caller must then abandon the upload.
func (u *Uploader) WriteChunk(ctx context.Context, c Chunk, src io.ReaderAt) ([]byte, []string, error) {
	results := make(chan replicaResult, len(c.Targets))
	launched := 0
	var failures []string
	for _, t := range c.Targets {
		if err, ok := u.skipped[t.NodeID]; ok {
			failures = append(failures, fmt.Sprintf("%s: skipped after earlier failure (%v)", t.NodeID, err))
			continue
		}
		launched++
		go func() {
			wctx, cancel := context.WithTimeout(ctx, u.timeout)
			defer cancel()
			sum, err := chunkwrite.Write(wctx, t.Address,
				chunkwrite.Header{ChunkID: c.ID, ObjectID: u.objectID, Size: c.Size},
				io.NewSectionReader(src, c.Offset, c.Size))
			results <- replicaResult{nodeID: t.NodeID, sum: sum, err: err}
		}()
	}

	var acked []string
	var sum []byte
	for range launched {
		r := <-results
		log := u.log.With("object_id", u.objectID, "chunk_id", c.ID, "node_id", r.nodeID)
		switch {
		case r.err != nil:
			if status.Code(r.err) == codes.DeadlineExceeded {
				r.err = fmt.Errorf("no answer within %s: %w", u.timeout, r.err)
			}
			log.Warn("replica write failed; node skipped for the rest of this upload", "error", r.err)
			u.skipped[r.nodeID] = r.err
			failures = append(failures, fmt.Sprintf("%s: %v", r.nodeID, r.err))
		case sum != nil && !bytes.Equal(sum, r.sum):
			// Every replica reads the same byte range; different hashes mean
			// the source changed mid-upload.
			return nil, nil, fmt.Errorf("chunk %d (%s): replicas hashed to %x and %x (file changed during upload?)",
				c.Index, c.ID, sum, r.sum)
		default:
			sum = r.sum
			acked = append(acked, r.nodeID)
			log.Info("replica written", "index", c.Index, "bytes", c.Size)
		}
	}

	if len(acked) < Quorum {
		return nil, nil, quorumError{fmt.Sprintf("chunk %d (%s): %d of %d replicas acknowledged, need %d: %s",
			c.Index, c.ID, len(acked), len(c.Targets), Quorum, strings.Join(failures, "; "))}
	}
	return sum, acked, nil
}
