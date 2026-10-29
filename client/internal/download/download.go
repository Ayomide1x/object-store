// Package download reads an object's chunks from their replicas, failing over
// to the next replica on errors, and writes the verified bytes to a file.
package download

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// Replica is one node holding a healthy copy of a chunk.
type Replica struct {
	NodeID  string
	Address string
	// Liveness is the coordinator's view of the node when it listed the
	// replica. It orders reads; it never removes a replica.
	Liveness objectstorev1.NodeLiveness
}

// Chunk is one chunk to read, with its healthy replicas in the coordinator's order.
type Chunk struct {
	ID       string
	Index    int32
	Size     int64
	SHA256   []byte
	Replicas []Replica
}

// Report is a hint for the coordinator that a replica served bad data or none.
type Report struct {
	ChunkID string
	NodeID  string
	Reason  objectstorev1.ReportBadReplicaRequest_Reason
	Detail  string
}

// Order returns the order in which to try a chunk's replicas.
type Order func([]Replica) []Replica

// Listed keeps the coordinator's order.
func Listed(r []Replica) []Replica { return r }

// Shuffled tries a chunk's replicas in a random order, spreading reads.
func Shuffled(r []Replica) []Replica {
	rand.Shuffle(len(r), func(i, j int) { r[i], r[j] = r[j], r[i] })
	return r
}

// Downloader reads the chunks of one object.
type Downloader struct {
	timeout time.Duration
	order   Order
	log     *slog.Logger
	// demoted holds nodes that were unreachable, hung or busy during this
	// read. They are tried last for the rest of it, so a hung node costs one
	// deadline per get rather than one per chunk; they are still tried if
	// every other replica fails.
	demoted map[string]bool
	reports []Report
	// buf holds one chunk's bytes until they are verified; reused per chunk.
	buf []byte
}

// New returns a Downloader. timeout bounds each replica read, connection included.
func New(timeout time.Duration, order Order, log *slog.Logger) *Downloader {
	return &Downloader{timeout: timeout, order: order, log: log, demoted: map[string]bool{}}
}

// Reports returns the bad-replica hints collected so far.
func (d *Downloader) Reports() []Report { return d.reports }

// errMismatch marks a replica that served bytes other than the recorded chunk.
var errMismatch = errors.New("replica served bytes that do not match the record")

// verdict is what a failed replica read means for the rest of the chunk.
type verdict struct {
	failOver bool // try the next replica
	demote   bool // try this node last for the rest of the get
	report   objectstorev1.ReportBadReplicaRequest_Reason
}

const noReport = objectstorev1.ReportBadReplicaRequest_REASON_UNSPECIFIED

// classify decides what a failed replica read means. A replica is reported
// only when it served bad data or claims not to have a chunk it is recorded as
// holding. Unreachable, slow or busy nodes are a liveness matter (stage 6).
func classify(err error) verdict {
	if errors.Is(err, errMismatch) {
		return verdict{failOver: true, report: objectstorev1.ReportBadReplicaRequest_REASON_CHECKSUM_MISMATCH}
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
		return verdict{failOver: true, demote: true, report: noReport}
	case codes.NotFound:
		return verdict{failOver: true, report: objectstorev1.ReportBadReplicaRequest_REASON_NOT_FOUND}
	case codes.DataLoss:
		return verdict{failOver: true, report: objectstorev1.ReportBadReplicaRequest_REASON_DATA_LOSS}
	case codes.InvalidArgument:
		// The request itself is malformed; every replica would refuse it.
		return verdict{failOver: false, report: noReport}
	default:
		// INTERNAL, UNKNOWN, UNIMPLEMENTED and anything else: not proof the
		// replica is bad, so fail over without reporting.
		return verdict{failOver: true, report: noReport}
	}
}

// rank orders replicas: ALIVE (or unreported) first, then SUSPECT and
// UNKNOWN, then DEAD, then nodes that already failed during this get. A DEAD
// replica is still tried, after every live one: liveness can be wrong.
func (d *Downloader) rank(r Replica) int {
	if d.demoted[r.NodeID] {
		return 3
	}
	switch r.Liveness {
	case objectstorev1.NodeLiveness_NODE_LIVENESS_SUSPECT, objectstorev1.NodeLiveness_NODE_LIVENESS_UNKNOWN:
		return 1
	case objectstorev1.NodeLiveness_NODE_LIVENESS_DEAD:
		return 2
	default: // ALIVE, or a coordinator that doesn't report liveness
		return 0
	}
}

// ReadChunk returns c's bytes, verified against c.SHA256, from the first
// replica that serves them. Each attempt reads into a reset buffer with a
// fresh hasher, so bytes from a replica that failed part-way never reach the
// result.
func (d *Downloader) ReadChunk(ctx context.Context, c Chunk) ([]byte, error) {
	order := d.order(append([]Replica(nil), c.Replicas...))
	// Liveness and demotion first; the shuffle (or listed order) within each rank.
	slices.SortStableFunc(order, func(a, b Replica) int { return d.rank(a) - d.rank(b) })
	if len(order) == 0 {
		return nil, fmt.Errorf("chunk %d (%s) has no healthy replicas", c.Index, c.ID)
	}

	var failures []string
	for _, r := range order {
		log := d.log.With("chunk_id", c.ID, "node_id", r.NodeID)
		data, err := d.readReplica(ctx, r, c)
		if err == nil {
			log.Info("chunk read", "index", c.Index, "bytes", len(data))
			return data, nil
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("read chunk %d (%s): %w", c.Index, c.ID, ctx.Err())
		}
		v := classify(err)
		failures = append(failures, fmt.Sprintf("%s: %v", r.NodeID, err))
		if v.demote {
			d.demoted[r.NodeID] = true
		}
		if v.report != noReport {
			d.reports = append(d.reports, Report{ChunkID: c.ID, NodeID: r.NodeID, Reason: v.report, Detail: err.Error()})
		}
		if !v.failOver {
			return nil, fmt.Errorf("read chunk %d (%s) from %s: %w", c.Index, c.ID, r.NodeID, err)
		}
		log.Warn("replica read failed; failing over", "index", c.Index, "error", err,
			"reported", v.report != noReport, "demoted", v.demote)
	}
	return nil, fmt.Errorf("chunk %d (%s): all %d replicas failed: %s",
		c.Index, c.ID, len(order), strings.Join(failures, "; "))
}

// readReplica reads c from one replica into the reset buffer and verifies it.
func (d *Downloader) readReplica(ctx context.Context, r Replica, c Chunk) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, d.timeout)
	defer cancel()

	conn, err := grpc.NewClient(r.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", r.Address, err)
	}
	defer conn.Close()
	stream, err := objectstorev1.NewStorageNodeServiceClient(conn).ReadChunk(ctx, &objectstorev1.ReadChunkRequest{ChunkId: c.ID})
	if err != nil {
		return nil, err
	}

	// Start clean: nothing from an earlier attempt survives.
	d.buf = d.buf[:0]
	hasher := sha256.New()
	for {
		resp, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if status.Code(err) == codes.DeadlineExceeded {
				return nil, fmt.Errorf("no answer within %s: %w", d.timeout, err)
			}
			return nil, err
		}
		if int64(len(d.buf)+len(resp.GetData())) > c.Size {
			return nil, fmt.Errorf("more than the chunk's %d bytes: %w", c.Size, errMismatch)
		}
		d.buf = append(d.buf, resp.GetData()...)
		hasher.Write(resp.GetData())
	}
	if int64(len(d.buf)) != c.Size {
		return nil, fmt.Errorf("got %d of %d bytes: %w", len(d.buf), c.Size, errMismatch)
	}
	if got := hasher.Sum(nil); !bytes.Equal(got, c.SHA256) {
		return nil, fmt.Errorf("sha256 %x, coordinator recorded %x: %w", got, c.SHA256, errMismatch)
	}
	return d.buf, nil
}

// ToFile reads every chunk in order and writes the object to path. Only
// verified chunks are written, to a temp file in the same directory that is
// fsynced and renamed over path once the whole object is there. A failed read
// leaves no file at path.
func (d *Downloader) ToFile(ctx context.Context, chunks []Chunk, size int64, path string) error {
	if err := checkChunkList(chunks, size); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".osctl-get-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	// Cleanup on failure. After a successful rename, Remove fails harmlessly.
	defer os.Remove(tmp.Name())
	defer tmp.Close()

	for _, c := range chunks {
		data, err := d.ReadChunk(ctx, c)
		if err != nil {
			return err
		}
		if _, err := tmp.Write(data); err != nil {
			return fmt.Errorf("write output: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("rename to %s: %w", path, err)
	}
	return nil
}

// checkChunkList rejects a chunk list whose indexes are not exactly 0..n-1 in
// order, whose sizes do not sum to the object size, or that lacks a SHA-256
// for any chunk: unverifiable data is not downloaded.
func checkChunkList(chunks []Chunk, size int64) error {
	var total int64
	for i, c := range chunks {
		if int(c.Index) != i {
			return fmt.Errorf("chunk list out of order or has a gap: position %d has index %d", i, c.Index)
		}
		if len(c.SHA256) != sha256.Size {
			return fmt.Errorf("chunk %d (%s) has no sha256 to verify against", i, c.ID)
		}
		total += c.Size
	}
	if total != size {
		return fmt.Errorf("chunk sizes sum to %d, object size is %d", total, size)
	}
	return nil
}
