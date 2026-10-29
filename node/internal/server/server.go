// Package server implements StorageNodeService over a chunkstore.Store.
package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
	"github.com/Ayomide1x/object-store/internal/chunkwrite"
	"github.com/Ayomide1x/object-store/node/internal/chunkstore"
)

const (
	// MaxChunkSize is the largest chunk a node accepts.
	MaxChunkSize = chunkstore.MaxChunkSize
	// MaxFrameSize is the largest data frame sent or accepted on a stream.
	MaxFrameSize = 1 << 20
)

// Server serves chunk reads and writes for one node.
type Server struct {
	objectstorev1.UnimplementedStorageNodeServiceServer

	nodeID string
	store  *chunkstore.Store
	log    *slog.Logger
	// readSlots bounds concurrent reads. Each read holds a whole chunk in
	// memory (up to 4 MiB) while verifying it, so this caps read memory.
	readSlots chan struct{}
}

// New returns a Server that stores chunks in store and serves at most
// maxReads reads at once.
func New(nodeID string, store *chunkstore.Store, log *slog.Logger, maxReads int) *Server {
	return &Server{nodeID: nodeID, store: store, log: log, readSlots: make(chan struct{}, maxReads)}
}

// WriteChunk receives one header frame, data frames, then one trailer frame
// with the data's SHA-256. The chunk becomes visible under its final name
// only if the size and hash match and the file is durable.
func (s *Server) WriteChunk(stream objectstorev1.StorageNodeService_WriteChunkServer) error {
	ctx := stream.Context()

	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "stream ended before header frame")
	}
	if err != nil {
		return fmt.Errorf("receive header: %w", err)
	}
	header := first.GetHeader()
	if header == nil {
		return status.Error(codes.InvalidArgument, "first frame must be a header")
	}
	if header.GetSizeBytes() < 0 || header.GetSizeBytes() > MaxChunkSize {
		return status.Errorf(codes.InvalidArgument, "size_bytes %d outside [0, %d]", header.GetSizeBytes(), MaxChunkSize)
	}

	log := s.log.With("node_id", s.nodeID, "object_id", header.GetObjectId(), "chunk_id", header.GetChunkId())

	// Fail fast before receiving the data. This is not the guarantee: the
	// final link in Commit is what refuses to replace a chunk.
	exists, err := s.store.Exists(ctx, header.GetChunkId())
	switch {
	case errors.Is(err, chunkstore.ErrInvalidID):
		return status.Error(codes.InvalidArgument, err.Error())
	case err != nil:
		log.Error("check chunk exists", "error", err)
		return status.Error(codes.Internal, err.Error())
	case exists:
		log.Warn("write rejected: chunk exists")
		return status.Errorf(codes.AlreadyExists, "chunk %s already exists", header.GetChunkId())
	}

	w, err := s.store.NewWriter(ctx, header.GetChunkId())
	if err != nil {
		log.Error("start chunk write", "error", err)
		return status.Error(codes.Internal, err.Error())
	}

	sum, err := receiveData(stream, w, header.GetSizeBytes())
	if err != nil {
		w.Abort()
		log.Warn("write failed", "error", err, "bytes_received", w.Written())
		return err
	}

	err = w.Commit(ctx, sum)
	switch {
	case errors.Is(err, chunkstore.ErrChecksumMismatch):
		// The request contradicts itself; the client still has the data and
		// may retry. Nothing exists under the final name.
		log.Warn("write rejected: checksum mismatch", "error", err)
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, chunkstore.ErrExists):
		log.Warn("write rejected: chunk exists at link")
		return status.Error(codes.AlreadyExists, err.Error())
	case err != nil:
		log.Error("commit chunk", "error", err)
		return status.Error(codes.Internal, err.Error())
	}

	log.Info("chunk written", "bytes", w.Written(), "sha256", fmt.Sprintf("%x", sum))
	return stream.SendAndClose(&objectstorev1.WriteChunkResponse{
		ChunkId:      header.GetChunkId(),
		BytesWritten: w.Written(),
		Sha256:       sum,
	})
}

// receiveData copies data frames into w until the trailer frame, then
// requires the stream to end. It returns the trailer's SHA-256, or a gRPC
// status error if the frames are malformed or their total differs from want.
func receiveData(stream objectstorev1.StorageNodeService_WriteChunkServer, w io.Writer, want int64) ([]byte, error) {
	var got int64
	var trailer *objectstorev1.WriteChunkTrailer
	for trailer == nil {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil, status.Error(codes.InvalidArgument, "stream ended without trailer frame")
		}
		if err != nil {
			return nil, fmt.Errorf("receive frame: %w", err)
		}
		switch frame := req.GetFrame().(type) {
		case *objectstorev1.WriteChunkRequest_Data:
			if len(frame.Data) > MaxFrameSize {
				return nil, status.Errorf(codes.InvalidArgument, "frame of %d bytes exceeds %d", len(frame.Data), MaxFrameSize)
			}
			if got+int64(len(frame.Data)) > want {
				return nil, status.Errorf(codes.InvalidArgument, "received more than size_bytes %d", want)
			}
			if _, err := w.Write(frame.Data); err != nil {
				return nil, status.Errorf(codes.Internal, "write chunk: %v", err)
			}
			got += int64(len(frame.Data))
		case *objectstorev1.WriteChunkRequest_Trailer:
			trailer = frame.Trailer
		case *objectstorev1.WriteChunkRequest_Header:
			return nil, status.Error(codes.InvalidArgument, "header frame repeated")
		default:
			return nil, status.Error(codes.InvalidArgument, "empty frame")
		}
	}

	// The trailer must be the last frame.
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		if err != nil {
			return nil, fmt.Errorf("receive after trailer: %w", err)
		}
		return nil, status.Error(codes.InvalidArgument, "frame after trailer")
	}
	if got != want {
		return nil, status.Errorf(codes.InvalidArgument, "received %d bytes, header said %d", got, want)
	}
	if len(trailer.GetSha256()) != 32 {
		return nil, status.Errorf(codes.InvalidArgument, "trailer sha256 is %d bytes, want 32", len(trailer.GetSha256()))
	}
	return trailer.GetSha256(), nil
}

// maxStatBatch caps one StatChunks request. A 5 GiB object has 1,280 chunks;
// the cap leaves headroom while bounding the work a single call can demand.
const maxStatBatch = 4096

// StatChunks reports, for each requested chunk, whether this node holds a
// durable verified copy. It reads headers only. A missing or bad chunk is an
// entry in the response, not an error: the caller needs the whole picture.
func (s *Server) StatChunks(ctx context.Context, req *objectstorev1.StatChunksRequest) (*objectstorev1.StatChunksResponse, error) {
	if len(req.GetChunkIds()) > maxStatBatch {
		return nil, status.Errorf(codes.InvalidArgument, "%d chunk ids exceeds the batch limit of %d", len(req.GetChunkIds()), maxStatBatch)
	}
	resp := &objectstorev1.StatChunksResponse{NodeId: s.nodeID}
	for _, id := range req.GetChunkIds() {
		stat := &objectstorev1.ChunkStat{ChunkId: id}
		size, sum, err := s.store.Stat(ctx, id)
		switch {
		case err == nil:
			stat.State = objectstorev1.ChunkStat_STATE_FOUND
			stat.SizeBytes = size
			stat.Sha256 = sum
		case errors.Is(err, chunkstore.ErrNotFound):
			stat.State = objectstorev1.ChunkStat_STATE_MISSING
		case errors.Is(err, chunkstore.ErrCorrupt):
			s.log.Error("stat found a bad chunk header", "node_id", s.nodeID, "chunk_id", id, "error", err)
			stat.State = objectstorev1.ChunkStat_STATE_BAD_HEADER
		case errors.Is(err, chunkstore.ErrInvalidID):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		default:
			s.log.Error("stat chunk", "node_id", s.nodeID, "chunk_id", id, "error", err)
			return nil, status.Errorf(codes.Internal, "stat chunk %s: %v", id, err)
		}
		resp.Stats = append(resp.Stats, stat)
	}
	return resp, nil
}

// VerifyChunk re-reads a whole chunk and reports whether it is healthy: header
// valid and naming this chunk, data matching the stored hash, and the stored
// hash matching expected_sha256, the coordinator's record. The coordinator
// asks this before it marks a replica bad on a client's report. It holds a
// read slot, like ReadChunk, so reports can't add unbounded work.
func (s *Server) VerifyChunk(ctx context.Context, req *objectstorev1.VerifyChunkRequest) (*objectstorev1.VerifyChunkResponse, error) {
	log := s.log.With("node_id", s.nodeID, "chunk_id", req.GetChunkId())
	select {
	case s.readSlots <- struct{}{}:
		defer func() { <-s.readSlots }()
	default:
		return nil, status.Errorf(codes.ResourceExhausted, "node %s is serving %d reads; retry later", s.nodeID, cap(s.readSlots))
	}

	resp := &objectstorev1.VerifyChunkResponse{NodeId: s.nodeID, ChunkId: req.GetChunkId()}
	data, err := s.store.Read(ctx, req.GetChunkId())
	switch {
	case errors.Is(err, chunkstore.ErrInvalidID):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, chunkstore.ErrNotFound):
		resp.State = objectstorev1.VerifyChunkResponse_STATE_MISSING
		resp.Detail = "no file under the chunk's final name"
	case errors.Is(err, chunkstore.ErrCorrupt):
		resp.State = objectstorev1.VerifyChunkResponse_STATE_CORRUPT
		resp.Detail = err.Error()
	case err != nil:
		log.Error("verify chunk", "error", err)
		return nil, status.Errorf(codes.Internal, "verify chunk %s: %v", req.GetChunkId(), err)
	default:
		// The file is internally consistent; it must also be the chunk the
		// coordinator recorded.
		if sum := sha256.Sum256(data); !bytes.Equal(sum[:], req.GetExpectedSha256()) {
			resp.State = objectstorev1.VerifyChunkResponse_STATE_CORRUPT
			resp.Detail = fmt.Sprintf("stored sha256 %x differs from the coordinator's record %x", sum, req.GetExpectedSha256())
		} else {
			resp.State = objectstorev1.VerifyChunkResponse_STATE_HEALTHY
		}
	}
	log.Info("chunk verified on request", "state", resp.GetState().String(), "detail", resp.GetDetail())
	return resp, nil
}

// PushChunk copies a chunk this node holds to another node, for repair. It
// verifies its own copy first (header, chunk ID, data hash) and that the hash
// is the coordinator's record, holding a read slot like ReadChunk, then
// streams it to the target's ordinary WriteChunk, which verifies again and
// writes durably without replacing anything. Problems with this node's copy
// are status errors; what happened at the target is the outcome.
func (s *Server) PushChunk(ctx context.Context, req *objectstorev1.PushChunkRequest) (*objectstorev1.PushChunkResponse, error) {
	log := s.log.With("node_id", s.nodeID, "chunk_id", req.GetChunkId(), "target_node_id", req.GetTargetNodeId())
	if req.GetTargetAddress() == "" {
		return nil, status.Error(codes.InvalidArgument, "target_address is required")
	}
	select {
	case s.readSlots <- struct{}{}:
		defer func() { <-s.readSlots }()
	default:
		return nil, status.Errorf(codes.ResourceExhausted, "node %s is serving %d reads; retry later", s.nodeID, cap(s.readSlots))
	}

	data, err := s.store.Read(ctx, req.GetChunkId())
	switch {
	case errors.Is(err, chunkstore.ErrInvalidID):
		return nil, status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, chunkstore.ErrNotFound):
		return nil, status.Error(codes.NotFound, err.Error())
	case errors.Is(err, chunkstore.ErrCorrupt):
		log.Error("push source failed verification; refusing to copy it", "error", err)
		return nil, status.Error(codes.DataLoss, err.Error())
	case err != nil:
		return nil, status.Errorf(codes.Internal, "read chunk %s: %v", req.GetChunkId(), err)
	}
	if sum := sha256.Sum256(data); !bytes.Equal(sum[:], req.GetExpectedSha256()) {
		log.Error("push source is not the recorded chunk; refusing to copy it")
		return nil, status.Errorf(codes.DataLoss, "chunk %s: stored sha256 %x differs from the coordinator's record %x",
			req.GetChunkId(), sum, req.GetExpectedSha256())
	}

	resp := &objectstorev1.PushChunkResponse{NodeId: s.nodeID, ChunkId: req.GetChunkId()}
	_, err = chunkwrite.Write(ctx, req.GetTargetAddress(),
		chunkwrite.Header{ChunkID: req.GetChunkId(), Size: int64(len(data))}, bytes.NewReader(data))
	switch {
	case err == nil:
		resp.Outcome = objectstorev1.PushChunkResponse_OUTCOME_COPIED
	case status.Code(err) == codes.AlreadyExists:
		resp.Outcome = objectstorev1.PushChunkResponse_OUTCOME_TARGET_HAS_CHUNK
		resp.Detail = err.Error()
	default:
		resp.Outcome = objectstorev1.PushChunkResponse_OUTCOME_TARGET_FAILED
		resp.Detail = err.Error()
	}
	log.Info("chunk pushed", "outcome", resp.GetOutcome().String(), "bytes", len(data), "detail", resp.GetDetail())
	return resp, nil
}

// ReadChunk verifies the whole chunk before sending its first frame, then
// streams it in frames of at most MaxFrameSize. A chunk that fails
// verification is never sent: the caller gets DATA_LOSS and no data.
func (s *Server) ReadChunk(req *objectstorev1.ReadChunkRequest, stream objectstorev1.StorageNodeService_ReadChunkServer) error {
	ctx := stream.Context()
	log := s.log.With("node_id", s.nodeID, "chunk_id", req.GetChunkId())

	select {
	case s.readSlots <- struct{}{}:
		defer func() { <-s.readSlots }()
	default:
		log.Warn("read rejected: concurrent read limit reached", "limit", cap(s.readSlots))
		return status.Errorf(codes.ResourceExhausted, "node %s is serving %d reads; retry later", s.nodeID, cap(s.readSlots))
	}

	data, err := s.store.Read(ctx, req.GetChunkId())
	switch {
	case errors.Is(err, chunkstore.ErrInvalidID):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, chunkstore.ErrNotFound):
		log.Warn("read of missing chunk")
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, chunkstore.ErrCorrupt):
		log.Error("chunk failed verification; refusing to serve it", "error", err)
		return status.Error(codes.DataLoss, err.Error())
	case err != nil:
		log.Error("read chunk", "error", err)
		return status.Error(codes.Internal, err.Error())
	}

	// data is never modified after this point, so sending slices of it is
	// safe (gRPC forbids modifying a message after Send).
	for off := 0; off < len(data); off += MaxFrameSize {
		end := min(off+MaxFrameSize, len(data))
		if err := stream.Send(&objectstorev1.ReadChunkResponse{Data: data[off:end]}); err != nil {
			return fmt.Errorf("send frame of chunk %s: %w", req.GetChunkId(), err)
		}
	}
	log.Info("chunk read", "bytes", len(data))
	return nil
}
