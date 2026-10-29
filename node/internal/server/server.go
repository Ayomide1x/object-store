// Package server implements StorageNodeService over a chunkstore.Store.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
	"github.com/Ayomide1x/object-store/node/internal/chunkstore"
)

const (
	// MaxChunkSize is the largest chunk a node accepts.
	MaxChunkSize = 4 << 20
	// MaxFrameSize is the largest data frame sent or accepted on a stream.
	MaxFrameSize = 1 << 20
)

// Server serves chunk reads and writes for one node.
type Server struct {
	objectstorev1.UnimplementedStorageNodeServiceServer

	nodeID string
	store  *chunkstore.Store
	log    *slog.Logger
}

// New returns a Server that stores chunks in store.
func New(nodeID string, store *chunkstore.Store, log *slog.Logger) *Server {
	return &Server{nodeID: nodeID, store: store, log: log}
}

// WriteChunk receives one header frame followed by data frames and writes the
// chunk to a new file. Any framing or size violation fails the RPC and removes
// the partial file.
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

	f, err := s.store.Create(ctx, header.GetChunkId())
	switch {
	case errors.Is(err, chunkstore.ErrInvalidID):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, chunkstore.ErrExists):
		log.Warn("write rejected: chunk exists")
		return status.Error(codes.AlreadyExists, err.Error())
	case err != nil:
		log.Error("create chunk file", "error", err)
		return status.Error(codes.Internal, err.Error())
	}

	written, err := receiveData(stream, f, header.GetSizeBytes())
	closeErr := f.Close()
	if err == nil && closeErr != nil {
		err = status.Errorf(codes.Internal, "close chunk file: %v", closeErr)
	}
	if err != nil {
		// Use a fresh context: the stream's may already be cancelled, and the
		// partial file must still go.
		if rmErr := s.store.Remove(context.WithoutCancel(ctx), header.GetChunkId()); rmErr != nil {
			log.Error("remove partial chunk", "error", rmErr)
		}
		log.Warn("write failed", "error", err, "bytes_received", written)
		return err
	}

	log.Info("chunk written", "bytes", written)
	return stream.SendAndClose(&objectstorev1.WriteChunkResponse{
		ChunkId:      header.GetChunkId(),
		BytesWritten: written,
	})
}

// receiveData copies data frames from stream into w until the client closes
// the stream. It returns a gRPC status error if the frames are malformed or
// their total differs from want.
func receiveData(stream objectstorev1.StorageNodeService_WriteChunkServer, w io.Writer, want int64) (int64, error) {
	var got int64
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return got, fmt.Errorf("receive frame: %w", err)
		}
		var data []byte
		switch frame := req.GetFrame().(type) {
		case *objectstorev1.WriteChunkRequest_Data:
			data = frame.Data
		case *objectstorev1.WriteChunkRequest_Header:
			return got, status.Error(codes.InvalidArgument, "header frame repeated")
		default:
			return got, status.Error(codes.InvalidArgument, "empty frame")
		}
		if len(data) > MaxFrameSize {
			return got, status.Errorf(codes.InvalidArgument, "frame of %d bytes exceeds %d", len(data), MaxFrameSize)
		}
		if got+int64(len(data)) > want {
			return got, status.Errorf(codes.InvalidArgument, "received more than size_bytes %d", want)
		}
		if _, err := w.Write(data); err != nil {
			return got, status.Errorf(codes.Internal, "write chunk file: %v", err)
		}
		got += int64(len(data))
	}
	if got != want {
		return got, status.Errorf(codes.InvalidArgument, "received %d bytes, header said %d", got, want)
	}
	return got, nil
}

// ReadChunk streams a chunk back in frames of at most MaxFrameSize.
func (s *Server) ReadChunk(req *objectstorev1.ReadChunkRequest, stream objectstorev1.StorageNodeService_ReadChunkServer) error {
	ctx := stream.Context()
	log := s.log.With("node_id", s.nodeID, "chunk_id", req.GetChunkId())

	f, err := s.store.Open(ctx, req.GetChunkId())
	switch {
	case errors.Is(err, chunkstore.ErrInvalidID):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, chunkstore.ErrNotFound):
		log.Warn("read of missing chunk")
		return status.Error(codes.NotFound, err.Error())
	case err != nil:
		log.Error("open chunk file", "error", err)
		return status.Error(codes.Internal, err.Error())
	}
	defer f.Close()

	var sent int64
	for {
		// A fresh buffer per frame: gRPC forbids modifying a message after Send.
		buf := make([]byte, MaxFrameSize)
		n, err := io.ReadFull(f, buf)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			err = io.EOF
		}
		if n > 0 {
			if sendErr := stream.Send(&objectstorev1.ReadChunkResponse{Data: buf[:n]}); sendErr != nil {
				return fmt.Errorf("send frame of chunk %s: %w", req.GetChunkId(), sendErr)
			}
			sent += int64(n)
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Error("read chunk file", "error", err)
			return status.Errorf(codes.Internal, "read chunk file: %v", err)
		}
	}
	log.Info("chunk read", "bytes", sent)
	return nil
}
