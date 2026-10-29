package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

const (
	maxChunkSize = 4 << 20
	maxFrameSize = 1 << 20
)

// put uploads a file of at most one chunk: initiate, stream the chunk to the
// chosen node, then complete with the node's acknowledgement.
func put(ctx context.Context, log *slog.Logger, coordAddr, path, key string) error {
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
	if size > maxChunkSize {
		return fmt.Errorf("%s is %d bytes; stage 1 supports at most %d", path, size, maxChunkSize)
	}

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
	chunk := initResp.GetChunk()
	if len(chunk.GetNodes()) == 0 {
		return fmt.Errorf("initiate upload of %q: coordinator returned no nodes", key)
	}
	node := chunk.GetNodes()[0]
	log = log.With("object_id", objectID, "chunk_id", chunk.GetChunkId(), "node_id", node.GetNodeId())

	written, err := writeChunk(ctx, node.GetAddress(), objectID, chunk.GetChunkId(), f, size)
	if err != nil {
		return fmt.Errorf("write chunk %s to %s: %w", chunk.GetChunkId(), node.GetNodeId(), err)
	}
	log.Info("chunk written", "bytes", written)

	_, err = coord.CompleteUpload(ctx, &objectstorev1.CompleteUploadRequest{
		ObjectId:  objectID,
		Key:       key,
		SizeBytes: size,
		Chunks: []*objectstorev1.CompletedChunk{{
			ChunkId:      chunk.GetChunkId(),
			Index:        0,
			SizeBytes:    size,
			AckedNodeIds: []string{node.GetNodeId()},
		}},
	})
	if err != nil {
		return fmt.Errorf("complete upload of %q: %w", key, err)
	}
	log.Info("upload complete", "key", key, "bytes", size)
	return nil
}

// writeChunk streams size bytes from r to the node as one header frame
// followed by data frames of at most maxFrameSize.
func writeChunk(ctx context.Context, addr, objectID, chunkID string, r io.Reader, size int64) (int64, error) {
	conn, err := dial(addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()

	stream, err := objectstorev1.NewStorageNodeServiceClient(conn).WriteChunk(ctx)
	if err != nil {
		return 0, fmt.Errorf("open stream: %w", err)
	}
	err = stream.Send(&objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Header{
		Header: &objectstorev1.WriteChunkHeader{ChunkId: chunkID, ObjectId: objectID, SizeBytes: size},
	}})
	if err != nil {
		return 0, closeWithStatus(stream, fmt.Errorf("send header: %w", err))
	}

	for {
		// A fresh buffer per frame: gRPC forbids modifying a message after Send.
		buf := make([]byte, maxFrameSize)
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			err := stream.Send(&objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Data{Data: buf[:n]}})
			if err != nil {
				return 0, closeWithStatus(stream, fmt.Errorf("send frame: %w", err))
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("read source: %w", readErr)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return 0, err
	}
	if resp.GetBytesWritten() != size {
		return 0, fmt.Errorf("node wrote %d bytes, expected %d", resp.GetBytesWritten(), size)
	}
	return resp.GetBytesWritten(), nil
}

// closeWithStatus is called when Send fails. Send returns io.EOF when the
// server has already ended the RPC; the server's actual error is only
// available from CloseAndRecv.
func closeWithStatus(stream objectstorev1.StorageNodeService_WriteChunkClient, sendErr error) error {
	if !errors.Is(sendErr, io.EOF) {
		return sendErr
	}
	if _, err := stream.CloseAndRecv(); err != nil {
		return err
	}
	return sendErr
}
