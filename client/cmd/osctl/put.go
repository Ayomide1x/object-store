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

const maxFrameSize = 1 << 20

// put uploads a file: initiate (the coordinator returns the full chunk plan),
// stream each chunk to its node in order, then complete with the nodes'
// acknowledgements. The file is read one frame at a time, never whole.
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

	// Chunks go up one after another; parallel upload is deferred (see DECISIONS.md).
	var completed []*objectstorev1.CompletedChunk
	var offset int64
	for _, chunk := range initResp.GetChunks() {
		node := chunk.GetNodes()[0]
		clog := log.With("chunk_id", chunk.GetChunkId(), "node_id", node.GetNodeId())

		// A SectionReader reads only this chunk's byte range of the file.
		section := io.NewSectionReader(f, offset, chunk.GetSizeBytes())
		written, err := writeChunk(ctx, node.GetAddress(), objectID, chunk.GetChunkId(), section, chunk.GetSizeBytes())
		if err != nil {
			return fmt.Errorf("write chunk %d (%s) to %s: %w", chunk.GetIndex(), chunk.GetChunkId(), node.GetNodeId(), err)
		}
		clog.Info("chunk written", "index", chunk.GetIndex(), "bytes", written)

		completed = append(completed, &objectstorev1.CompletedChunk{
			ChunkId:      chunk.GetChunkId(),
			Index:        chunk.GetIndex(),
			SizeBytes:    chunk.GetSizeBytes(),
			AckedNodeIds: []string{node.GetNodeId()},
		})
		offset += chunk.GetSizeBytes()
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

	var sent int64
	for {
		// A fresh buffer per frame: gRPC forbids modifying a message after Send.
		buf := make([]byte, maxFrameSize)
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			err := stream.Send(&objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Data{Data: buf[:n]}})
			if err != nil {
				return 0, closeWithStatus(stream, fmt.Errorf("send frame: %w", err))
			}
			sent += int64(n)
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return 0, fmt.Errorf("read source: %w", readErr)
		}
	}
	// A file that shrank since stat yields fewer bytes than planned. The node
	// would reject the chunk anyway; failing here gives the clearer error.
	if sent != size {
		_, _ = stream.CloseAndRecv()
		return 0, fmt.Errorf("source ended after %d of %d bytes (file changed during upload?)", sent, size)
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
