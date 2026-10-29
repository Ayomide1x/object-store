// Package chunkwrite writes one chunk to one storage node: a header frame,
// data frames of at most 1 MiB, then a trailer with the SHA-256 of the bytes
// sent, computed while sending.
//
// It lives at the module root so both the client (fan-out writes) and, from
// stage 7, the node (node-to-node copies) can use it.
package chunkwrite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// MaxFrameSize is the largest data frame sent on a stream.
const MaxFrameSize = 1 << 20

// Header identifies the chunk being written.
type Header struct {
	ChunkID  string
	ObjectID string // for the node's logs only
	Size     int64
}

// Write streams h.Size bytes from r to the node at addr and returns the
// SHA-256 it sent, after checking the node acknowledged the same size and
// hash. ctx bounds the whole write, connection included.
func Write(ctx context.Context, addr string, h Header, r io.Reader) ([]byte, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	stream, err := objectstorev1.NewStorageNodeServiceClient(conn).WriteChunk(ctx)
	if err != nil {
		return nil, fmt.Errorf("open stream: %w", err)
	}
	err = stream.Send(&objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Header{
		Header: &objectstorev1.WriteChunkHeader{ChunkId: h.ChunkID, ObjectId: h.ObjectID, SizeBytes: h.Size},
	}})
	if err != nil {
		return nil, closeWithStatus(stream, fmt.Errorf("send header: %w", err))
	}

	// Everything read from r also goes through the hasher.
	hasher := sha256.New()
	r = io.TeeReader(r, hasher)
	var sent int64
	for {
		// A fresh buffer per frame: gRPC forbids modifying a message after Send.
		buf := make([]byte, MaxFrameSize)
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			err := stream.Send(&objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Data{Data: buf[:n]}})
			if err != nil {
				return nil, closeWithStatus(stream, fmt.Errorf("send frame: %w", err))
			}
			sent += int64(n)
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read source: %w", readErr)
		}
	}
	// A source that shrank yields fewer bytes than planned. The node would
	// reject the chunk anyway; failing here gives the clearer error.
	if sent != h.Size {
		_, _ = stream.CloseAndRecv()
		return nil, fmt.Errorf("source ended after %d of %d bytes (file changed during upload?)", sent, h.Size)
	}

	sum := hasher.Sum(nil)
	err = stream.Send(&objectstorev1.WriteChunkRequest{Frame: &objectstorev1.WriteChunkRequest_Trailer{
		Trailer: &objectstorev1.WriteChunkTrailer{Sha256: sum},
	}})
	if err != nil {
		return nil, closeWithStatus(stream, fmt.Errorf("send trailer: %w", err))
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	if resp.GetBytesWritten() != h.Size {
		return nil, fmt.Errorf("node wrote %d bytes, expected %d", resp.GetBytesWritten(), h.Size)
	}
	// A node that verified a different hash (or none: an older node) has not
	// acknowledged these bytes.
	if !bytes.Equal(resp.GetSha256(), sum) {
		return nil, fmt.Errorf("node verified sha256 %x, sent %x", resp.GetSha256(), sum)
	}
	return sum, nil
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
