package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Ayomide1x/object-store/client/internal/download"
	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

const (
	defaultChunkReadTimeout = 10 * time.Second
	reportTimeout           = 5 * time.Second
)

// get downloads an object into path. Each chunk is read from one healthy
// replica and, on failure, from the next; only verified bytes are written,
// and a failed get leaves no file at path. Replicas that served bad data (or
// none) are reported to the coordinator afterwards, as hints it confirms with
// the node; the get's outcome never depends on a report.
func get(ctx context.Context, log *slog.Logger, coordAddr, key, path string) error {
	timeout, err := durationEnv("OSCTL_CHUNK_READ_TIMEOUT", defaultChunkReadTimeout)
	if err != nil {
		return err
	}
	order, err := readOrder()
	if err != nil {
		return err
	}

	conn, err := dial(coordAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	coord := objectstorev1.NewCoordinatorServiceClient(conn)

	obj, err := coord.GetObject(ctx, &objectstorev1.GetObjectRequest{Key: key})
	if err != nil {
		return fmt.Errorf("get object %q: %w", key, err)
	}
	log = log.With("object_id", obj.GetObjectId())

	var chunks []download.Chunk
	for _, c := range obj.GetChunks() {
		chunk := download.Chunk{ID: c.GetChunkId(), Index: c.GetIndex(), Size: c.GetSizeBytes(), SHA256: c.GetSha256()}
		for _, r := range c.GetReplicas() {
			chunk.Replicas = append(chunk.Replicas,
				download.Replica{NodeID: r.GetNodeId(), Address: r.GetAddress(), Liveness: r.GetLiveness()})
		}
		chunks = append(chunks, chunk)
	}

	d := download.New(timeout, order, log)
	readErr := d.ToFile(ctx, chunks, obj.GetSizeBytes(), path)
	sendReports(coord, log, d.Reports())
	if readErr != nil {
		return fmt.Errorf("get %q: %w", key, readErr)
	}
	log.Info("download complete", "key", key, "bytes", obj.GetSizeBytes(), "chunks", len(chunks))
	return nil
}

// sendReports tells the coordinator about each bad replica seen, and logs its
// verdict. Failures are only logged: a report is a hint, never part of the get.
func sendReports(coord objectstorev1.CoordinatorServiceClient, log *slog.Logger, reports []download.Report) {
	for _, r := range reports {
		ctx, cancel := context.WithTimeout(context.Background(), reportTimeout)
		resp, err := coord.ReportBadReplica(ctx, &objectstorev1.ReportBadReplicaRequest{
			ChunkId: r.ChunkID, NodeId: r.NodeID, Reason: r.Reason, Detail: r.Detail,
		})
		cancel()
		if err != nil {
			log.Warn("bad replica report failed", "chunk_id", r.ChunkID, "node_id", r.NodeID, "error", err)
			continue
		}
		log.Info("bad replica reported", "chunk_id", r.ChunkID, "node_id", r.NodeID,
			"reason", r.Reason.String(), "verdict", resp.GetVerdict().String())
	}
}

// readOrder reads OSCTL_READ_ORDER: "random" (default) shuffles each chunk's
// replicas to spread load; "listed" keeps the coordinator's order (by node ID),
// for scripts that need to know which replica is read first.
func readOrder() (download.Order, error) {
	switch v := os.Getenv("OSCTL_READ_ORDER"); v {
	case "", "random":
		return download.Shuffled, nil
	case "listed":
		return download.Listed, nil
	default:
		return nil, fmt.Errorf("OSCTL_READ_ORDER must be random or listed, got %q", v)
	}
}
