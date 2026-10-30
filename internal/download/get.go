package download

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// reportTimeout bounds each bad-replica report.
const reportTimeout = 5 * time.Second

// Object is what the coordinator says a key resolves to: the object and each
// chunk's healthy replicas.
type Object struct {
	ID     string
	Size   int64
	Chunks []Chunk
}

// Fetch asks the coordinator for the object key resolves to, with node
// addresses from the given set. The error wraps the coordinator's status.
func Fetch(ctx context.Context, coord objectstorev1.CoordinatorServiceClient, key string,
	addresses objectstorev1.NodeAddressSet) (*Object, error) {
	resp, err := coord.GetObject(ctx, &objectstorev1.GetObjectRequest{Key: key, NodeAddresses: addresses})
	if err != nil {
		return nil, fmt.Errorf("get object %q: %w", key, err)
	}
	obj := &Object{ID: resp.GetObjectId(), Size: resp.GetSizeBytes()}
	for _, c := range resp.GetChunks() {
		chunk := Chunk{ID: c.GetChunkId(), Index: c.GetIndex(), Size: c.GetSizeBytes(), SHA256: c.GetSha256()}
		for _, r := range c.GetReplicas() {
			chunk.Replicas = append(chunk.Replicas, Replica{NodeID: r.GetNodeId(), Address: r.GetAddress(), Liveness: r.GetLiveness()})
		}
		obj.Chunks = append(obj.Chunks, chunk)
	}
	return obj, nil
}

// SendReports tells the coordinator about each bad replica seen, and logs its
// verdict. Failures are only logged: a report is a hint, never part of the
// read. Each report has its own deadline and no parent context, so a reader
// that went away still has its reports delivered.
func SendReports(coord objectstorev1.CoordinatorServiceClient, log *slog.Logger, reports []Report) {
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
