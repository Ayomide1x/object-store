package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
	"github.com/Ayomide1x/object-store/internal/download"
)

const defaultChunkReadTimeout = 10 * time.Second

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

	obj, err := download.Fetch(ctx, coord, key, objectstorev1.NodeAddressSet_NODE_ADDRESS_SET_CLIENT)
	if err != nil {
		return err
	}
	log = log.With("object_id", obj.ID)

	d := download.New(timeout, order, log)
	readErr := d.ToFile(ctx, obj.Chunks, obj.Size, path)
	download.SendReports(coord, log, d.Reports())
	if readErr != nil {
		return fmt.Errorf("get %q: %w", key, readErr)
	}
	log.Info("download complete", "key", key, "bytes", obj.Size, "chunks", len(obj.Chunks))
	return nil
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
