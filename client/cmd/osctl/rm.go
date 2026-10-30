package main

import (
	"context"
	"fmt"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// rm removes a key. The key stops resolving at once; the object's chunk files
// are deleted later by the coordinator's garbage collection, after its read
// grace. A key that doesn't resolve is an error.
func rm(ctx context.Context, log *slog.Logger, coordAddr, key string) error {
	conn, err := dial(coordAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := objectstorev1.NewCoordinatorServiceClient(conn).DeleteObject(ctx, &objectstorev1.DeleteObjectRequest{Key: key})
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("rm %q: key not found", key)
	}
	if err != nil {
		return fmt.Errorf("rm %q: %w", key, err)
	}
	log.Info("key removed; files are deleted after the grace period", "key", key, "object_id", resp.GetObjectId())
	return nil
}
