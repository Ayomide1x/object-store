package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
	"github.com/Ayomide1x/object-store/internal/upload"
)

const defaultChunkWriteTimeout = 10 * time.Second

// put uploads a file through upload.Put. Each replica reads its own view of
// the file, one frame at a time, hashing as it streams: nothing is buffered.
func put(ctx context.Context, log *slog.Logger, coordAddr, path, key string) error {
	timeout, err := durationEnv("OSCTL_CHUNK_WRITE_TIMEOUT", defaultChunkWriteTimeout)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return fmt.Errorf("stat %s: %w", path, err)
	}

	conn, err := dial(coordAddr)
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = upload.Put(ctx, objectstorev1.NewCoordinatorServiceClient(conn), log, key, info.Size(),
		upload.FileSource{File: f}, upload.Options{ChunkWriteTimeout: timeout})
	return err
}

// durationEnv reads a positive Go duration such as "10s" from env, or returns def.
func durationEnv(env string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(env)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration such as 10s, got %q", env, v)
	}
	return d, nil
}
