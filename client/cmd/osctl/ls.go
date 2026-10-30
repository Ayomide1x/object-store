package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	objectstorev1 "github.com/Ayomide1x/object-store/gen/go/objectstore/v1"
)

// ls prints the keys that currently resolve, optionally only those starting
// with prefix: key, size in bytes, commit time (UTC) and object ID.
func ls(ctx context.Context, out io.Writer, coordAddr, prefix string) error {
	conn, err := dial(coordAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := objectstorev1.NewCoordinatorServiceClient(conn).ListObjects(ctx, &objectstorev1.ListObjectsRequest{Prefix: prefix})
	if err != nil {
		return fmt.Errorf("ls %q: %w", prefix, err)
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "KEY\tSIZE\tCOMMITTED\tOBJECT")
	for _, o := range resp.GetObjects() {
		fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n", o.GetKey(), o.GetSizeBytes(), formatMillis(o.GetCommittedAtUnixMillis()), o.GetObjectId())
	}
	return tw.Flush()
}

// stat prints the object a key resolves to and every recorded replica of each
// chunk: node, state, how the row came to exist, and the node's liveness.
func stat(ctx context.Context, out io.Writer, coordAddr, key string) error {
	conn, err := dial(coordAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := objectstorev1.NewCoordinatorServiceClient(conn).StatObject(ctx, &objectstorev1.StatObjectRequest{Key: key})
	if status.Code(err) == codes.NotFound {
		return fmt.Errorf("stat %q: key not found", key)
	}
	if err != nil {
		return fmt.Errorf("stat %q: %w", key, err)
	}
	fmt.Fprintf(out, "key:       %s\nobject:    %s\nsize:      %d\ncommitted: %s\nchunks:    %d\n",
		key, resp.GetObjectId(), resp.GetSizeBytes(), formatMillis(resp.GetCommittedAtUnixMillis()), len(resp.GetChunks()))
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "INDEX\tCHUNK\tSIZE\tSHA256\tNODE\tSTATE\tORIGIN\tLIVENESS\tREASON")
	for _, c := range resp.GetChunks() {
		sum := hex.EncodeToString(c.GetSha256())
		if len(sum) > 12 {
			sum = sum[:12]
		}
		if len(c.GetReplicas()) == 0 {
			fmt.Fprintf(tw, "%d\t%s\t%d\t%s\t-\t-\t-\t-\t\n", c.GetIndex(), c.GetChunkId(), c.GetSizeBytes(), sum)
		}
		for _, r := range c.GetReplicas() {
			fmt.Fprintf(tw, "%d\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n", c.GetIndex(), c.GetChunkId(), c.GetSizeBytes(), sum,
				r.GetNodeId(), strings.TrimPrefix(r.GetState().String(), "STATE_"), r.GetOrigin(),
				strings.TrimPrefix(r.GetLiveness().String(), "NODE_LIVENESS_"), r.GetBadReason())
		}
	}
	return tw.Flush()
}

func formatMillis(ms int64) string {
	if ms == 0 {
		return "-"
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}
