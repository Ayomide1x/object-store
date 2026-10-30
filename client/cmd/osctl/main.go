// Command osctl is the object store CLI.
//
//	osctl put <file> <key>
//	osctl get <key> <file>
//	osctl rm <key>       the key stops resolving now; its files are deleted
//	                     after the coordinator's read grace
//	osctl ls [prefix]    keys that currently resolve
//	osctl stat <key>     the object and every recorded replica of each chunk
//
// Environment:
//
//	OSCTL_COORDINATOR           coordinator address (default 127.0.0.1:9090)
//	OSCTL_CHUNK_WRITE_TIMEOUT   deadline for each replica write, a Go duration
//	                            (default 10s). A node that misses it, or fails,
//	                            is not tried again for the rest of that upload.
//	OSCTL_CHUNK_READ_TIMEOUT    deadline for each replica read (default 10s). A
//	                            node that misses it, is unreachable or busy is
//	                            tried last for the rest of that get.
//	OSCTL_READ_ORDER            "random" (default): shuffle each chunk's replicas;
//	                            "listed": the coordinator's order, by node ID.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const defaultCoordinator = "127.0.0.1:9090"

const usage = `usage:
  osctl put <file> <key>
  osctl get <key> <file>
  osctl rm <key>
  osctl ls [prefix]
  osctl stat <key>`

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	if err := run(ctx, log, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "osctl:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger, args []string) error {
	if len(args) == 0 {
		return errors.New(usage)
	}
	coordAddr := os.Getenv("OSCTL_COORDINATOR")
	if coordAddr == "" {
		coordAddr = defaultCoordinator
	}

	switch args[0] {
	case "put":
		if len(args) != 3 {
			return errors.New(usage)
		}
		return put(ctx, log, coordAddr, args[1], args[2])
	case "get":
		if len(args) != 3 {
			return errors.New(usage)
		}
		return get(ctx, log, coordAddr, args[1], args[2])
	case "rm":
		if len(args) != 2 {
			return errors.New(usage)
		}
		return rm(ctx, log, coordAddr, args[1])
	case "ls":
		if len(args) > 2 {
			return errors.New(usage)
		}
		prefix := ""
		if len(args) == 2 {
			prefix = args[1]
		}
		return ls(ctx, os.Stdout, coordAddr, prefix)
	case "stat":
		if len(args) != 2 {
			return errors.New(usage)
		}
		return stat(ctx, os.Stdout, coordAddr, args[1])
	default:
		return fmt.Errorf("unknown command %q\n%s", args[0], usage)
	}
}

// dial opens a plaintext gRPC connection. Connections are lazy: errors show up
// on the first RPC, not here.
func dial(addr string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	return conn, nil
}
