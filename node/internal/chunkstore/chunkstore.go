// Package chunkstore keeps chunk files in a single directory on local disk,
// one file per chunk, named by chunk ID.
package chunkstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ErrExists is returned by Create when a chunk file already exists.
var ErrExists = errors.New("chunk already exists")

// ErrNotFound is returned by Open when no chunk file exists.
var ErrNotFound = errors.New("chunk not found")

// ErrInvalidID is returned when a chunk ID is not a canonical UUID.
var ErrInvalidID = errors.New("invalid chunk id")

// Store is a directory of chunk files.
type Store struct {
	dir string
}

// New creates the chunk directory if needed and returns a Store over it.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create chunk dir %s: %w", dir, err)
	}
	return &Store{dir: dir}, nil
}

// Create opens a new chunk file for writing. It fails with ErrExists if the
// chunk is already present: chunks are never overwritten (invariant 4).
func (s *Store) Create(ctx context.Context, chunkID string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := s.path(chunkID)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("create chunk %s: %w", chunkID, ErrExists)
	}
	if err != nil {
		return nil, fmt.Errorf("create chunk %s: %w", chunkID, err)
	}
	return f, nil
}

// Open opens an existing chunk file for reading.
func (s *Store) Open(ctx context.Context, chunkID string) (*os.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := s.path(chunkID)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("open chunk %s: %w", chunkID, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("open chunk %s: %w", chunkID, err)
	}
	return f, nil
}

// Remove deletes a chunk file. It is used only to discard a write that failed
// before it was acknowledged.
func (s *Store) Remove(ctx context.Context, chunkID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	path, err := s.path(chunkID)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove chunk %s: %w", chunkID, err)
	}
	return nil
}

// path maps a chunk ID to its file. Rejecting anything but a canonical UUID
// keeps IDs like "../x" from escaping the directory.
func (s *Store) path(chunkID string) (string, error) {
	if !isCanonicalUUID(chunkID) {
		return "", fmt.Errorf("chunk id %q: %w", chunkID, ErrInvalidID)
	}
	return filepath.Join(s.dir, chunkID), nil
}

// isCanonicalUUID reports whether s has the lowercase 8-4-4-4-12 hex form.
func isCanonicalUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		switch i {
		case 8, 13, 18, 23:
			if c != '-' {
				return false
			}
		default:
			if !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
				return false
			}
		}
	}
	return true
}
