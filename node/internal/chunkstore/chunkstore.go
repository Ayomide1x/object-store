// Package chunkstore keeps chunk files on local disk.
//
// Layout under the data directory (one filesystem, so link() works):
//
//	chunks/<chunk_id>   final, immutable chunk files
//	tmp/                in-progress writes, emptied at startup
//
// A chunk file is self-describing: a 56-byte header, then the data.
//
//	offset  0   8 bytes  magic "OSCHUNK1" (also the format version)
//	offset  8  16 bytes  chunk ID (the UUID's bytes)
//	offset 24  32 bytes  SHA-256 of the data
//	offset 56   data     0 .. MaxChunkSize bytes
//
// A file becomes visible under its final name only after its hash matched and
// it was fsynced, and the final step never replaces an existing chunk.
package chunkstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	// MaxChunkSize is the largest chunk data a node stores.
	MaxChunkSize = 4 << 20
	// HeaderSize is the size of the header at the start of every chunk file.
	HeaderSize = 56

	magic       = "OSCHUNK1"
	tempPrefix  = "chunk-"
	idOffset    = 8
	hashOffset  = 24
	idLength    = 16
	hashLength  = sha256.Size
	chunksDir   = "chunks"
	tempDirName = "tmp"
)

var (
	// ErrExists is returned when a chunk is already stored under its final name.
	ErrExists = errors.New("chunk already exists")
	// ErrNotFound is returned when no chunk file exists.
	ErrNotFound = errors.New("chunk not found")
	// ErrInvalidID is returned when a chunk ID is not a canonical UUID.
	ErrInvalidID = errors.New("invalid chunk id")
	// ErrChecksumMismatch is returned by Commit when the data does not match
	// the expected SHA-256.
	ErrChecksumMismatch = errors.New("checksum mismatch")
	// ErrCorrupt is returned by Read when a stored chunk fails verification.
	ErrCorrupt = errors.New("chunk failed verification")
)

// ops are the filesystem calls that make a write durable and visible. They
// are fields so a test can record the order in which Commit makes them.
type ops struct {
	syncFile func(f *os.File) error
	link     func(oldname, newname string) error
	syncDir  func(path string) error
	remove   func(path string) error
}

func defaultOps() ops {
	return ops{
		syncFile: (*os.File).Sync,
		link:     os.Link,
		syncDir:  syncDir,
		remove:   os.Remove,
	}
}

// Store is a directory of chunk files.
type Store struct {
	chunks string
	tmp    string
	ops    ops
}

// New creates the chunk and temp directories under dataDir if needed.
func New(dataDir string) (*Store, error) {
	s := &Store{
		chunks: filepath.Join(dataDir, chunksDir),
		tmp:    filepath.Join(dataDir, tempDirName),
		ops:    defaultOps(),
	}
	for _, dir := range []string{s.chunks, s.tmp} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create %s: %w", dir, err)
		}
	}
	// Make the new directory entries themselves durable.
	if err := syncDir(dataDir); err != nil {
		return nil, err
	}
	return s, nil
}

// CleanTemps deletes every file in the temp directory and returns their
// names. It must run before the node accepts writes: any temp file present
// then belongs to a write that died with a previous process.
func (s *Store) CleanTemps(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.tmp)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", s.tmp, err)
	}
	var removed []string
	for _, e := range entries {
		if err := os.RemoveAll(filepath.Join(s.tmp, e.Name())); err != nil {
			return removed, fmt.Errorf("remove stray temp %s: %w", e.Name(), err)
		}
		removed = append(removed, e.Name())
	}
	return removed, nil
}

// ChunkIDFromTempName extracts the chunk ID from a temp file name, or returns
// "" if the name has an unexpected form.
func ChunkIDFromTempName(name string) string {
	rest, ok := strings.CutPrefix(name, tempPrefix)
	if !ok || len(rest) < 36 || !isCanonicalUUID(rest[:36]) {
		return ""
	}
	return rest[:36]
}

// Exists reports whether a chunk is stored under its final name. Writers use
// it only to fail fast; Commit's link() is what refuses to replace a chunk.
func (s *Store) Exists(ctx context.Context, chunkID string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	path, err := s.path(chunkID)
	if err != nil {
		return false, err
	}
	_, err = os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat chunk %s: %w", chunkID, err)
	}
	return true, nil
}

// Writer receives one chunk's data into a temp file, hashing it as it goes.
// Exactly one of Commit or Abort must be called.
type Writer struct {
	store   *Store
	chunkID string
	idBytes []byte
	file    *os.File
	hasher  hash.Hash
	written int64
}

// NewWriter starts a write of chunkID into a new temp file.
func (s *Store) NewWriter(ctx context.Context, chunkID string) (*Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := s.path(chunkID); err != nil {
		return nil, err
	}
	idBytes, err := uuidBytes(chunkID)
	if err != nil {
		return nil, err
	}
	f, err := os.CreateTemp(s.tmp, tempPrefix+chunkID+"-*")
	if err != nil {
		return nil, fmt.Errorf("create temp for chunk %s: %w", chunkID, err)
	}
	// Leave room for the header; it is written once the hash is known.
	if _, err := f.Seek(HeaderSize, io.SeekStart); err != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, fmt.Errorf("seek temp for chunk %s: %w", chunkID, err)
	}
	return &Writer{store: s, chunkID: chunkID, idBytes: idBytes, file: f, hasher: sha256.New()}, nil
}

// Write appends data to the chunk and feeds it to the running hash.
func (w *Writer) Write(p []byte) (int, error) {
	if w.written+int64(len(p)) > MaxChunkSize {
		return 0, fmt.Errorf("chunk %s exceeds %d bytes", w.chunkID, MaxChunkSize)
	}
	n, err := w.file.Write(p)
	w.hasher.Write(p[:n])
	w.written += int64(n)
	if err != nil {
		return n, fmt.Errorf("write temp for chunk %s: %w", w.chunkID, err)
	}
	return n, nil
}

// Written is the number of data bytes written so far.
func (w *Writer) Written() int64 { return w.written }

// TempPath is the temp file's path, for tests and logs.
func (w *Writer) TempPath() string { return w.file.Name() }

// Commit makes the chunk durable and visible, in this order:
//
//  1. compare the data's SHA-256 with want (ErrChecksumMismatch if different)
//  2. write the header, fsync the temp file, close it
//  3. link the temp file to the final name; link fails if the name exists, so
//     an existing chunk is never replaced (ErrExists)
//  4. fsync the chunks directory, so the new name survives a crash
//  5. unlink the temp name
//
// On any error the temp file is removed and nothing exists under the final
// name that this call created.
func (w *Writer) Commit(ctx context.Context, want []byte) (err error) {
	defer func() {
		if err != nil {
			w.discard()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	got := w.hasher.Sum(nil)
	if !bytes.Equal(got, want) {
		return fmt.Errorf("chunk %s: data hashes to %x, expected %x: %w", w.chunkID, got, want, ErrChecksumMismatch)
	}

	header := make([]byte, 0, HeaderSize)
	header = append(header, magic...)
	header = append(header, w.idBytes...)
	header = append(header, got...)
	if _, err := w.file.WriteAt(header, 0); err != nil {
		return fmt.Errorf("write header for chunk %s: %w", w.chunkID, err)
	}
	if err := w.store.ops.syncFile(w.file); err != nil {
		return fmt.Errorf("sync chunk %s: %w", w.chunkID, err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close chunk %s: %w", w.chunkID, err)
	}

	final, err := w.store.path(w.chunkID)
	if err != nil {
		return err
	}
	if err := w.store.ops.link(w.file.Name(), final); err != nil {
		if errors.Is(err, os.ErrExist) {
			return fmt.Errorf("link chunk %s: %w", w.chunkID, ErrExists)
		}
		return fmt.Errorf("link chunk %s: %w", w.chunkID, err)
	}
	// The chunk is now complete under its final name. A failure from here on
	// is reported, but the final file stays: it is valid, and removing it
	// could race with a reader.
	if err := w.store.ops.syncDir(w.store.chunks); err != nil {
		return fmt.Errorf("sync chunks dir after chunk %s: %w", w.chunkID, err)
	}
	if err := w.store.ops.remove(w.file.Name()); err != nil {
		return fmt.Errorf("unlink temp of chunk %s: %w", w.chunkID, err)
	}
	return nil
}

// Abort discards the write. It is safe to call after a failed Commit.
func (w *Writer) Abort() { w.discard() }

func (w *Writer) discard() {
	w.file.Close() // may already be closed
	os.Remove(w.file.Name())
}

// Read returns a chunk's data after verifying the whole file: the magic, that
// the stored chunk ID is chunkID, and that the data matches the stored hash.
// Nothing is returned unless all three hold, so a caller never sees
// unverified bytes (invariant 5). The whole chunk is held in memory.
func (s *Store) Read(ctx context.Context, chunkID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	path, err := s.path(chunkID)
	if err != nil {
		return nil, err
	}
	idBytes, err := uuidBytes(chunkID)
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
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat chunk %s: %w", chunkID, err)
	}
	size := info.Size()
	if size < HeaderSize || size > HeaderSize+MaxChunkSize {
		return nil, fmt.Errorf("chunk %s: file is %d bytes: %w", chunkID, size, ErrCorrupt)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(f, buf); err != nil {
		return nil, fmt.Errorf("read chunk %s: %w", chunkID, err)
	}

	if string(buf[:idOffset]) != magic {
		return nil, fmt.Errorf("chunk %s: bad magic %q: %w", chunkID, buf[:idOffset], ErrCorrupt)
	}
	if !bytes.Equal(buf[idOffset:hashOffset], idBytes) {
		return nil, fmt.Errorf("chunk %s: header names chunk %x: %w", chunkID, buf[idOffset:hashOffset], ErrCorrupt)
	}
	data := buf[HeaderSize:]
	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], buf[hashOffset:HeaderSize]) {
		return nil, fmt.Errorf("chunk %s: data hashes to %x, header says %x: %w",
			chunkID, sum, buf[hashOffset:HeaderSize], ErrCorrupt)
	}
	return data, nil
}

// path maps a chunk ID to its final file. Rejecting anything but a canonical
// UUID keeps IDs like "../x" from escaping the directory.
func (s *Store) path(chunkID string) (string, error) {
	if !isCanonicalUUID(chunkID) {
		return "", fmt.Errorf("chunk id %q: %w", chunkID, ErrInvalidID)
	}
	return filepath.Join(s.chunks, chunkID), nil
}

// syncDir fsyncs a directory, making changes to its entries durable.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open dir %s: %w", path, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("sync dir %s: %w", path, err)
	}
	return nil
}

// uuidBytes returns the 16 bytes of a canonical UUID string.
func uuidBytes(chunkID string) ([]byte, error) {
	if !isCanonicalUUID(chunkID) {
		return nil, fmt.Errorf("chunk id %q: %w", chunkID, ErrInvalidID)
	}
	b, err := hex.DecodeString(strings.ReplaceAll(chunkID, "-", ""))
	if err != nil || len(b) != idLength {
		return nil, fmt.Errorf("chunk id %q: %w", chunkID, ErrInvalidID)
	}
	return b, nil
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
