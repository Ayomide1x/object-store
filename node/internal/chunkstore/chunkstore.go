// Package chunkstore keeps chunk files on local disk.
//
// Layout under the data directory (one filesystem, so link() works):
//
//	chunks/<chunk_id>      final, immutable chunk files
//	tmp/                   in-progress writes, emptied at startup
//	tombstones/<chunk_id>  empty files: chunk IDs deleted for good, never
//	                       written again
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
//
// Deleting a chunk removes its final name; it never modifies the file, so a
// read that already opened it finishes. A deletion with a tombstone also
// makes the ID unwritable, so a late write can't leave a file nobody tracks.
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

	magic         = "OSCHUNK1"
	tempPrefix    = "chunk-"
	idOffset      = 8
	hashOffset    = 24
	idLength      = 16
	hashLength    = sha256.Size
	chunksDir     = "chunks"
	tempDirName   = "tmp"
	tombstonesDir = "tombstones"
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
	// ErrDeleted is returned when writing a chunk ID that has a tombstone.
	ErrDeleted = errors.New("chunk deleted")
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
	chunks     string
	tmp        string
	tombstones string
	ops        ops
}

// New creates the chunk and temp directories under dataDir if needed.
func New(dataDir string) (*Store, error) {
	s := &Store{
		chunks:     filepath.Join(dataDir, chunksDir),
		tmp:        filepath.Join(dataDir, tempDirName),
		tombstones: filepath.Join(dataDir, tombstonesDir),
		ops:        defaultOps(),
	}
	for _, dir := range []string{s.chunks, s.tmp, s.tombstones} {
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

// NewWriter starts a write of chunkID into a new temp file. A chunk ID with a
// tombstone is refused with ErrDeleted. That is only a fast path: Commit
// checks again after the final link, which is what closes the race with a
// concurrent Delete.
func (s *Store) NewWriter(ctx context.Context, chunkID string) (*Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := s.path(chunkID); err != nil {
		return nil, err
	}
	deleted, err := s.tombstoned(chunkID)
	if err != nil {
		return nil, err
	}
	if deleted {
		return nil, fmt.Errorf("write chunk %s: %w", chunkID, ErrDeleted)
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
//  5. if the chunk ID now has a tombstone, a Delete ran concurrently: remove
//     the new final name, fsync the directory, and fail with ErrDeleted
//  6. unlink the temp name
//
// On any error the temp file is removed and nothing exists under the final
// name that this call created, except after a failure to fsync the directory
// in step 4 (the file is then valid and stays).
//
// Step 5 is what makes a delete and a write that interleave always end with
// no file. Delete makes its tombstone durable before it unlinks. So either
// this check sees the tombstone and removes the file, or the tombstone came
// after this check, and so after the link, and Delete's unlink removes it.
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
	deleted, err := w.store.tombstoned(w.chunkID)
	if err != nil {
		return err
	}
	if deleted {
		if err := w.store.ops.remove(final); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove chunk %s written after its deletion: %w", w.chunkID, err)
		}
		if err := w.store.ops.syncDir(w.store.chunks); err != nil {
			return err
		}
		return fmt.Errorf("chunk %s was deleted during the write: %w", w.chunkID, ErrDeleted)
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

// Stat reports the data size and stored SHA-256 of a chunk, reading only its
// header. A chunk with a tombstone is ErrNotFound, even in the moment between
// a racing write's link and its removal. A file exists under a final name only after its hash was verified
// and it was fsynced (see Commit), so a valid header naming chunkID is proof
// of a durable, verified write. Data corruption since then is caught by Read.
// Returns ErrNotFound if there is no file, ErrCorrupt if the header is invalid
// or names another chunk.
func (s *Store) Stat(ctx context.Context, chunkID string) (size int64, sum []byte, err error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	path, err := s.path(chunkID)
	if err != nil {
		return 0, nil, err
	}
	idBytes, err := uuidBytes(chunkID)
	if err != nil {
		return 0, nil, err
	}
	deleted, err := s.tombstoned(chunkID)
	if err != nil {
		return 0, nil, err
	}
	if deleted {
		return 0, nil, fmt.Errorf("stat chunk %s: tombstoned: %w", chunkID, ErrNotFound)
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil, fmt.Errorf("stat chunk %s: %w", chunkID, ErrNotFound)
	}
	if err != nil {
		return 0, nil, fmt.Errorf("stat chunk %s: %w", chunkID, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return 0, nil, fmt.Errorf("stat chunk %s: %w", chunkID, err)
	}
	if info.Size() < HeaderSize || info.Size() > HeaderSize+MaxChunkSize {
		return 0, nil, fmt.Errorf("chunk %s: file is %d bytes: %w", chunkID, info.Size(), ErrCorrupt)
	}
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		return 0, nil, fmt.Errorf("read header of chunk %s: %w", chunkID, err)
	}
	if string(header[:idOffset]) != magic {
		return 0, nil, fmt.Errorf("chunk %s: bad magic %q: %w", chunkID, header[:idOffset], ErrCorrupt)
	}
	if !bytes.Equal(header[idOffset:hashOffset], idBytes) {
		return 0, nil, fmt.Errorf("chunk %s: header names chunk %x: %w", chunkID, header[idOffset:hashOffset], ErrCorrupt)
	}
	return info.Size() - HeaderSize, header[hashOffset:HeaderSize], nil
}

// Deletion is one chunk to remove.
type Deletion struct {
	ChunkID string
	// Tombstone makes the chunk ID unwritable from now on.
	Tombstone bool
}

// Delete removes the final file of each chunk and reports, per chunk, whether
// a file was there (false means it was already absent, which is not an
// error). Every ID is validated before anything changes. In order:
//
//  1. create the tombstones that were asked for, then fsync the tombstones
//     directory, so they are durable before any file goes
//  2. unlink each chunk's final name
//  3. fsync the chunks directory, so the removals survive a crash
//
// A file is never modified, only unlinked: a reader that already opened it
// keeps reading the same bytes until it closes it.
func (s *Store) Delete(ctx context.Context, deletions []Deletion) ([]bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for _, d := range deletions {
		if _, err := s.path(d.ChunkID); err != nil {
			return nil, err
		}
	}

	tombstoned := false
	for _, d := range deletions {
		if !d.Tombstone {
			continue
		}
		f, err := os.OpenFile(filepath.Join(s.tombstones, d.ChunkID), os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("create tombstone for chunk %s: %w", d.ChunkID, err)
		}
		if err := f.Close(); err != nil {
			return nil, fmt.Errorf("close tombstone for chunk %s: %w", d.ChunkID, err)
		}
		tombstoned = true
	}
	if tombstoned {
		if err := s.ops.syncDir(s.tombstones); err != nil {
			return nil, err
		}
	}

	removed := make([]bool, len(deletions))
	for i, d := range deletions {
		path, _ := s.path(d.ChunkID) // validated above
		err := s.ops.remove(path)
		switch {
		case err == nil:
			removed[i] = true
		case errors.Is(err, os.ErrNotExist):
			// Already gone: deleting is idempotent.
		default:
			return nil, fmt.Errorf("remove chunk %s: %w", d.ChunkID, err)
		}
	}
	if err := s.ops.syncDir(s.chunks); err != nil {
		return nil, err
	}
	return removed, nil
}

// SweepTombstoned removes every final chunk file that has a tombstone and
// returns their chunk IDs. It must run before the node accepts writes, like
// CleanTemps: such a file can exist only if the process died between a racing
// write's link and its tombstone check (Commit step 5), or between a Delete's
// tombstone and its unlink.
func (s *Store) SweepTombstoned(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(s.tombstones)
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", s.tombstones, err)
	}
	var removed []string
	for _, e := range entries {
		path, err := s.path(e.Name())
		if err != nil {
			continue // not a tombstone this store wrote
		}
		err = s.ops.remove(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return removed, fmt.Errorf("remove tombstoned chunk %s: %w", e.Name(), err)
		}
		removed = append(removed, e.Name())
	}
	if len(removed) > 0 {
		if err := s.ops.syncDir(s.chunks); err != nil {
			return removed, err
		}
	}
	return removed, nil
}

// tombstoned reports whether chunkID has a tombstone.
func (s *Store) tombstoned(chunkID string) (bool, error) {
	_, err := os.Stat(filepath.Join(s.tombstones, chunkID))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check tombstone of chunk %s: %w", chunkID, err)
	}
	return true, nil
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
