package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

const (
	archiveMagic       = "VFFSEG01"
	archiveVersion     = uint32(1)
	archiveHeaderSize  = 20 // magic(8) + version(4) + start_round(8)
	defaultSegmentSize = 1000
	checkpointFileName = "checkpoint"
	segmentsDirName    = "segments"
)

// ArchiveSink appends validated raw blocks to local segment files.
//
// Durability model (when Sync=true, the default):
//
//  1. Append batch records to the appropriate segment file(s)
//  2. Sync each dirty segment (fdatasync / File.Sync)
//  3. Atomically update archive/checkpoint to the batch final round
//     (write temp → Sync → rename → Sync directory when possible)
//
// The checkpoint file is authoritative. On open, any segment bytes beyond the
// checkpoint are truncated so a crash between step 2 and 3 cannot advance
// the durable cursor. Idempotent retries of an already-checkpointed batch
// are no-ops.
//
// Format (little-endian), one segment file per aligned round range:
//
//	archive/segments/{start:020d}.seg
//	Header: magic "VFFSEG01" | version u32 | start_round u64
//	Record: round u64 | hash_len u16 | hash | prev_len u16 | prev | raw_len u32 | raw
type ArchiveSink struct {
	root        string
	segmentSize uint64
	syncWrites  bool

	mu         sync.Mutex
	checkpoint uint64 // 0 = none
	hasCP      bool
	segIndex   []segRef // sorted segment starts for O(log n) file lookup

	bytesWritten uint64
	onBytes      func(n int)
}

// ArchiveOptions configures a new archive sink.
type ArchiveOptions struct {
	Root        string
	SegmentSize int  // rounds per segment file; default 1000
	DisableSync bool // tests only — skips fsync (NOT crash-durable)
}

// NewArchive opens (or creates) an archive directory and reconciles segments
// to the durable checkpoint. Fsync is enabled unless DisableSync is set.
func NewArchive(opts ArchiveOptions) (*ArchiveSink, error) {
	root := strings.TrimSpace(opts.Root)
	if root == "" {
		return nil, fmt.Errorf("archive root required")
	}
	segSize := opts.SegmentSize
	if segSize < 1 {
		segSize = defaultSegmentSize
	}
	if err := os.MkdirAll(filepath.Join(root, segmentsDirName), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir archive: %w", err)
	}
	s := &ArchiveSink{
		root:        root,
		segmentSize: uint64(segSize),
		syncWrites:  !opts.DisableSync,
	}
	if err := s.loadCheckpoint(); err != nil {
		return nil, err
	}
	if err := s.reconcile(); err != nil {
		return nil, err
	}
	if err := s.rebuildIndexLocked(); err != nil {
		return nil, err
	}
	return s, nil
}

// OnBytes registers a callback invoked with bytes appended on each successful batch.
func (s *ArchiveSink) OnBytes(fn func(n int)) { s.onBytes = fn }

// BytesWritten returns total payload bytes appended this process.
func (s *ArchiveSink) BytesWritten() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bytesWritten
}

func (s *ArchiveSink) checkpointPath() string {
	return filepath.Join(s.root, checkpointFileName)
}

func (s *ArchiveSink) segmentsDir() string {
	return filepath.Join(s.root, segmentsDirName)
}

func (s *ArchiveSink) segmentPath(start uint64) string {
	return filepath.Join(s.segmentsDir(), fmt.Sprintf("%020d.seg", start))
}

func (s *ArchiveSink) segmentStart(round uint64) uint64 {
	return (round / s.segmentSize) * s.segmentSize
}

func (s *ArchiveSink) loadCheckpoint() error {
	cp, ok, err := readCheckpointFile(s.checkpointPath())
	if err != nil {
		return err
	}
	s.hasCP = ok
	s.checkpoint = cp
	return nil
}

// refreshCheckpointLocked reloads the durable checkpoint from disk when another
// process (e.g. the follower) appends to the archive. Safe for read-only consumers.
func (s *ArchiveSink) refreshCheckpointLocked() error {
	cp, ok, err := readCheckpointFile(s.checkpointPath())
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if s.hasCP && cp <= s.checkpoint {
		return nil
	}
	s.hasCP = true
	s.checkpoint = cp
	return s.rebuildIndexLocked()
}

func readCheckpointFile(path string) (uint64, bool, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("read checkpoint: %w", err)
	}
	b = []byte(strings.TrimSpace(string(b)))
	if len(b) == 0 {
		return 0, false, nil
	}
	if len(b) == 8 {
		return binary.LittleEndian.Uint64(b), true, nil
	}
	n, err := strconv.ParseUint(string(b), 10, 64)
	if err != nil {
		return 0, false, fmt.Errorf("parse checkpoint: %w", err)
	}
	return n, true, nil
}

func (s *ArchiveSink) writeCheckpoint(round uint64) error {
	path := s.checkpointPath()
	tmp := path + ".tmp"
	content := []byte(strconv.FormatUint(round, 10) + "\n")
	if err := os.WriteFile(tmp, content, 0o644); err != nil {
		return fmt.Errorf("write checkpoint tmp: %w", err)
	}
	if s.syncWrites {
		f, err := os.Open(tmp)
		if err != nil {
			return err
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return fmt.Errorf("sync checkpoint tmp: %w", err)
		}
		_ = f.Close()
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	if s.syncWrites {
		dir, err := os.Open(s.root)
		if err == nil {
			_ = dir.Sync()
			_ = dir.Close()
		}
	}
	s.checkpoint = round
	s.hasCP = true
	return nil
}

// reconcile truncates segment data past the durable checkpoint.
func (s *ArchiveSink) reconcile() error {
	entries, err := os.ReadDir(s.segmentsDir())
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".seg") {
			continue
		}
		path := filepath.Join(s.segmentsDir(), e.Name())
		if err := s.truncateSegmentToCheckpoint(path); err != nil {
			return fmt.Errorf("reconcile %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (s *ArchiveSink) truncateSegmentToCheckpoint(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	start, err := readSegmentHeader(f)
	if err != nil {
		// Corrupt/incomplete header: drop the segment; checkpoint remains authoritative.
		_ = f.Close()
		return os.Remove(path)
	}
	endExclusive := start + s.segmentSize
	if s.hasCP && s.checkpoint < start {
		// Entire segment is past checkpoint — remove.
		_ = f.Close()
		return os.Remove(path)
	}
	keepThrough := endExclusive - 1
	if s.hasCP && s.checkpoint < keepThrough {
		keepThrough = s.checkpoint
	}
	if !s.hasCP {
		// No checkpoint: discard any segment content (not durable yet).
		_ = f.Close()
		return os.Remove(path)
	}

	off := int64(archiveHeaderSize)
	var lastOK int64 = archiveHeaderSize
	for {
		rec, n, err := readRecordAt(f, off)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Truncated/corrupt trailer — keep lastOK.
			break
		}
		if rec.Round > keepThrough {
			break
		}
		off += n
		lastOK = off
		if rec.Round == keepThrough {
			break
		}
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() > lastOK {
		if err := f.Truncate(lastOK); err != nil {
			return err
		}
		if s.syncWrites {
			if err := f.Sync(); err != nil {
				return err
			}
		}
	}
	return nil
}

func readSegmentHeader(r io.Reader) (startRound uint64, err error) {
	hdr := make([]byte, archiveHeaderSize)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return 0, err
	}
	if string(hdr[0:8]) != archiveMagic {
		return 0, fmt.Errorf("bad magic %q", string(hdr[0:8]))
	}
	ver := binary.LittleEndian.Uint32(hdr[8:12])
	if ver != archiveVersion {
		return 0, fmt.Errorf("unsupported version %d", ver)
	}
	return binary.LittleEndian.Uint64(hdr[12:20]), nil
}

type archiveRecord struct {
	Round    uint64
	Hash     string
	PrevHash string
	Raw      []byte
}

func readRecordAt(f *os.File, off int64) (archiveRecord, int64, error) {
	var fixed [8 + 2]byte
	if _, err := f.ReadAt(fixed[:], off); err != nil {
		if err == io.EOF || errors.Is(err, io.ErrUnexpectedEOF) {
			return archiveRecord{}, 0, io.EOF
		}
		return archiveRecord{}, 0, err
	}
	round := binary.LittleEndian.Uint64(fixed[0:8])
	hashLen := binary.LittleEndian.Uint16(fixed[8:10])
	pos := off + 10
	hash := make([]byte, hashLen)
	if _, err := f.ReadAt(hash, pos); err != nil {
		return archiveRecord{}, 0, io.EOF
	}
	pos += int64(hashLen)

	var prevLenBuf [2]byte
	if _, err := f.ReadAt(prevLenBuf[:], pos); err != nil {
		return archiveRecord{}, 0, io.EOF
	}
	prevLen := binary.LittleEndian.Uint16(prevLenBuf[:])
	pos += 2
	prev := make([]byte, prevLen)
	if _, err := f.ReadAt(prev, pos); err != nil {
		return archiveRecord{}, 0, io.EOF
	}
	pos += int64(prevLen)

	var rawLenBuf [4]byte
	if _, err := f.ReadAt(rawLenBuf[:], pos); err != nil {
		return archiveRecord{}, 0, io.EOF
	}
	rawLen := binary.LittleEndian.Uint32(rawLenBuf[:])
	pos += 4
	raw := make([]byte, rawLen)
	if rawLen > 0 {
		if _, err := f.ReadAt(raw, pos); err != nil {
			return archiveRecord{}, 0, io.EOF
		}
	}
	pos += int64(rawLen)
	return archiveRecord{
		Round:    round,
		Hash:     string(hash),
		PrevHash: string(prev),
		Raw:      raw,
	}, pos - off, nil
}

func encodeRecord(blk block.Block) []byte {
	hash := []byte(blk.BlockHash)
	prev := []byte(blk.PreviousBlockHash)
	raw := blk.Raw
	size := 8 + 2 + len(hash) + 2 + len(prev) + 4 + len(raw)
	buf := make([]byte, size)
	binary.LittleEndian.PutUint64(buf[0:8], blk.Round)
	binary.LittleEndian.PutUint16(buf[8:10], uint16(len(hash)))
	o := 10
	copy(buf[o:], hash)
	o += len(hash)
	binary.LittleEndian.PutUint16(buf[o:o+2], uint16(len(prev)))
	o += 2
	copy(buf[o:], prev)
	o += len(prev)
	binary.LittleEndian.PutUint32(buf[o:o+4], uint32(len(raw)))
	o += 4
	copy(buf[o:], raw)
	return buf
}

// Commit persists a single block.
func (s *ArchiveSink) Commit(ctx context.Context, blk block.Block) error {
	return s.CommitBatch(ctx, []block.Block{blk})
}

// CommitBatch appends blocks, syncs, then advances the archive checkpoint.
func (s *ArchiveSink) CommitBatch(ctx context.Context, blocks []block.Block) error {
	if len(blocks) == 0 {
		return nil
	}
	if err := ValidateBatch(blocks); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	final := blocks[len(blocks)-1].Round
	if s.hasCP && final <= s.checkpoint {
		return nil // idempotent
	}

	// Skip rounds already durable.
	startIdx := 0
	if s.hasCP {
		for startIdx < len(blocks) && blocks[startIdx].Round <= s.checkpoint {
			startIdx++
		}
	}
	if startIdx >= len(blocks) {
		return nil
	}
	toWrite := blocks[startIdx:]

	// Contiguity vs checkpoint.
	if s.hasCP {
		if toWrite[0].Round != s.checkpoint+1 {
			return fmt.Errorf("archive gap: checkpoint=%d next=%d", s.checkpoint, toWrite[0].Round)
		}
	}

	dirty := map[string]*os.File{}
	defer func() {
		for _, f := range dirty {
			_ = f.Close()
		}
	}()

	var written int
	for _, blk := range toWrite {
		if blk.BlockHash == "" {
			return fmt.Errorf("archive: empty block hash round %d", blk.Round)
		}
		if len(blk.Raw) == 0 {
			return fmt.Errorf("archive: empty raw bytes round %d", blk.Round)
		}
		segStart := s.segmentStart(blk.Round)
		path := s.segmentPath(segStart)
		f, ok := dirty[path]
		if !ok {
			var err error
			f, err = s.openSegmentForAppend(path, segStart)
			if err != nil {
				return err
			}
			dirty[path] = f
		}
		rec := encodeRecord(blk)
		if _, err := f.Write(rec); err != nil {
			return fmt.Errorf("append round %d: %w", blk.Round, err)
		}
		written += len(rec)
	}

	if s.syncWrites {
		for _, f := range dirty {
			if err := f.Sync(); err != nil {
				return fmt.Errorf("sync segment: %w", err)
			}
		}
	}

	if err := s.writeCheckpoint(final); err != nil {
		return err
	}
	s.bytesWritten += uint64(written)
	if s.onBytes != nil {
		s.onBytes(written)
	}
	_ = s.rebuildIndexLocked()
	return nil
}

func (s *ArchiveSink) openSegmentForAppend(path string, start uint64) (*os.File, error) {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o644)
		if err != nil {
			return nil, err
		}
		hdr := make([]byte, archiveHeaderSize)
		copy(hdr[0:8], archiveMagic)
		binary.LittleEndian.PutUint32(hdr[8:12], archiveVersion)
		binary.LittleEndian.PutUint64(hdr[12:20], start)
		if _, err := f.Write(hdr); err != nil {
			_ = f.Close()
			return nil, err
		}
		return f, nil
	}
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	// Seek to end after verifying header.
	got, err := readSegmentHeader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if got != start {
		_ = f.Close()
		return nil, fmt.Errorf("segment start mismatch path=%s want=%d got=%d", path, start, got)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// LastProcessedRound returns the durable archive checkpoint.
func (s *ArchiveSink) LastProcessedRound(ctx context.Context) (uint64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshCheckpointLocked(); err != nil {
		return 0, false, err
	}
	if !s.hasCP {
		return 0, false, nil
	}
	return s.checkpoint, true, nil
}

// MaxBlockRound returns the checkpoint (archive durability bound).
func (s *ArchiveSink) MaxBlockRound(ctx context.Context) (uint64, bool, error) {
	return s.LastProcessedRound(ctx)
}

// BlockHash returns the hash for a round from segment data.
func (s *ArchiveSink) BlockHash(ctx context.Context, round uint64) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasCP || round > s.checkpoint {
		return "", false, nil
	}
	rec, err := s.readRoundLocked(round)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) {
			return "", false, nil
		}
		return "", false, err
	}
	return rec.Hash, true, nil
}

// GetBlock returns the canonical block for a durable archive round.
// Rounds past the checkpoint are not available (ok=false, err=nil).
func (s *ArchiveSink) GetBlock(ctx context.Context, round uint64) (block.Block, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.hasCP || round > s.checkpoint {
		if err := s.refreshCheckpointLocked(); err != nil {
			return block.Block{}, false, err
		}
	}
	if !s.hasCP || round > s.checkpoint {
		return block.Block{}, false, nil
	}
	rec, err := s.readRoundLocked(round)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) {
			return block.Block{}, false, nil
		}
		return block.Block{}, false, err
	}
	blk := block.Block{
		Round:             rec.Round,
		BlockHash:         rec.Hash,
		PreviousBlockHash: rec.PrevHash,
		Raw:               append([]byte(nil), rec.Raw...),
	}
	if decoded, err := block.DecodeRaw(rec.Raw); err == nil {
		if decoded.Round != rec.Round {
			return block.Block{}, false, fmt.Errorf("round mismatch stored=%d decoded=%d", rec.Round, decoded.Round)
		}
		if decoded.BlockHash != rec.Hash {
			return block.Block{}, false, fmt.Errorf("hash mismatch round %d", rec.Round)
		}
		if decoded.PreviousBlockHash != rec.PrevHash {
			return block.Block{}, false, fmt.Errorf("prev hash mismatch round %d", rec.Round)
		}
		blk = decoded
	}
	return blk, true, nil
}

func (s *ArchiveSink) readRoundLocked(round uint64) (archiveRecord, error) {
	// Index-assisted path (works across SegmentSize mismatches via filename starts).
	if path, ok := s.segmentPathForRoundLocked(round); ok {
		rec, err := readRoundFromSegment(path, round)
		if err == nil {
			return rec, nil
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, io.EOF) && !errors.Is(err, errRoundNotInSegment) {
			return archiveRecord{}, err
		}
	}

	// Fallback: naming based on this process's segmentSize, then full scan.
	path := s.segmentPath(s.segmentStart(round))
	if rec, err := readRoundFromSegment(path, round); err == nil {
		return rec, nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, io.EOF) && !errors.Is(err, errRoundNotInSegment) {
		return archiveRecord{}, err
	}

	if len(s.segIndex) == 0 {
		if err := s.rebuildIndexLocked(); err != nil {
			return archiveRecord{}, err
		}
	}
	for _, ref := range s.segIndex {
		if ref.path == path {
			continue
		}
		rec, err := readRoundFromSegment(ref.path, round)
		if err == nil {
			return rec, nil
		}
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) || errors.Is(err, errRoundNotInSegment) {
			continue
		}
		return archiveRecord{}, err
	}
	return archiveRecord{}, io.EOF
}

var errRoundNotInSegment = errors.New("round not in segment")

func readRoundFromSegment(path string, round uint64) (archiveRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return archiveRecord{}, err
	}
	defer f.Close()
	start, err := readSegmentHeader(f)
	if err != nil {
		return archiveRecord{}, err
	}
	// If header start is after the round, this segment cannot contain it.
	if start > round {
		return archiveRecord{}, errRoundNotInSegment
	}
	off := int64(archiveHeaderSize)
	for {
		rec, n, err := readRecordAt(f, off)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return archiveRecord{}, errRoundNotInSegment
			}
			return archiveRecord{}, err
		}
		if rec.Round == round {
			return rec, nil
		}
		if rec.Round > round {
			return archiveRecord{}, errRoundNotInSegment
		}
		off += n
	}
}

// Close releases resources (no-op; files are closed per batch).
func (s *ArchiveSink) Close() error { return nil }

// Root returns the archive directory.
func (s *ArchiveSink) Root() string { return s.root }

// ReadArchiveBlocks loads blocks in [from, to] inclusive from a durable archive.
// For large ranges prefer IterateArchiveBlocks to avoid holding the full slice.
func ReadArchiveBlocks(root string, from, to uint64) ([]block.Block, error) {
	var out []block.Block
	err := IterateArchiveBlocks(context.Background(), root, from, to, func(blk block.Block) error {
		out = append(out, blk)
		return nil
	})
	return out, err
}
