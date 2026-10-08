package storage

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// ArchiveVerifyReport summarizes an independent archive verification.
type ArchiveVerifyReport struct {
	Root           string
	FirstRound     uint64
	LastRound      uint64
	Blocks         uint64
	Segments       int
	Gaps           int
	HashErrors     int
	CorruptRecords int
	Checkpoint     uint64
	HasCheckpoint  bool
	SegmentSize    uint64
	Version        uint32
	Bytes          int64
	OK             bool
	Errors         []string
}

// ArchiveInfoReport is lightweight metadata about an archive root.
type ArchiveInfoReport struct {
	Root          string
	FirstRound    uint64
	LastRound     uint64
	HasData       bool
	Checkpoint    uint64
	HasCheckpoint bool
	Segments      int
	Blocks        uint64
	Bytes         int64
	SegmentSize   uint64
	Version       uint32
}

// VerifyArchive inspects archive segments without contacting Voi.
// Optional from/to bound the verified range (0 = unbounded on that side).
func VerifyArchive(root string, from, to uint64) (*ArchiveVerifyReport, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return nil, fmt.Errorf("archive root required")
	}
	rep := &ArchiveVerifyReport{Root: root, Version: archiveVersion, SegmentSize: defaultSegmentSize}
	if st, err := os.Stat(root); err != nil {
		return nil, fmt.Errorf("archive root: %w", err)
	} else if !st.IsDir() {
		return nil, fmt.Errorf("archive root is not a directory: %s", root)
	}

	// Read checkpoint without reconcile so trailers past the durable cursor remain visible.
	cp, ok, err := readCheckpointFile(filepath.Join(root, checkpointFileName))
	if err != nil {
		return nil, err
	}
	rep.HasCheckpoint = ok
	rep.Checkpoint = cp

	segDir := filepath.Join(root, segmentsDirName)
	if _, err := os.Stat(segDir); os.IsNotExist(err) {
		rep.OK = true
		return rep, nil
	}
	entries, err := os.ReadDir(segDir)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".seg") {
			paths = append(paths, filepath.Join(segDir, e.Name()))
		}
	}
	sort.Strings(paths)
	rep.Segments = len(paths)

	var (
		expectRound uint64
		haveExpect  bool
		prevHash    string
		havePrev    bool
		firstSet    bool
	)

	for _, path := range paths {
		info, err := os.Stat(path)
		if err == nil {
			rep.Bytes += info.Size()
		}
		f, err := os.Open(path)
		if err != nil {
			rep.Errors = append(rep.Errors, err.Error())
			rep.CorruptRecords++
			continue
		}
		start, err := readSegmentHeader(f)
		if err != nil {
			rep.Errors = append(rep.Errors, fmt.Sprintf("%s: header: %v", filepath.Base(path), err))
			rep.CorruptRecords++
			_ = f.Close()
			continue
		}
		_ = start
		off := int64(archiveHeaderSize)
		for {
			rec, n, err := readRecordAt(f, off)
			if err == io.EOF {
				break
			}
			if err != nil {
				rep.CorruptRecords++
				rep.Errors = append(rep.Errors, fmt.Sprintf("%s: truncated/corrupt at offset %d", filepath.Base(path), off))
				break
			}
			off += n

			if from > 0 && rec.Round < from {
				continue
			}
			if to > 0 && rec.Round > to {
				break
			}
			// Only count rounds within durable checkpoint when present.
			if ok && rec.Round > cp {
				continue
			}

			if !firstSet {
				rep.FirstRound = rec.Round
				firstSet = true
				expectRound = rec.Round
				haveExpect = true
			}
			if haveExpect && rec.Round != expectRound {
				rep.Gaps++
				rep.Errors = append(rep.Errors, fmt.Sprintf("gap: expected %d got %d", expectRound, rec.Round))
			}
			expectRound = rec.Round + 1
			haveExpect = true
			rep.LastRound = rec.Round
			rep.Blocks++

			if havePrev {
				if err := block.ValidateLinkage(prevHash, block.Block{
					Round:             rec.Round,
					PreviousBlockHash: rec.PrevHash,
				}); err != nil {
					rep.HashErrors++
					rep.Errors = append(rep.Errors, err.Error())
				}
			}
			if decoded, err := block.DecodeRaw(rec.Raw); err == nil {
				if decoded.BlockHash != rec.Hash || decoded.PreviousBlockHash != rec.PrevHash || decoded.Round != rec.Round {
					rep.HashErrors++
					rep.Errors = append(rep.Errors, fmt.Sprintf("decode/hash mismatch round %d", rec.Round))
				}
			}
			prevHash = rec.Hash
			havePrev = true
		}
		_ = f.Close()
	}

	if ok && firstSet && to == 0 && from == 0 {
		if rep.LastRound != cp {
			rep.Errors = append(rep.Errors, fmt.Sprintf("checkpoint %d != last durable record %d", cp, rep.LastRound))
		}
	}
	if from > 0 && to >= from && firstSet {
		want := to - from + 1
		if ok && to > cp {
			want = cp - from + 1
		}
		if rep.Blocks != want && ok {
			// soft: report if incomplete in range
			if rep.Blocks < want {
				rep.Gaps++
				rep.Errors = append(rep.Errors, fmt.Sprintf("incomplete range: blocks=%d want=%d", rep.Blocks, want))
			}
		}
	}

	rep.OK = rep.Gaps == 0 && rep.HashErrors == 0 && rep.CorruptRecords == 0 && len(rep.Errors) == 0
	if !ok && rep.Blocks == 0 {
		rep.OK = true // empty archive is valid
	}
	return rep, nil
}

// InspectArchive returns archive metadata without full hash verification.
func InspectArchive(root string) (*ArchiveInfoReport, error) {
	v, err := VerifyArchive(root, 0, 0)
	if err != nil {
		return nil, err
	}
	return &ArchiveInfoReport{
		Root:          v.Root,
		FirstRound:    v.FirstRound,
		LastRound:     v.LastRound,
		HasData:       v.Blocks > 0,
		Checkpoint:    v.Checkpoint,
		HasCheckpoint: v.HasCheckpoint,
		Segments:      v.Segments,
		Blocks:        v.Blocks,
		Bytes:         v.Bytes,
		SegmentSize:   v.SegmentSize,
		Version:       v.Version,
	}, nil
}

// FormatVerifySummary renders the concise verify CLI output.
func FormatVerifySummary(r *ArchiveVerifyReport) string {
	status := "OK"
	if !r.OK {
		status = "FAIL"
	}
	cp := "none"
	if r.HasCheckpoint {
		cp = fmt.Sprintf("%d", r.Checkpoint)
	}
	rounds := "empty"
	if r.Blocks > 0 {
		rounds = fmt.Sprintf("%d–%d", r.FirstRound, r.LastRound)
	}
	return fmt.Sprintf(`Archive: %s
Rounds: %s
Blocks: %d
Segments: %d
Gaps: %d
Hash errors: %d
Corrupt records: %d
Checkpoint: %s
Status: %s
`, r.Root, rounds, r.Blocks, r.Segments, r.Gaps, r.HashErrors, r.CorruptRecords, cp, status)
}
