package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// BlockHandler is invoked for each block during iteration. Returning an error stops iteration.
type BlockHandler func(blk block.Block) error

// IterateArchiveBlocks streams blocks in [from, to] inclusive without loading the
// full range into memory. Verifies stored hashes and prev-hash linkage.
func IterateArchiveBlocks(ctx context.Context, root string, from, to uint64, fn BlockHandler) error {
	if to < from {
		return fmt.Errorf("invalid range %d-%d", from, to)
	}
	if fn == nil {
		return fmt.Errorf("nil block handler")
	}
	s, err := NewArchive(ArchiveOptions{Root: root})
	if err != nil {
		return err
	}
	defer s.Close()

	cp, ok, err := s.LastProcessedRound(ctx)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("archive has no checkpoint")
	}
	if to > cp {
		return fmt.Errorf("end round %d past checkpoint %d", to, cp)
	}

	var prevHash string
	havePrev := false
	if from > 0 {
		if h, ok, err := s.BlockHash(ctx, from-1); err == nil && ok {
			prevHash = h
			havePrev = true
		}
	}

	segDir := filepath.Join(root, segmentsDirName)
	entries, err := os.ReadDir(segDir)
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".seg") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	var got uint64
	for _, name := range names {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		path := filepath.Join(segDir, name)
		err := forEachSegmentBlock(path, from, to, func(blk block.Block) error {
			if havePrev {
				if err := block.ValidateLinkage(prevHash, blk); err != nil {
					return fmt.Errorf("round %d: %w", blk.Round, err)
				}
			}
			if blk.BlockHash == "" {
				return fmt.Errorf("round %d: empty hash", blk.Round)
			}
			if err := fn(blk); err != nil {
				return err
			}
			prevHash = blk.BlockHash
			havePrev = true
			got++
			return nil
		})
		if err != nil {
			return err
		}
	}
	want := to - from + 1
	if got != want {
		return fmt.Errorf("archive incomplete: got %d blocks want %d", got, want)
	}
	return nil
}

func forEachSegmentBlock(path string, from, to uint64, fn BlockHandler) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := readSegmentHeader(f); err != nil {
		return err
	}
	off := int64(archiveHeaderSize)
	for {
		rec, n, err := readRecordAt(f, off)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		off += n
		if rec.Round < from {
			continue
		}
		if rec.Round > to {
			return nil
		}
		blk := block.Block{
			Round:             rec.Round,
			BlockHash:         rec.Hash,
			PreviousBlockHash: rec.PrevHash,
			Raw:               append([]byte(nil), rec.Raw...),
		}
		if decoded, err := block.DecodeRaw(rec.Raw); err == nil {
			if decoded.Round != rec.Round {
				return fmt.Errorf("round mismatch stored=%d decoded=%d", rec.Round, decoded.Round)
			}
			if decoded.BlockHash != rec.Hash {
				return fmt.Errorf("hash mismatch round %d", rec.Round)
			}
			if decoded.PreviousBlockHash != rec.PrevHash {
				return fmt.Errorf("prev hash mismatch round %d", rec.Round)
			}
			blk = decoded
		}
		if err := fn(blk); err != nil {
			return err
		}
	}
}
