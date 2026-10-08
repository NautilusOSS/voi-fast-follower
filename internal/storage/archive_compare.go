package storage

import (
	"bytes"
	"context"
	"fmt"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// ArchiveCompareReport summarizes a semantic archive comparison.
type ArchiveCompareReport struct {
	Start      uint64
	End        uint64
	Blocks     uint64
	OK         bool
	FirstDiff  uint64
	HasDiff    bool
	DiffReason string
}

// CompareArchives checks that two archives contain the same rounds, hashes,
// previous hashes, and raw payloads over [from, to]. Filesystem metadata is ignored.
func CompareArchives(ctx context.Context, rootA, rootB string, from, to uint64) (*ArchiveCompareReport, error) {
	rep := &ArchiveCompareReport{Start: from, End: to, OK: true}
	if to < from {
		return nil, fmt.Errorf("invalid range %d-%d", from, to)
	}

	type snap struct {
		round uint64
		hash  string
		prev  string
		raw   []byte
	}
	var sideA []snap
	if err := IterateArchiveBlocks(ctx, rootA, from, to, func(blk block.Block) error {
		sideA = append(sideA, snap{
			round: blk.Round,
			hash:  blk.BlockHash,
			prev:  blk.PreviousBlockHash,
			raw:   append([]byte(nil), blk.Raw...),
		})
		return nil
	}); err != nil {
		return nil, fmt.Errorf("archive A: %w", err)
	}

	idx := 0
	stop := fmt.Errorf("compare stop")
	err := IterateArchiveBlocks(ctx, rootB, from, to, func(blk block.Block) error {
		if idx >= len(sideA) {
			rep.OK = false
			rep.HasDiff = true
			rep.FirstDiff = blk.Round
			rep.DiffReason = "B has extra blocks"
			return stop
		}
		a := sideA[idx]
		idx++
		rep.Blocks++
		wantRound := from + uint64(idx) - 1
		switch {
		case a.round != wantRound || blk.Round != wantRound:
			rep.OK, rep.HasDiff, rep.FirstDiff = false, true, wantRound
			rep.DiffReason = fmt.Sprintf("round mismatch A=%d B=%d want=%d", a.round, blk.Round, wantRound)
			return stop
		case a.hash != blk.BlockHash:
			rep.OK, rep.HasDiff, rep.FirstDiff = false, true, blk.Round
			rep.DiffReason = "block hash mismatch"
			return stop
		case a.prev != blk.PreviousBlockHash:
			rep.OK, rep.HasDiff, rep.FirstDiff = false, true, blk.Round
			rep.DiffReason = "previous hash mismatch"
			return stop
		case !bytes.Equal(a.raw, blk.Raw):
			rep.OK, rep.HasDiff, rep.FirstDiff = false, true, blk.Round
			rep.DiffReason = "raw payload mismatch"
			return stop
		}
		return nil
	})
	if err != nil && !rep.HasDiff {
		return nil, fmt.Errorf("archive B: %w", err)
	}
	if rep.OK && idx != len(sideA) {
		rep.OK = false
		rep.HasDiff = true
		rep.DiffReason = "A has extra blocks"
	}
	if rep.OK && rep.Blocks != to-from+1 {
		rep.OK = false
		rep.DiffReason = fmt.Sprintf("block count %d want %d", rep.Blocks, to-from+1)
	}
	return rep, nil
}
