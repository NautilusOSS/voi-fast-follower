package storage

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// ExportArchive copies durable blocks in [from, to] into destRoot as a self-contained
// archive (segments + checkpoint). Dest must be empty or nonexistent.
func ExportArchive(ctx context.Context, srcRoot, destRoot string, from, to uint64) error {
	if to < from {
		return fmt.Errorf("invalid range %d-%d", from, to)
	}
	srcRoot = strings.TrimSpace(srcRoot)
	destRoot = strings.TrimSpace(destRoot)
	if srcRoot == "" || destRoot == "" {
		return fmt.Errorf("src and dest archive roots required")
	}

	src, err := NewArchive(ArchiveOptions{Root: srcRoot})
	if err != nil {
		return err
	}
	cp, ok, err := src.LastProcessedRound(ctx)
	if err != nil {
		_ = src.Close()
		return err
	}
	segSize := int(src.segmentSize)
	syncOff := !src.syncWrites
	_ = src.Close()
	if !ok {
		return fmt.Errorf("source archive has no checkpoint")
	}
	if to > cp {
		return fmt.Errorf("end round %d past checkpoint %d", to, cp)
	}

	if err := ensureEmptyArchiveDest(destRoot); err != nil {
		return err
	}
	dest, err := NewArchive(ArchiveOptions{
		Root:        destRoot,
		SegmentSize: segSize,
		DisableSync: syncOff,
	})
	if err != nil {
		return err
	}
	defer dest.Close()

	const batchN = 100
	buf := make([]block.Block, 0, batchN)
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		if err := dest.CommitBatch(ctx, buf); err != nil {
			return err
		}
		buf = buf[:0]
		return nil
	}

	err = IterateArchiveBlocks(ctx, srcRoot, from, to, func(blk block.Block) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		buf = append(buf, blk)
		if len(buf) >= batchN {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}

	rep, err := VerifyArchive(destRoot, from, to)
	if err != nil {
		return fmt.Errorf("verify export: %w", err)
	}
	if !rep.OK {
		return fmt.Errorf("export verification failed: %v", rep.Errors)
	}
	return nil
}

// ImportArchive copies a source archive into destRoot.
// Dest must be empty; import verifies the source before copying bytes.
func ImportArchive(ctx context.Context, srcRoot, destRoot string) error {
	srcRoot = strings.TrimSpace(srcRoot)
	destRoot = strings.TrimSpace(destRoot)
	if srcRoot == "" || destRoot == "" {
		return fmt.Errorf("src and dest archive roots required")
	}
	info, err := InspectArchive(srcRoot)
	if err != nil {
		return err
	}
	if !info.HasCheckpoint || !info.HasData {
		return fmt.Errorf("source archive has no durable data")
	}
	rep, err := VerifyArchive(srcRoot, info.FirstRound, info.Checkpoint)
	if err != nil {
		return err
	}
	if !rep.OK {
		return fmt.Errorf("source archive failed verification: %v", rep.Errors)
	}
	if err := ensureEmptyArchiveDest(destRoot); err != nil {
		return err
	}
	return ExportArchive(ctx, srcRoot, destRoot, info.FirstRound, info.Checkpoint)
}

// PruneArchive removes durable history below keepFrom (inclusive keep).
// Checkpoint tip is unchanged. Rounds < keepFrom become unavailable.
func PruneArchive(ctx context.Context, root string, keepFrom uint64) error {
	root = strings.TrimSpace(root)
	if root == "" {
		return fmt.Errorf("archive root required")
	}
	if keepFrom == 0 {
		return fmt.Errorf("keepFrom must be >= 1")
	}
	info, err := InspectArchive(root)
	if err != nil {
		return err
	}
	if !info.HasCheckpoint {
		return fmt.Errorf("archive has no checkpoint")
	}
	if keepFrom > info.Checkpoint {
		return fmt.Errorf("keepFrom %d past checkpoint %d", keepFrom, info.Checkpoint)
	}
	if info.HasData && keepFrom <= info.FirstRound {
		return nil // already within window
	}

	tmp := root + ".prune-tmp"
	_ = os.RemoveAll(tmp)
	if err := ExportArchive(ctx, root, tmp, keepFrom, info.Checkpoint); err != nil {
		_ = os.RemoveAll(tmp)
		return err
	}

	bak := root + ".prune-bak"
	_ = os.RemoveAll(bak)
	if err := os.Rename(root, bak); err != nil {
		_ = os.RemoveAll(tmp)
		return err
	}
	if err := os.Rename(tmp, root); err != nil {
		_ = os.Rename(bak, root)
		_ = os.RemoveAll(tmp)
		return err
	}
	_ = os.RemoveAll(bak)
	return nil
}

func ensureEmptyArchiveDest(root string) error {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	for _, e := range entries {
		// Allow empty dir only.
		return fmt.Errorf("destination archive %s is not empty (%s present); refuse overwrite", root, e.Name())
	}
	return nil
}

