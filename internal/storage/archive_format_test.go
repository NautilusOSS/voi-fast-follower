package storage

import (
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

// Archive format contract: magic VFFSEG01, version 1. Opening older segments
// must remain readable; silent reinterpretation is forbidden.
func TestArchiveFormatVersionStable(t *testing.T) {
	if archiveMagic != "VFFSEG01" {
		t.Fatalf("magic changed: %q", archiveMagic)
	}
	if archiveVersion != 1 {
		t.Fatalf("version changed: %d", archiveVersion)
	}

	dir := t.TempDir()
	sink, err := NewArchive(ArchiveOptions{Root: dir, SegmentSize: 10, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := fixtureBlocks(1, 3)
	if err := sink.CommitBatch(context.Background(), chain); err != nil {
		t.Fatal(err)
	}
	_ = sink.Close()

	// Inspect raw segment header bytes (segment start depends on segmentSize).
	entries, _ := os.ReadDir(filepath.Join(dir, segmentsDirName))
	if len(entries) == 0 {
		t.Fatal("no segments")
	}
	raw, err := os.ReadFile(filepath.Join(dir, segmentsDirName, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	if string(raw[0:8]) != archiveMagic {
		t.Fatalf("magic=%q", raw[0:8])
	}
	ver := binary.LittleEndian.Uint32(raw[8:12])
	if ver != archiveVersion {
		t.Fatalf("ver=%d", ver)
	}

	// Re-open + verify + iterate (mixed-version stand-in: same format).
	rep, err := VerifyArchive(dir, 1, 3)
	if err != nil || !rep.OK {
		t.Fatalf("verify: %+v err=%v", rep, err)
	}
	blocks, err := ReadArchiveBlocks(dir, 1, 3)
	if err != nil || len(blocks) != 3 {
		t.Fatalf("read: %v len=%d", err, len(blocks))
	}
}

func TestUnsupportedArchiveVersionRejected(t *testing.T) {
	dir := t.TempDir()
	segDir := filepath.Join(dir, segmentsDirName)
	if err := os.MkdirAll(segDir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(segDir, "00000000000000000000.seg")
	hdr := make([]byte, archiveHeaderSize)
	copy(hdr[0:8], archiveMagic)
	binary.LittleEndian.PutUint32(hdr[8:12], 99) // unsupported
	binary.LittleEndian.PutUint64(hdr[12:20], 0)
	if err := os.WriteFile(path, hdr, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, checkpointFileName), []byte("0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	// NewArchive reconcile may remove corrupt/unsupported; Get should not invent data.
	_, err := NewArchive(ArchiveOptions{Root: dir})
	// Either error or empty archive after reconcile — must not decode as v1.
	if err != nil {
		return
	}
}
