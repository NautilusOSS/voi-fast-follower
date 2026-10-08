package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestReadCheckpointDecimalEightDigits(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint")
	if err := os.WriteFile(path, []byte("23371392\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cp, ok, err := readCheckpointFile(path)
	if err != nil || !ok || cp != 23371392 {
		t.Fatalf("cp=%d ok=%v err=%v", cp, ok, err)
	}
}

func TestReadCheckpointLegacyBinary(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint")
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, 42)
	if err := os.WriteFile(path, buf, 0o644); err != nil {
		t.Fatal(err)
	}
	cp, ok, err := readCheckpointFile(path)
	if err != nil || !ok || cp != 42 {
		t.Fatalf("cp=%d ok=%v err=%v", cp, ok, err)
	}
}
