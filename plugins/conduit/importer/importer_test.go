package importer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/algorand/go-algorand-sdk/v2/encoding/json"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/algorand/conduit/conduit/plugins"

	ffconduit "github.com/NautilusOSS/voi-fast-follower/internal/conduit"
	"github.com/NautilusOSS/voi-fast-follower/internal/storage"
)

func writeGenesis(t *testing.T, path string) {
	t.Helper()
	g := sdk.Genesis{
		SchemaID: "test-v1",
		Network:  "testnet",
		Proto:    "test-proto",
	}
	if err := os.WriteFile(path, json.Encode(g), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestImporterArchiveOffline(t *testing.T) {
	dir := t.TempDir()
	archRoot := filepath.Join(dir, "archive")
	genesisPath := filepath.Join(dir, "genesis.json")
	writeGenesis(t, genesisPath)

	sink, err := storage.NewArchive(storage.ArchiveOptions{Root: archRoot, SegmentSize: 100, DisableSync: true})
	if err != nil {
		t.Fatal(err)
	}
	chain := ffconduit.MakeFixtureChain(300, 5, true)
	if err := sink.CommitBatch(context.Background(), chain); err != nil {
		t.Fatal(err)
	}
	_ = sink.Close()

	imp := &archiveImporter{}
	cfgStr := fmt.Sprintf("archive_path: %q\ngenesis_file: %q\nmode: offline\n", archRoot, genesisPath)
	logger := logrus.New()
	logger.SetOutput(os.Stderr)
	if err := imp.Init(context.Background(), nil, plugins.MakePluginConfig(cfgStr), logger); err != nil {
		t.Fatalf("init: %v", err)
	}
	defer imp.Close()

	gen, err := imp.GetGenesis()
	if err != nil || gen.Network != "testnet" {
		t.Fatalf("genesis=%v err=%v", gen, err)
	}

	var prev string
	for r := uint64(300); r <= 304; r++ {
		bd, err := imp.GetBlock(r)
		if err != nil {
			t.Fatalf("GetBlock %d: %v", r, err)
		}
		if bd.Round() != r {
			t.Fatalf("round=%d want=%d", bd.Round(), r)
		}
		if len(bd.Payset) != 1 {
			t.Fatalf("payset empty at %d", r)
		}
		if bd.Delta != nil {
			t.Fatal("delta should be nil")
		}
		h := ffconduit.HashHeader(bd.BlockHeader)
		if h != chain[r-300].BlockHash {
			t.Fatalf("hash mismatch at %d", r)
		}
		if prev != "" && chain[r-300].PreviousBlockHash != prev {
			t.Fatalf("linkage at %d", r)
		}
		prev = h
	}

	_, err = imp.GetBlock(999)
	if err == nil {
		t.Fatal("expected missing round error")
	}
}
