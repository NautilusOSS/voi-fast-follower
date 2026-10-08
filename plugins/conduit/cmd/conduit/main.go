// Command conduit is a custom Conduit binary that registers the voi_archive
// importer alongside a small set of built-in Conduit plugins.
//
// Build (from repo root):
//
//	cd plugins/conduit && go build -o ../../bin/conduit ./cmd/conduit
//
// List plugins:
//
//	./bin/conduit list
package main

import (
	"fmt"
	"os"

	// Built-in Conduit plugins used for demos / e2e.
	_ "github.com/algorand/conduit/conduit/plugins/exporters/filewriter"
	_ "github.com/algorand/conduit/conduit/plugins/exporters/noop"
	_ "github.com/algorand/conduit/conduit/plugins/exporters/postgresql"
	_ "github.com/algorand/conduit/conduit/plugins/importers/algod"
	_ "github.com/algorand/conduit/conduit/plugins/importers/filereader"
	_ "github.com/algorand/conduit/conduit/plugins/importers/noop"
	_ "github.com/algorand/conduit/conduit/plugins/processors/filterprocessor"
	_ "github.com/algorand/conduit/conduit/plugins/processors/noop"

	// Fast Follower archive importer (optional Conduit consumer).
	_ "github.com/NautilusOSS/voi-fast-follower/plugins/conduit/importer"

	"github.com/algorand/conduit/pkg/cli"
)

func main() {
	cmd := cli.MakeConduitCmdWithUtilities()
	if err := cmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	os.Exit(0)
}
