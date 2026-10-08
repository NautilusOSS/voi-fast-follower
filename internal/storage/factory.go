package storage

import (
	"context"
	"fmt"
	"strings"
)

// SinkBundle holds the composed sink used by the follower plus optional
// typed handles for APIs that need a specific backend.
type SinkBundle struct {
	Primary  BatchBlockSink
	Postgres *PostgresBlockSink
	Archive  *ArchiveSink
	Multi    *MultiSink
	Names    []string
}

// Close closes all owned sinks.
func (b *SinkBundle) Close() error {
	if b.Primary != nil {
		return b.Primary.Close()
	}
	return nil
}

// BuildOptions configures sink construction.
type BuildOptions struct {
	PostgresURL        string
	PostgresMigrations string
	PostgresInsertMode string
	PostgresAsync      bool

	ArchivePath        string
	ArchiveSegmentSize int
	ArchiveDisableSync bool
}

// BuildSinks constructs Postgres and/or Archive sinks. At least one must be configured.
// When both are enabled they are wrapped in a MultiSink (all-must-succeed).
func BuildSinks(ctx context.Context, opts BuildOptions) (*SinkBundle, error) {
	wantPG := strings.TrimSpace(opts.PostgresURL) != ""
	wantArch := strings.TrimSpace(opts.ArchivePath) != ""
	if !wantPG && !wantArch {
		return nil, fmt.Errorf("at least one sink required: database.url or archive.path")
	}

	out := &SinkBundle{}
	var sinks []BatchBlockSink
	var names []string

	if wantPG {
		pg, err := NewPostgres(ctx, opts.PostgresURL, opts.PostgresMigrations)
		if err != nil {
			return nil, err
		}
		if err := pg.SetInsertMode(opts.PostgresInsertMode); err != nil {
			_ = pg.Close()
			return nil, err
		}
		pg.SetAsyncCommit(opts.PostgresAsync)
		out.Postgres = pg
		sinks = append(sinks, pg)
		names = append(names, "postgres")
	}
	if wantArch {
		arch, err := NewArchive(ArchiveOptions{
			Root:        opts.ArchivePath,
			SegmentSize: opts.ArchiveSegmentSize,
			DisableSync: opts.ArchiveDisableSync,
		})
		if err != nil {
			if out.Postgres != nil {
				_ = out.Postgres.Close()
			}
			return nil, err
		}
		out.Archive = arch
		sinks = append(sinks, arch)
		names = append(names, "archive")
	}

	out.Names = names
	if len(sinks) == 1 {
		out.Primary = sinks[0]
		return out, nil
	}
	multi, err := NewMultiSink(sinks, names)
	if err != nil {
		_ = out.Close()
		return nil, err
	}
	out.Multi = multi
	out.Primary = multi
	return out, nil
}
