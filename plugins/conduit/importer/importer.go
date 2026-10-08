package importer

import (
	"context"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/algorand/go-algorand-sdk/v2/encoding/json"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"
	"github.com/algorand/conduit/conduit/data"
	"github.com/algorand/conduit/conduit/plugins"
	"github.com/algorand/conduit/conduit/plugins/importers"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
	ffconduit "github.com/NautilusOSS/voi-fast-follower/internal/conduit"
	"github.com/NautilusOSS/voi-fast-follower/internal/stream"
)

const (
	// PluginName is the conduit.yml importer name.
	PluginName  = "voi_archive"
	modeOffline   = "offline"
	modeFollow    = "follow"
	modeBootstrap = "bootstrap" // alias of follow: historical archive then live via growing archive
)

//go:embed sample.yaml
var sampleConfig string

type archiveImporter struct {
	logger       *logrus.Logger
	cfg          Config
	ctx          context.Context
	cancel       context.CancelFunc
	src          *stream.ArchiveSource
	genesis      *sdk.Genesis
	pollInterval time.Duration
	waitTimeout  time.Duration
}

func parseDurations(pollStr, waitStr string) (poll, wait time.Duration, err error) {
	poll = 200 * time.Millisecond
	if strings.TrimSpace(pollStr) != "" {
		poll, err = time.ParseDuration(pollStr)
		if err != nil {
			return 0, 0, fmt.Errorf("voi_archive: poll_interval: %w", err)
		}
	}
	if strings.TrimSpace(waitStr) != "" {
		wait, err = time.ParseDuration(waitStr)
		if err != nil {
			return 0, 0, fmt.Errorf("voi_archive: wait_timeout: %w", err)
		}
	}
	return poll, wait, nil
}

func init() {
	importers.Register(PluginName, importers.ImporterConstructorFunc(func() importers.Importer {
		return &archiveImporter{}
	}))
}

var metadata = plugins.Metadata{
	Name:         PluginName,
	Description:  "Importer that reads ordered blocks from a voi-fast-follower archive (no Voi sync).",
	Deprecated:   false,
	SampleConfig: sampleConfig,
}

func (imp *archiveImporter) Metadata() plugins.Metadata { return metadata }

func (imp *archiveImporter) Init(ctx context.Context, _ data.InitProvider, cfg plugins.PluginConfig, logger *logrus.Logger) error {
	imp.ctx, imp.cancel = context.WithCancel(ctx)
	imp.logger = logger
	if err := cfg.UnmarshalConfig(&imp.cfg); err != nil {
		return fmt.Errorf("voi_archive: config: %w", err)
	}
	imp.cfg.ArchivePath = strings.TrimSpace(imp.cfg.ArchivePath)
	imp.cfg.GenesisFile = strings.TrimSpace(imp.cfg.GenesisFile)
	if imp.cfg.ArchivePath == "" {
		return fmt.Errorf("voi_archive: archive_path required")
	}
	if imp.cfg.GenesisFile == "" {
		return fmt.Errorf("voi_archive: genesis_file required")
	}
	if imp.cfg.Mode == "" {
		imp.cfg.Mode = modeFollow
	}
	if imp.cfg.Mode == modeBootstrap {
		imp.cfg.Mode = modeFollow
	}
	switch imp.cfg.Mode {
	case modeOffline, modeFollow:
	default:
		return fmt.Errorf("voi_archive: unsupported mode %q (use offline|follow|bootstrap)", imp.cfg.Mode)
	}
	poll, wait, err := parseDurations(imp.cfg.PollInterval, imp.cfg.WaitTimeout)
	if err != nil {
		return err
	}
	imp.pollInterval = poll
	imp.waitTimeout = wait

	raw, err := os.ReadFile(imp.cfg.GenesisFile)
	if err != nil {
		return fmt.Errorf("voi_archive: read genesis: %w", err)
	}
	var genesis sdk.Genesis
	if err := json.Decode(raw, &genesis); err != nil {
		return fmt.Errorf("voi_archive: parse genesis: %w", err)
	}
	imp.genesis = &genesis

	src, err := stream.OpenArchive(imp.cfg.ArchivePath)
	if err != nil {
		return fmt.Errorf("voi_archive: open archive: %w", err)
	}
	imp.src = src

	cp, ok, err := src.Checkpoint(imp.ctx)
	if err != nil {
		return err
	}
	if ok {
		imp.logger.Infof("voi_archive: opened archive checkpoint=%d mode=%s", cp, imp.cfg.Mode)
	} else {
		imp.logger.Warnf("voi_archive: archive has no checkpoint yet (mode=%s)", imp.cfg.Mode)
	}
	return nil
}

func (imp *archiveImporter) GetGenesis() (*sdk.Genesis, error) {
	if imp.genesis == nil {
		return nil, fmt.Errorf("voi_archive: GetGenesis before Init")
	}
	return imp.genesis, nil
}

func (imp *archiveImporter) Close() error {
	if imp.cancel != nil {
		imp.cancel()
	}
	if imp.src != nil {
		return imp.src.Close()
	}
	return nil
}

func (imp *archiveImporter) GetBlock(rnd uint64) (data.BlockData, error) {
	start := time.Now()
	blk, err := imp.fetch(rnd)
	if err != nil {
		deliveryErrors.Inc()
		return data.BlockData{}, err
	}
	translated, err := ffconduit.Translate(blk)
	if err != nil {
		deliveryErrors.Inc()
		return data.BlockData{}, err
	}
	out := data.BlockData{
		BlockHeader: translated.BlockHeader,
		Payset:      translated.Payset,
		Certificate: translated.Certificate,
		// Delta intentionally nil — archive has BlockRaw only.
	}
	blocksDelivered.Inc()
	deliveryLatency.Observe(time.Since(start).Seconds())
	if cp, ok, _ := imp.src.Checkpoint(imp.ctx); ok {
		conduitLag.Set(float64(int64(cp) - int64(rnd)))
	}
	return out, nil
}

func (imp *archiveImporter) fetch(rnd uint64) (block.Block, error) {
	switch imp.cfg.Mode {
	case modeFollow:
		return imp.src.GetWait(imp.ctx, rnd, stream.WaitOptions{
			PollInterval: imp.pollInterval,
			Timeout:      imp.waitTimeout,
		})
	default: // offline
		blk, ok, err := imp.src.Get(imp.ctx, rnd)
		if err != nil {
			return block.Block{}, err
		}
		if !ok {
			cp, cpOK, _ := imp.src.Checkpoint(imp.ctx)
			if cpOK {
				return block.Block{}, fmt.Errorf("voi_archive: round %d not in archive (checkpoint=%d)", rnd, cp)
			}
			return block.Block{}, fmt.Errorf("voi_archive: round %d not in archive (no checkpoint)", rnd)
		}
		return blk, nil
	}
}

func (imp *archiveImporter) ProvideMetrics(subsystem string) []prometheus.Collector {
	_ = subsystem
	return collectors()
}
