// Package conduit translates canonical follower blocks into the field bundle
// Conduit importers feed into data.BlockData — without depending on Conduit itself.
//
// Translation is non-destructive: block.Block.Raw remains the source of truth.
package conduit

import (
	"fmt"
	"sync"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// msgpackMu serializes use of the SDK's shared codec handle.
var msgpackMu sync.Mutex

// BlockData is the Conduit-compatible field bundle derived from a canonical Block.
// Delta is intentionally absent: Fast Follower archives store BlockRaw only.
type BlockData struct {
	BlockHeader sdk.BlockHeader
	Payset      []sdk.SignedTxnInBlock
	Certificate *map[string]interface{}
}

// Round returns the block round.
func (d BlockData) Round() uint64 { return uint64(d.BlockHeader.Round) }

// Translate decodes blk.Raw into Conduit fields and checks round/hash consistency.
func Translate(blk block.Block) (BlockData, error) {
	if len(blk.Raw) == 0 {
		return BlockData{}, fmt.Errorf("conduit: empty raw bytes for round %d", blk.Round)
	}

	// Domain decode validates hash / prev-hash / txn extraction against Raw.
	decoded, err := block.DecodeRaw(blk.Raw)
	if err != nil {
		return BlockData{}, fmt.Errorf("conduit: decode BlockRaw round %d: %w", blk.Round, err)
	}
	if blk.Round != 0 && decoded.Round != blk.Round {
		return BlockData{}, fmt.Errorf("conduit: round mismatch raw=%d meta=%d", decoded.Round, blk.Round)
	}
	if blk.BlockHash != "" && decoded.BlockHash != blk.BlockHash {
		return BlockData{}, fmt.Errorf("conduit: block hash mismatch round %d", blk.Round)
	}
	if blk.PreviousBlockHash != "" && decoded.PreviousBlockHash != blk.PreviousBlockHash {
		return BlockData{}, fmt.Errorf("conduit: prev hash mismatch round %d", blk.Round)
	}

	msgpackMu.Lock()
	defer msgpackMu.Unlock()

	var resp models.BlockResponse
	if err := msgpack.Decode(blk.Raw, &resp); err != nil {
		return BlockData{}, fmt.Errorf("conduit: decode BlockResponse round %d: %w", blk.Round, err)
	}

	out := BlockData{
		BlockHeader: resp.Block.BlockHeader,
		Payset:      append([]sdk.SignedTxnInBlock(nil), resp.Block.Payset...),
		Certificate: resp.Cert,
	}
	if blk.TxnCount != 0 && len(out.Payset) != blk.TxnCount {
		return BlockData{}, fmt.Errorf("conduit: txn count mismatch round %d raw=%d meta=%d",
			blk.Round, len(out.Payset), blk.TxnCount)
	}
	return out, nil
}
