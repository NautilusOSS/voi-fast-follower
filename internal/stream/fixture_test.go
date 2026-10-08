package stream

import (
	"encoding/base32"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

func fixtureChain(base uint64, n int) []block.Block {
	out := make([]block.Block, n)
	var prevHash string
	for i := 0; i < n; i++ {
		var branch sdk.BlockHash
		if prevHash != "" {
			raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(prevHash)
			if err == nil && len(raw) == len(branch) {
				copy(branch[:], raw)
			}
		}
		round := base + uint64(i)
		resp := models.BlockResponse{
			Block: sdk.Block{
				BlockHeader: sdk.BlockHeader{
					Round:     sdk.Round(round),
					Branch:    branch,
					TimeStamp: int64(round),
				},
			},
			Cert: &map[string]interface{}{"r": float64(round)},
		}
		raw := append([]byte(nil), msgpack.Encode(resp)...)
		blk, err := block.DecodeRaw(raw)
		if err != nil {
			panic(err)
		}
		out[i] = blk
		prevHash = blk.BlockHash
	}
	return out
}
