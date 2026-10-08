package conduit

import (
	"crypto/sha512"
	"encoding/base32"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	sdk "github.com/algorand/go-algorand-sdk/v2/types"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// MakeFixtureBlock builds a canonical Block with valid algod-shaped BlockRaw
// msgpack for deterministic tests (no network).
func MakeFixtureBlock(round uint64, prevHash string, withTxn bool) block.Block {
	var branch sdk.BlockHash
	if prevHash != "" {
		raw, err := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(prevHash)
		if err == nil && len(raw) == len(branch) {
			copy(branch[:], raw)
		}
	}

	payset := sdk.Payset{}
	if withTxn {
		var sender sdk.Address
		sender[0] = byte(round)
		stib := sdk.SignedTxnInBlock{
			SignedTxnWithAD: sdk.SignedTxnWithAD{
				SignedTxn: sdk.SignedTxn{
					Txn: sdk.Transaction{
						Type: sdk.PaymentTx,
						Header: sdk.Header{
							Sender:     sender,
							Fee:        1000,
							FirstValid: sdk.Round(round),
							LastValid:  sdk.Round(round + 1000),
						},
						PaymentTxnFields: sdk.PaymentTxnFields{
							Amount: 1,
						},
					},
				},
			},
		}
		payset = append(payset, stib)
	}

	hdr := sdk.BlockHeader{
		Round:     sdk.Round(round),
		Branch:    branch,
		TimeStamp: int64(round),
	}
	resp := models.BlockResponse{
		Block: sdk.Block{
			BlockHeader: hdr,
			Payset:      payset,
		},
		Cert: &map[string]interface{}{
			"round": float64(round),
		},
	}

	msgpackMu.Lock()
	raw := append([]byte(nil), msgpack.Encode(resp)...)
	msgpackMu.Unlock()

	blk, err := block.DecodeRaw(raw)
	if err != nil {
		// DecodeRaw failure would indicate fixture construction bug.
		panic(err)
	}
	return blk
}

// MakeFixtureChain returns n contiguous fixture blocks starting at base.
func MakeFixtureChain(base uint64, n int, withTxn bool) []block.Block {
	out := make([]block.Block, n)
	var prev string
	for i := 0; i < n; i++ {
		out[i] = MakeFixtureBlock(base+uint64(i), prev, withTxn)
		prev = out[i].BlockHash
	}
	return out
}

// HashHeader exposes the BH hash used by fixtures for assertions.
func HashHeader(hdr sdk.BlockHeader) string {
	msgpackMu.Lock()
	enc := msgpack.Encode(hdr)
	msgpackMu.Unlock()
	sum := sha512.Sum512_256(append([]byte("BH"), enc...))
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])
}
