package block

import (
	"crypto/sha512"
	"encoding/base32"
	"fmt"
	"sync"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"
	"github.com/algorand/go-algorand-sdk/v2/crypto"
	"github.com/algorand/go-algorand-sdk/v2/encoding/msgpack"
	"github.com/algorand/go-algorand-sdk/v2/types"
)

// msgpackMu serializes use of the SDK's shared codec handle.
// go-codec Handles are not safe for concurrent Encode/Decode.
var msgpackMu sync.Mutex

// Block is the canonical, sink-agnostic representation of a fetched Voi block.
// The follower emits an ordered stream of Block values; sinks persist or forward
// them. Raw always retains the original algod msgpack BlockRaw bytes.
type Block struct {
	Round             uint64
	BlockHash         string
	PreviousBlockHash string
	Timestamp         int64
	TxnCount          int
	Raw               []byte
	Transactions      []Transaction
}

// Transaction is a minimal raw-first transaction record.
type Transaction struct {
	TxID  string
	Sender string
	Type  string
	AppID uint64 // set for application call txs; 0 otherwise
	Raw   []byte
}

// DecodeRaw parses algod msgpack block bytes (EncodedBlockCertificate shape)
// into a domain Block, preserving the original raw payload.
func DecodeRaw(raw []byte) (Block, error) {
	if len(raw) == 0 {
		return Block{}, fmt.Errorf("empty block bytes")
	}

	msgpackMu.Lock()
	defer msgpackMu.Unlock()

	var resp models.BlockResponse
	if err := msgpack.Decode(raw, &resp); err != nil {
		return Block{}, fmt.Errorf("decode block response: %w", err)
	}

	hdr := resp.Block.BlockHeader
	round := uint64(hdr.Round)
	hash := hashBlockHeaderLocked(hdr)
	prev := digestBase32(types.Digest(hdr.Branch))

	txs := make([]Transaction, 0, len(resp.Block.Payset))
	for _, stib := range resp.Block.Payset {
		tx := stib.Txn
		txid := crypto.GetTxID(tx)
		var appID uint64
		if tx.Type == types.ApplicationCallTx {
			appID = uint64(tx.ApplicationID)
		}
		txs = append(txs, Transaction{
			TxID:   txid,
			Sender: tx.Sender.String(),
			Type:   string(tx.Type),
			AppID:  appID,
			Raw:    append([]byte(nil), msgpack.Encode(stib)...),
		})
	}

	return Block{
		Round:             round,
		BlockHash:         hash,
		PreviousBlockHash: prev,
		Timestamp:         hdr.TimeStamp,
		TxnCount:          len(txs),
		Raw:               append([]byte(nil), raw...),
		Transactions:      txs,
	}, nil
}

func hashBlockHeaderLocked(hdr types.BlockHeader) string {
	enc := msgpack.Encode(hdr)
	sum := sha512.Sum512_256(append([]byte("BH"), enc...))
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(sum[:])
}

func digestBase32(d types.Digest) string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(d[:])
}

// ValidateLinkage checks that blk.PreviousBlockHash matches the prior block hash.
func ValidateLinkage(prevHash string, blk Block) error {
	if prevHash == "" {
		return nil
	}
	if blk.PreviousBlockHash != prevHash {
		return fmt.Errorf("hash linkage broken: prev=%s block.prev=%s round=%d",
			prevHash, blk.PreviousBlockHash, blk.Round)
	}
	return nil
}

// AppIDFromRaw returns the application id from a stored SignedTxnInBlock msgpack blob.
func AppIDFromRaw(raw []byte) (uint64, bool) {
	if len(raw) == 0 {
		return 0, false
	}
	msgpackMu.Lock()
	defer msgpackMu.Unlock()
	var stib types.SignedTxnInBlock
	if err := msgpack.Decode(raw, &stib); err != nil {
		return 0, false
	}
	if stib.Txn.Type != types.ApplicationCallTx {
		return 0, false
	}
	return uint64(stib.Txn.ApplicationID), true
}
