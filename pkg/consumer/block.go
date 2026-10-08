package consumer

import (
	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

// Block is the canonical raw block exposed to external consumers.
type Block struct {
	Round             uint64
	BlockHash         string
	PreviousBlockHash string
	Timestamp         int64
	TxnCount          int
	Raw               []byte
}

func toInternal(b Block) block.Block {
	return block.Block{
		Round:             b.Round,
		BlockHash:         b.BlockHash,
		PreviousBlockHash: b.PreviousBlockHash,
		Timestamp:         b.Timestamp,
		TxnCount:          b.TxnCount,
		Raw:               append([]byte(nil), b.Raw...),
	}
}

func fromInternal(b block.Block) Block {
	txn := b.TxnCount
	if txn == 0 && len(b.Transactions) > 0 {
		txn = len(b.Transactions)
	}
	return Block{
		Round:             b.Round,
		BlockHash:         b.BlockHash,
		PreviousBlockHash: b.PreviousBlockHash,
		Timestamp:         b.Timestamp,
		TxnCount:          txn,
		Raw:               append([]byte(nil), b.Raw...),
	}
}
