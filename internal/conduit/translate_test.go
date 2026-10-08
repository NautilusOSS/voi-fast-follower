package conduit

import (
	"testing"

	"github.com/NautilusOSS/voi-fast-follower/internal/block"
)

func TestTranslatePreservesRoundHashPaysetCert(t *testing.T) {
	chain := MakeFixtureChain(100, 3, true)
	for i, blk := range chain {
		data, err := Translate(blk)
		if err != nil {
			t.Fatalf("round %d: %v", blk.Round, err)
		}
		if data.Round() != blk.Round {
			t.Fatalf("round mismatch %d != %d", data.Round(), blk.Round)
		}
		if HashHeader(data.BlockHeader) != blk.BlockHash {
			t.Fatalf("hash not preserved round %d", blk.Round)
		}
		if i > 0 {
			prev := chain[i-1]
			if err := block.ValidateLinkage(prev.BlockHash, blk); err != nil {
				t.Fatal(err)
			}
			// Branch in header must match previous block hash digest path used by DecodeRaw.
			if blk.PreviousBlockHash != prev.BlockHash {
				t.Fatalf("prev hash link broken at %d", blk.Round)
			}
		}
		if len(data.Payset) != 1 {
			t.Fatalf("payset len=%d want 1", len(data.Payset))
		}
		if data.Certificate == nil {
			t.Fatal("certificate missing")
		}
		// Raw must remain intact on the canonical block.
		if len(blk.Raw) == 0 {
			t.Fatal("raw emptied")
		}
		again, err := Translate(blk)
		if err != nil {
			t.Fatal(err)
		}
		if again.Round() != data.Round() || len(again.Payset) != len(data.Payset) {
			t.Fatal("retranslate mismatch")
		}
	}
}

func TestTranslateEmptyRaw(t *testing.T) {
	_, err := Translate(block.Block{Round: 1})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestTranslateHashMismatch(t *testing.T) {
	blk := MakeFixtureBlock(5, "", false)
	blk.BlockHash = "NOTAHASH"
	_, err := Translate(blk)
	if err == nil {
		t.Fatal("expected hash mismatch")
	}
}

func TestTranslateTxnCount(t *testing.T) {
	blk := MakeFixtureBlock(7, "", true)
	if blk.TxnCount != 1 {
		t.Fatalf("fixture txncount=%d", blk.TxnCount)
	}
	data, err := Translate(blk)
	if err != nil {
		t.Fatal(err)
	}
	if len(data.Payset) != 1 {
		t.Fatalf("payset=%d", len(data.Payset))
	}
}
