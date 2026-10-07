package block

import (
	"context"
	"os"
	"testing"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
)

func TestDecodeRawLiveBlock(t *testing.T) {
	if os.Getenv("VOI_INTEGRATION") == "" {
		t.Skip("set VOI_INTEGRATION=1 to run live algod tests")
	}
	url := os.Getenv("VOI_ALGOD_URL")
	if url == "" {
		url = "https://mainnet-api.voi.nodely.dev"
	}
	c, err := algod.MakeClient(url, os.Getenv("VOI_ALGOD_TOKEN"))
	if err != nil {
		t.Fatal(err)
	}
	st, err := c.Status().Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	round := st.LastRound - 10
	raw, err := c.BlockRaw(round).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	blk, err := DecodeRaw(raw)
	if err != nil {
		t.Fatal(err)
	}
	if blk.Round != round {
		t.Fatalf("round=%d want=%d", blk.Round, round)
	}
	if blk.BlockHash == "" {
		t.Fatal("empty block hash")
	}
	if len(blk.Raw) == 0 {
		t.Fatal("raw not preserved")
	}
	apiHash, err := c.GetBlockHash(round).Do(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if blk.BlockHash != apiHash.Blockhash {
		t.Fatalf("hash mismatch got=%s want=%s", blk.BlockHash, apiHash.Blockhash)
	}
	if blk.TxnCount != len(blk.Transactions) {
		t.Fatalf("txn count mismatch")
	}
}

func TestDecodeRawEmpty(t *testing.T) {
	_, err := DecodeRaw(nil)
	if err == nil {
		t.Fatal("expected error")
	}
}
