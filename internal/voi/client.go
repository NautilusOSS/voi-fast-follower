package voi

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/algod"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common"
	"github.com/algorand/go-algorand-sdk/v2/client/v2/common/models"

	"github.com/nicholasshellabarger/voi-fast-follower/internal/block"
)

// Client wraps algod with retry/backoff for node outages.
type Client struct {
	algod      *algod.Client
	log        *slog.Logger
	maxRetries int
	baseDelay  time.Duration
	maxDelay   time.Duration
}

// New creates a Voi algod client.
func New(algodURL, token string, log *slog.Logger) (*Client, error) {
	ac, err := algod.MakeClient(algodURL, token)
	if err != nil {
		return nil, fmt.Errorf("make algod client: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	return &Client{
		algod:      ac,
		log:        log,
		maxRetries: 0, // 0 = retry forever until context cancel
		baseDelay:  500 * time.Millisecond,
		maxDelay:   30 * time.Second,
	}, nil
}

// Status returns the current node status.
func (c *Client) Status(ctx context.Context) (models.NodeStatus, error) {
	var status models.NodeStatus
	err := c.withRetry(ctx, "status", func(ctx context.Context) error {
		var err error
		status, err = c.algod.Status().Do(ctx)
		return err
	})
	return status, err
}

// LastRound returns the network tip round.
func (c *Client) LastRound(ctx context.Context) (uint64, error) {
	st, err := c.Status(ctx)
	if err != nil {
		return 0, err
	}
	return st.LastRound, nil
}

// GetBlock retrieves a block via msgpack BlockRaw and decodes it.
// The original raw msgpack bytes are preserved on the returned Block.
func (c *Client) GetBlock(ctx context.Context, round uint64) (block.Block, error) {
	var raw []byte
	err := c.withRetry(ctx, fmt.Sprintf("block/%d", round), func(ctx context.Context) error {
		var err error
		raw, err = c.algod.BlockRaw(round).Do(ctx)
		return err
	})
	if err != nil {
		return block.Block{}, err
	}
	return block.DecodeRaw(raw)
}

// FetchBlock is an alias for GetBlock (kept for older call sites/tests).
func (c *Client) FetchBlock(ctx context.Context, round uint64) (block.Block, error) {
	return c.GetBlock(ctx, round)
}

// AccountBalance returns the microVOI balance for an address (best-effort).
// Uses a short local retry loop so explorer requests do not share follower backoff state.
func (c *Client) AccountBalance(ctx context.Context, address string) (uint64, error) {
	var last error
	delay := 200 * time.Millisecond
	for attempt := 1; attempt <= 3; attempt++ {
		info, err := c.algod.AccountInformation(address).Do(ctx)
		if err == nil {
			return info.Amount, nil
		}
		last = err
		if ctx.Err() != nil || !isRetryable(err) {
			return 0, err
		}
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
	}
	return 0, last
}

// WaitForBlockAfter waits until a block after round is available and returns
// the new network tip (last-round).
func (c *Client) WaitForBlockAfter(ctx context.Context, round uint64) (uint64, error) {
	var status models.NodeStatus
	err := c.withRetry(ctx, fmt.Sprintf("wait-after/%d", round), func(ctx context.Context) error {
		var err error
		status, err = c.algod.StatusAfterBlock(round).Do(ctx)
		return err
	})
	if err != nil {
		return 0, err
	}
	return status.LastRound, nil
}

func (c *Client) withRetry(ctx context.Context, op string, fn func(context.Context) error) error {
	delay := c.baseDelay
	attempt := 0
	for {
		attempt++
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !isRetryable(err) {
			return err
		}
		if c.maxRetries > 0 && attempt > c.maxRetries {
			return fmt.Errorf("%s: exceeded retries: %w", op, err)
		}
		c.log.Warn("algod request failed; backing off",
			"op", op,
			"attempt", attempt,
			"delay", delay,
			"err", err,
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > c.maxDelay {
			delay = c.maxDelay
		}
	}
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	var badReq common.BadRequest
	var invalid common.InvalidToken
	var notFound common.NotFound
	if errors.As(err, &badReq) || errors.As(err, &invalid) || errors.As(err, &notFound) {
		return false
	}

	var httpErr common.HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.StatusCode {
		case 400, 401, 403, 404:
			return false
		case 408, 429, 500, 502, 503, 504:
			return true
		}
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "connection refused"),
		strings.Contains(msg, "connection reset"),
		strings.Contains(msg, "timeout"),
		strings.Contains(msg, "temporarily unavailable"),
		strings.Contains(msg, "i/o timeout"),
		strings.Contains(msg, "eof"),
		strings.Contains(msg, "http 502"),
		strings.Contains(msg, "http 503"),
		strings.Contains(msg, "http 504"),
		strings.Contains(msg, "http 429"):
		return true
	case strings.Contains(msg, "http 400"),
		strings.Contains(msg, "http 401"),
		strings.Contains(msg, "http 403"),
		strings.Contains(msg, "http 404"):
		return false
	}
	// Unknown transport-ish failures: retry.
	return true
}
