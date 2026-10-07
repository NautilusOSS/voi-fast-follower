package voi

import (
	"errors"
	"net"
	"testing"

	"github.com/algorand/go-algorand-sdk/v2/client/v2/common"
)

func TestIsRetryable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"not found", common.NotFound{HTTPError: common.HTTPError{StatusCode: 404, Message: "no"}}, false},
		{"bad request", common.BadRequest{HTTPError: common.HTTPError{StatusCode: 400, Message: "bad"}}, false},
		{"internal", common.InternalError{HTTPError: common.HTTPError{StatusCode: 500, Message: "boom"}}, true},
		{"net", &net.OpError{Op: "dial", Err: errors.New("connection refused")}, true},
		{"msg 503", errors.New("HTTP 503: unavailable"), true},
		{"msg 404", errors.New("HTTP 404: missing"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRetryable(tc.err); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
}
