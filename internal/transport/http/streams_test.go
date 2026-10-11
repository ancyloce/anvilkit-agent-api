package http_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	httptransport "github.com/ancyloce/anvilkit-agent-api/internal/transport/http"
)

// P1.4 (API-03, API-10): streams are capped per principal with a retryable
// 429 and Retry-After, every stream advertises its reconnect interval, and
// readiness follows Control's health.
func TestStreamCapsRetryAndReadiness(t *testing.T) {
	fake := newFake()
	opts := testOptions()
	opts.Stream.MaxStreamsPerPrincipal, opts.Stream.MaxStreams, opts.Stream.RetryBase = 2, 3, 2*time.Second
	controlDown := false
	srv, err := httptransport.NewServer(opts, verifier{}, fake, httptransport.Gateways{}, func(context.Context) error {
		if controlDown {
			return errors.New("control unreachable")
		}
		return nil
	})
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	tokenA, tokenB := "token-tenant-a-0123456789", "token-tenant-b-0123456789"
	status, _, _ := do(t, ts, "POST", "/api/v1/operations", tokenA, createBody("s1"))
	require.Equal(t, 202, status)
	status, _, _ = do(t, ts, "POST", "/api/v1/operations", tokenB, createBody("s2"))
	require.Equal(t, 202, status)
	opA, opB := "op_s1", "op_s2"

	open := func(token, op string) *http.Response {
		resp := sseRequest(t, ts, "/api/v1/operations/"+op+"/events", token, nil)
		t.Cleanup(func() { resp.Body.Close() })
		return resp
	}
	first := open(tokenA, opA)
	require.Equal(t, 200, first.StatusCode)
	// The stream starts with the advertised reconnect interval (base plus up to 50% jitter).
	line, err := bufio.NewReader(first.Body).ReadString('\n')
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(line, "retry: "), line)
	var retry int
	_, err = fmt.Sscanf(strings.TrimSpace(strings.TrimPrefix(line, "retry: ")), "%d", &retry)
	require.NoError(t, err)
	require.GreaterOrEqual(t, retry, 2000)
	require.LessOrEqual(t, retry, 3000)
	second := open(tokenA, opA)
	require.Equal(t, 200, second.StatusCode)
	// The third stream of the principal is refused as retryable capacity with Retry-After; another principal is admitted.
	third := open(tokenA, opA)
	require.Equal(t, 429, third.StatusCode)
	require.Equal(t, "5", third.Header.Get("Retry-After"))
	body := new(strings.Builder)
	_, _ = bufioCopy(body, third.Body)
	require.Contains(t, body.String(), `"code":"CAPACITY_EXHAUSTED"`)
	require.Contains(t, body.String(), `"retryable":true`)
	other := open(tokenB, opB)
	require.Equal(t, 200, other.StatusCode)
	// The process cap: a further stream of anyone is refused.
	fourth := open(tokenB, opB)
	require.Equal(t, 429, fourth.StatusCode)
	// A closed stream frees its slot.
	first.Body.Close()
	require.Eventually(t, func() bool {
		resp := sseRequest(t, ts, "/api/v1/operations/"+opA+"/events", tokenA, nil)
		defer resp.Body.Close()
		return resp.StatusCode == 200
	}, 5*time.Second, 50*time.Millisecond)

	// Readiness follows Control's health.
	status, _, _ = do(t, ts, "GET", "/readyz", "", "")
	require.Equal(t, 204, status)
	controlDown = true
	status, _, _ = do(t, ts, "GET", "/readyz", "", "")
	require.Equal(t, 503, status)
}

func bufioCopy(dst *strings.Builder, src io.Reader) (int64, error) { return io.Copy(dst, src) }
