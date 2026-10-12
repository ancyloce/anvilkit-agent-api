package http_test

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	httptransport "github.com/ancyloce/anvilkit-agent-api/internal/transport/http"
)

// TestShutdownEndsOpenStreams is P1.7 AC3 at the transport: with three
// event streams open, Stop ends each with a final `end` event, the listener
// drains well inside its bound (streams never go idle on their own, so
// without the end the drain would run to the bound) and Stop reports a
// clean drain.
func TestShutdownEndsOpenStreams(t *testing.T) {
	fake := newFake()
	srv, err := httptransport.NewServer(testOptions(), verifier{}, fake, httptransport.Gateways{}, func(context.Context) error { return nil })
	require.NoError(t, err)
	addr, err := srv.Start()
	require.NoError(t, err)
	base := "http://" + addr.String()
	token := "token-tenant-a-0123456789"
	req, err := http.NewRequest("POST", base+"/api/v1/operations", strings.NewReader(createBody("drain")))
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	require.Equal(t, 202, resp.StatusCode)

	bodies := make([]chan string, 3)
	for i := range bodies {
		req, err := http.NewRequest("GET", base+"/api/v1/operations/op_drain/events", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode)
		r := bufio.NewReader(resp.Body)
		line, err := r.ReadString('\n')
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(line, "retry: "), "the stream is open")
		bodies[i] = make(chan string, 1)
		go func(out chan<- string) {
			defer resp.Body.Close()
			rest, _ := io.ReadAll(r)
			out <- string(rest)
		}(bodies[i])
	}
	const drain = 20 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), drain)
	defer cancel()
	started := time.Now()
	require.NoError(t, srv.Stop(ctx), "a clean drain")
	took := time.Since(started)
	require.Less(t, took, 2*time.Second, "the drain does not wait for its bound")
	for i, b := range bodies {
		select {
		case body := <-b:
			require.Contains(t, body, "event:end\n", "stream %d ended with the end event", i)
			require.Contains(t, body, `"reason":"server_shutdown"`)
			require.Contains(t, body, `"operationId":"op_drain"`)
		case <-time.After(5 * time.Second):
			t.Fatalf("stream %d was not closed", i)
		}
	}
	require.NoError(t, <-srv.Served(), "the listener stopped by Stop")
	t.Logf("P1.7 AC3 (transport): 3 open streams ended with the end event; drain took %s of its %s bound", took.Round(time.Millisecond), drain)
}
