package http_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	httptransport "github.com/ancyloce/anvilkit-agent-api/internal/transport/http"
)

// blockingVerifier holds a request inside the server: it says when the
// request arrived and answers only once released.
type blockingVerifier struct{ arrived, release chan struct{} }

func (v blockingVerifier) Verify(ctx context.Context, bearer string) (application.Principal, error) {
	close(v.arrived)
	<-v.release
	return verifier{}.Verify(ctx, bearer)
}

// TestStopForcesTheConnectionsTheDrainLeaves (RB-A25, DD-09 §3): a request
// still in its handler when the drain bound passes is closed, Stop reports
// the forced stop without an error, and the lifecycle gauges record it next
// to the one active configuration generation.
func TestStopForcesTheConnectionsTheDrainLeaves(t *testing.T) {
	fake := newFake()
	reg := prometheus.NewRegistry()
	metrics := httptransport.NewMetrics(reg)
	opts := testOptions()
	opts.Metrics = metrics
	v := blockingVerifier{arrived: make(chan struct{}), release: make(chan struct{})}
	srv, err := httptransport.NewServer(opts, v, fake, httptransport.Gateways{}, func(context.Context) error { return nil })
	require.NoError(t, err)
	addr, err := srv.Start()
	require.NoError(t, err)

	answered := make(chan error, 1)
	go func() {
		req, _ := http.NewRequest("GET", "http://"+addr.String()+"/api/v1/operations/op_blocked", nil)
		req.Header.Set("Authorization", "Bearer token-tenant-a-0123456789")
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		answered <- err
	}()
	select {
	case <-v.arrived:
	case <-time.After(5 * time.Second):
		t.Fatal("the request never reached the server")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	begin := time.Now()
	forced, err := srv.Stop(ctx)
	require.NoError(t, err, "a forced stop is no failed stop")
	require.True(t, forced, "the blocked request outlived the drain bound")
	metrics.Stopped(time.Since(begin), forced)
	select {
	case err := <-answered:
		require.Error(t, err, "the client sees its connection closed")
	case <-time.After(5 * time.Second):
		t.Fatal("the blocked request's connection was not closed")
	}
	close(v.release)

	families, err := reg.Gather()
	require.NoError(t, err)
	got := map[string]float64{}
	for _, f := range families {
		if m := f.GetMetric(); len(m) == 1 && m[0].GetGauge() != nil {
			got[f.GetName()] = m[0].GetGauge().GetValue()
		}
	}
	require.Equal(t, 1.0, got["anvilkit_api_config_generation"])
	require.Equal(t, 1.0, got["anvilkit_api_shutdown_forced"])
	require.GreaterOrEqual(t, got["anvilkit_api_shutdown_drain_seconds"], 0.3)
}
