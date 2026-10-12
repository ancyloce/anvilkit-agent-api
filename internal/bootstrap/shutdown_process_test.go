package bootstrap

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// operationsFake is the Control the drain test needs: one operation, an
// empty event page and a stream that stays open until the API ends it.
type operationsFake struct {
	controlv1.UnimplementedOperationServiceServer
}

func (operationsFake) GetOperation(_ context.Context, r *controlv1.GetOperationRequest) (*controlv1.GetOperationResponse, error) {
	return &controlv1.GetOperationResponse{Operation: &controlv1.OperationView{OperationId: r.GetOperationId(), TenantId: "tenant_a", ProjectId: "proj_a", Revision: "1"}}, nil
}

func (operationsFake) ListOperationEvents(context.Context, *controlv1.ListOperationEventsRequest) (*controlv1.ListOperationEventsResponse, error) {
	return &controlv1.ListOperationEventsResponse{}, nil
}

func (operationsFake) StreamOperationEvents(_ *controlv1.StreamOperationEventsRequest, s grpc.ServerStreamingServer[controlv1.StreamOperationEventsResponse]) error {
	<-s.Context().Done()
	return s.Context().Err()
}

// TestProcessDrainsOpenStreamsAndExitsZero is P1.7 AC3: the API process,
// terminated with three event streams open, withdraws readiness, ends
// every stream with a final `end` event, completes within its configured
// drain, flushes its traces to the collector and exits 0.
func TestProcessDrainsOpenStreamsAndExitsZero(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and runs the API binary")
	}
	listen := func() net.Listener {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		return l
	}
	controlLis := listen()
	gs := grpc.NewServer()
	controlv1.RegisterOperationServiceServer(gs, operationsFake{})
	hs := health.NewServer()
	hs.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(gs, hs)
	go func() { _ = gs.Serve(controlLis) }()
	defer gs.Stop()
	collectorLis := listen()
	cs := grpc.NewServer()
	collector := &traceCollector{}
	coltracepb.RegisterTraceServiceServer(cs, collector)
	go func() { _ = cs.Serve(collectorLis) }()
	defer cs.Stop()
	apiLis := listen()
	apiAddr := apiLis.Addr().String()
	require.NoError(t, apiLis.Close())

	dir := t.TempDir()
	bin := filepath.Join(dir, "anvilkit-agent-api")
	build := exec.Command("go", "build", "-o", bin, "./cmd/anvilkit-agent-api")
	build.Dir = filepath.Join("..", "..")
	build.Env = append(os.Environ(), "GOWORK=off")
	out, err := build.CombinedOutput()
	require.NoError(t, err, "%s", out)
	const token = "token-drain-0123456789abcdef"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "principals.json"), []byte(fmt.Sprintf(`{%q:{"tenantId":"tenant_a","projectId":"proj_a","actorId":"user_a","roles":["operator"]}}`, token)), 0o600))
	const drain = 5 * time.Second
	cfg := fmt.Sprintf(`development:
  enabled: true
http:
  listen: %s
  shutdown_timeout: %s
control:
  address: %s
  identity:
    mode: development
auth:
  mode: fixture
  principals_file: %s
telemetry:
  otlp_endpoint: %s
  sample_ratio: 1
  otlp_tls:
    mode: development
`, apiAddr, drain, controlLis.Addr(), filepath.Join(dir, "principals.json"), collectorLis.Addr())
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.yaml"), []byte(cfg), 0o600))
	var logs bytes.Buffer
	proc := exec.Command(bin)
	proc.Env = append(os.Environ(), "ANVILKIT_API_CONFIG="+filepath.Join(dir, "config.yaml"))
	proc.Stdout, proc.Stderr = &logs, &logs
	require.NoError(t, proc.Start())
	defer func() { _ = proc.Process.Kill() }()
	base := "http://" + apiAddr
	require.Eventually(t, func() bool {
		resp, err := http.Get(base + "/readyz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusNoContent
	}, 30*time.Second, 100*time.Millisecond, "the API is ready: %s", &logs)

	bodies := make([]chan string, 3)
	for i := range bodies {
		req, err := http.NewRequest("GET", base+"/api/v1/operations/op_drain/events", nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		require.Equal(t, 200, resp.StatusCode, "%s", &logs)
		r := bufio.NewReader(resp.Body)
		line, err := r.ReadString('\n')
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(line, "retry: "))
		bodies[i] = make(chan string, 1)
		go func(out chan<- string) {
			defer resp.Body.Close()
			rest, _ := io.ReadAll(r)
			out <- string(rest)
		}(bodies[i])
	}
	started := time.Now()
	require.NoError(t, proc.Process.Signal(syscall.SIGTERM))
	waited := make(chan error, 1)
	go func() { waited <- proc.Wait() }()
	select {
	case err := <-waited:
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			t.Fatalf("the API exited %d: %s", exit.ExitCode(), &logs)
		}
		require.NoError(t, err)
	case <-time.After(drain + 10*time.Second):
		t.Fatalf("the API did not stop: %s", &logs)
	}
	took := time.Since(started)
	require.Less(t, took, drain, "the stop completes within the configured drain")
	require.Equal(t, 0, proc.ProcessState.ExitCode())
	for i, b := range bodies {
		select {
		case body := <-b:
			require.Contains(t, body, "event:end\n", "stream %d", i)
			require.Contains(t, body, `"reason":"server_shutdown"`)
		case <-time.After(5 * time.Second):
			t.Fatalf("stream %d was not closed", i)
		}
	}
	require.Greater(t, collector.spans.Load(), int32(0), "the traces were flushed to the collector at stop")
	t.Logf("P1.7 AC3: SIGTERM with 3 open streams: each ended with the end event, stopped in %s (drain %s), exit 0, %d spans flushed", took.Round(time.Millisecond), drain, collector.spans.Load())
}
