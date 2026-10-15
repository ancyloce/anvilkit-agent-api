package http

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/ancyloce/anvilkit-agent-api/internal/requestid"
)

// P3.6 AC3 (API-09): the requestId of an error response is in the API's log
// line, on the API's server span, and reaches the owner as gRPC metadata on
// a server span of the same trace (the owners set it on their spans from
// that metadata: Control's and MCP's requestid interceptors).
func TestRequestIDReachesLogSpanAndOwner(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prev := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(prev) })

	// The owner stand-in: a gRPC server with the owners' stats handler that
	// records the metadata and span context each call arrives with.
	type arrival struct {
		requestID string
		traceID   trace.TraceID
	}
	arrivals := make(chan arrival, 1)
	lis := bufconn.Listen(1 << 20)
	owner := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(tp))),
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			var id string
			if v := md.Get(requestid.MetadataKey); len(v) == 1 {
				id = v[0]
			}
			arrivals <- arrival{requestID: id, traceID: trace.SpanContextFromContext(ctx).TraceID()}
			return handler(ctx, req)
		}))
	grpc_health_v1.RegisterHealthServer(owner, health.NewServer())
	go func() { _ = owner.Serve(lis) }()
	t.Cleanup(owner.Stop)
	conn, err := grpc.NewClient("passthrough:///owner", grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(tp))),
		grpc.WithChainUnaryInterceptor(requestid.UnaryClient()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	var logged bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logged, nil))
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(observe(tp.Tracer("api"), nil), requestID(), logRequests(log), errorEnvelope())
	engine.GET("/api/v1/operations/:operationId", func(c *gin.Context) {
		// The handler calls the owner with the request's context, then fails.
		_, err := grpc_health_v1.NewHealthClient(conn).Check(c.Request.Context(), &grpc_health_v1.HealthCheckRequest{})
		require.NoError(t, err)
		fail(c, &apiError{status: http.StatusServiceUnavailable, code: "DEPENDENCY_UNAVAILABLE", message: "owner unavailable", retryable: true})
	})

	w := httptest.NewRecorder()
	engine.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/operations/op_1", nil))
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	var body struct {
		Error struct {
			RequestID string `json:"requestId"`
		} `json:"error"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	id := body.Error.RequestID
	require.NotEmpty(t, id)
	require.Equal(t, id, w.Header().Get("X-Request-Id"))

	// The log line.
	var line map[string]any
	require.NoError(t, json.Unmarshal(logged.Bytes(), &line))
	require.Equal(t, "request failed", line["msg"])
	require.Equal(t, id, line["requestId"])
	require.Equal(t, "DEPENDENCY_UNAVAILABLE", line["errorCode"])
	require.Equal(t, "/api/v1/operations/:operationId", line["route"], "the route template, never the path value")
	require.NotContains(t, logged.String(), "op_1")

	// The API's server span.
	var server sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.SpanKind() == trace.SpanKindServer && s.Name() == "GET /api/v1/operations/:operationId" {
			server = s
		}
	}
	require.NotNil(t, server)
	attrs := map[string]string{}
	for _, a := range server.Attributes() {
		attrs[string(a.Key)] = a.Value.Emit()
	}
	require.Equal(t, id, attrs[requestid.SpanAttribute])

	// The owner: the identifier as metadata, its server span in the same trace.
	got := <-arrivals
	require.Equal(t, id, got.requestID)
	require.Equal(t, server.SpanContext().TraceID(), got.traceID)
}
