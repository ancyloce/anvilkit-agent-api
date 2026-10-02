package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// A request's span and metrics carry the method, the route template and the
// status only; the caller's traceparent, path values, query, headers and
// token never reach them.
func TestObserveRecordsControlledAttributesOnly(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tracer := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec)).Tracer("test")
	reg := prometheus.NewRegistry()
	m := NewMetrics(reg)
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Use(observe(tracer, m))
	engine.GET("/api/v1/operations/:operationId", func(c *gin.Context) { c.Status(http.StatusNotFound) })

	req := httptest.NewRequest(http.MethodGet, "/api/v1/operations/op-secret-123?token=leak", nil)
	req.Header.Set("Authorization", "Bearer secret-token-value")
	req.Header.Set("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	engine.ServeHTTP(httptest.NewRecorder(), req)

	spans := rec.Ended()
	require.Len(t, spans, 1)
	s := spans[0]
	require.Equal(t, "GET /api/v1/operations/:operationId", s.Name())
	require.False(t, s.Parent().IsValid(), "a caller's traceparent is never adopted")
	require.NotEqual(t, "0af7651916cd43dd8448eb211c80319c", s.SpanContext().TraceID().String())
	keys := map[string]string{}
	for _, a := range s.Attributes() {
		keys[string(a.Key)] = a.Value.Emit()
	}
	require.Equal(t, map[string]string{"http.request.method": "GET", "http.route": "/api/v1/operations/:operationId", "http.response.status_code": "404"}, keys)
	require.Equal(t, 1.0, testutil.ToFloat64(m.requests.WithLabelValues("GET", "/api/v1/operations/:operationId", "404")))
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, f := range families {
		for _, metric := range f.GetMetric() {
			for _, l := range metric.GetLabel() {
				require.NotContains(t, l.GetValue(), "op-secret-123")
				require.NotContains(t, l.GetValue(), "leak")
				require.NotContains(t, l.GetValue(), "secret-token-value")
			}
		}
	}
}
