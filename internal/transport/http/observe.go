package http

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

// Metrics are the API's request metrics: counts and durations by method,
// route template and status. Labels never carry a path value, query,
// header, token or identifier from the request.
type Metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

// NewMetrics registers the request metrics on reg.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{Name: "anvilkit_api_requests_total", Help: "HTTP requests by method, route template and status."},
			[]string{"method", "route", "code"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: "anvilkit_api_request_duration_seconds", Help: "HTTP request duration by method and route template.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 14)}, []string{"method", "route"}),
	}
	reg.MustRegister(m.requests, m.duration)
	return m
}

// observe gives every request one server span and its metrics with
// controlled attributes only: the method, the route template and the status
// (security.md: no bodies, headers, tokens, query values or caller
// identifiers). The API is the trust boundary, so a caller's traceparent is
// never adopted: each request starts a new trace, which the Control client
// then propagates inward.
func observe(tracer trace.Tracer, m *Metrics) gin.HandlerFunc {
	if tracer == nil {
		tracer = noop.NewTracerProvider().Tracer("")
	}
	return func(c *gin.Context) {
		start := time.Now()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		method := c.Request.Method
		ctx, span := tracer.Start(c.Request.Context(), method+" "+route, trace.WithNewRoot(), trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.request.method", method), attribute.String("http.route", route)))
		c.Request = c.Request.WithContext(ctx)
		c.Next()
		status := c.Writer.Status()
		span.SetAttributes(attribute.Int("http.response.status_code", status))
		if status >= http.StatusInternalServerError {
			span.SetStatus(codes.Error, http.StatusText(status))
		}
		span.End()
		if m != nil {
			m.requests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
			m.duration.WithLabelValues(method, route).Observe(time.Since(start).Seconds())
		}
	}
}
