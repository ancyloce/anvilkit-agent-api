// Package http is the API's Gin transport (A01/A02): authentication, strict
// JSON parsing, kin-openapi request validation against the embedded
// generated spec, the strict generated handlers, the hand-registered SSE
// route and the public error envelope.
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/gin-gonic/gin"
	ginmiddleware "github.com/oapi-codegen/gin-middleware"

	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

const (
	ctxPrincipal = "anvilkit.principal"
	ctxRequestID = "anvilkit.requestId"
	ctxDigest    = "anvilkit.requestDigest"
)

// Options is the validated transport configuration injected by the
// bootstrap; nothing in this package reads the environment or a mutable
// global.
type Options struct {
	Listen            string
	ReadHeaderTimeout time.Duration
	BodyLimitBytes    int64
	Stream            StreamBounds
}

// StreamBounds limits the SSE transport (contracts.md §6 "bounded
// slow-consumer buffers disconnect safely without canceling the business
// operation"). They are transport limits, not business clocks.
type StreamBounds struct {
	// HeartbeatInterval keeps intermediaries from closing an idle stream;
	// the comment frame carries no business meaning.
	HeartbeatInterval time.Duration
	// FrameBuffer is the number of encoded frames held for a consumer that
	// reads slower than Control emits.
	FrameBuffer int
	// SlowConsumerGrace is how long a full buffer is tolerated before the
	// consumer is disconnected; durable events are replayed on reconnect.
	SlowConsumerGrace time.Duration
	// WriteTimeout bounds every write to the connection so a stalled peer
	// cannot hold the handler forever.
	WriteTimeout time.Duration
}

// Server owns the Gin engine and the http.Server.
type Server struct {
	engine *gin.Engine
	http   *http.Server
	served chan error
}

func NewServer(opts Options, verifier application.Verifier, control application.Control, ready func(context.Context) error) (*Server, error) {
	spec, err := agentapi.GetSwagger()
	if err != nil {
		return nil, err
	}
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	// Errors are collected with c.Error and rendered once by errorEnvelope,
	// which is registered first so it runs last; a panic becomes the same
	// envelope through Gin's recovery.
	engine.Use(requestID(), errorEnvelope(), gin.CustomRecoveryWithWriter(io.Discard, func(c *gin.Context, _ any) {
		fail(c, &apiError{status: http.StatusServiceUnavailable, code: "DEPENDENCY_UNAVAILABLE", message: "internal failure", retryable: true})
	}))

	engine.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	engine.GET("/readyz", func(c *gin.Context) {
		if err := ready(c.Request.Context()); err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		c.Status(http.StatusNoContent)
	})

	api := engine.Group("/api/v1", authenticate(verifier))
	// SSE is excluded from the generated strict server because it streams;
	// its cursor parameters are bound with Gin's ShouldBind* against the
	// same canonical-sequence rule and its frames come from gin-contrib/sse.
	api.GET("/operations/:operationId/events", streamEvents(control, opts.Stream))

	validator := ginmiddleware.OapiRequestValidatorWithOptions(spec, &ginmiddleware.Options{
		Options: openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc},
		ErrorHandler: func(c *gin.Context, message string, statusCode int) {
			fail(c, invalidArgument(message))
		},
	})
	handlers := agentapi.NewStrictHandlerWithOptions(&strictHandlers{control: control}, nil, agentapi.StrictGinServerOptions{
		RequestErrorHandlerFunc:  func(c *gin.Context, err error) { fail(c, invalidArgument(err.Error())) },
		HandlerErrorFunc:         func(c *gin.Context, err error) { fail(c, fromControl(err)) },
		ResponseErrorHandlerFunc: func(c *gin.Context, err error) { fail(c, fromControl(err)) },
	})
	agentapi.RegisterHandlersWithOptions(api, handlers, agentapi.GinServerOptions{
		Middlewares: []agentapi.MiddlewareFunc{agentapi.MiddlewareFunc(strictBody(opts.BodyLimitBytes)), agentapi.MiddlewareFunc(validator)},
	})

	return &Server{engine: engine, http: &http.Server{Addr: opts.Listen, Handler: engine, ReadHeaderTimeout: opts.ReadHeaderTimeout}}, nil
}

// Handler exposes the engine for in-process tests.
func (s *Server) Handler() http.Handler { return s.engine }

// Start binds the configured address before it returns, so an occupied port
// or an invalid address is a startup error rather than a background failure
// after a reported start, then serves on that listener in the background.
// The bound address is returned (the configured port may be 0).
func (s *Server) Start() (net.Addr, error) {
	ln, err := net.Listen("tcp", s.http.Addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", s.http.Addr, err)
	}
	served := make(chan error, 1)
	s.served = served
	go func() {
		err := s.http.Serve(ln)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil // Stop closed the listener
		}
		served <- err
	}()
	return ln.Addr(), nil
}

// Served yields the outcome of the started listener once it stops: nil
// after Stop, otherwise the unexpected Serve error, which the bootstrap
// reports instead of discarding.
func (s *Server) Served() <-chan error { return s.served }

func (s *Server) Stop(ctx context.Context) error { return s.http.Shutdown(ctx) }

// ---- middleware ----

func requestID() gin.HandlerFunc {
	return func(c *gin.Context) {
		id := newRequestID()
		c.Set(ctxRequestID, id)
		c.Header("X-Request-Id", id)
		c.Next()
	}
}

func authenticate(v application.Verifier) gin.HandlerFunc {
	return func(c *gin.Context) {
		h := c.GetHeader("Authorization")
		if !strings.HasPrefix(h, "Bearer ") || len(h) <= 7 {
			fail(c, &apiError{status: http.StatusUnauthorized, code: "UNAUTHENTICATED", message: "bearer token required"})
			return
		}
		p, err := v.Verify(c.Request.Context(), strings.TrimPrefix(h, "Bearer "))
		if err != nil {
			fail(c, &apiError{status: http.StatusUnauthorized, code: "UNAUTHENTICATED", message: "token not accepted"})
			return
		}
		c.Set(ctxPrincipal, p)
		c.Next()
	}
}

// strictBody bounds the body, rejects duplicate members, trailing values and
// non-finite numbers before validation, and records the canonical digest
// used as the command's request digest (contracts.md §4). kin-openapi does
// not enforce these rules, so this thin adapter stays.
func strictBody(limit int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.Body == nil || c.Request.ContentLength == 0 {
			c.Next()
			return
		}
		raw, err := io.ReadAll(io.LimitReader(c.Request.Body, limit+1))
		if err != nil {
			fail(c, invalidArgument("unreadable body"))
			return
		}
		if int64(len(raw)) > limit {
			fail(c, invalidArgument("body exceeds the size bound"))
			return
		}
		decoded, err := strictDecode(raw)
		if err != nil {
			fail(c, invalidArgument(err.Error()))
			return
		}
		digest, err := application.CanonicalDigest(decoded)
		if err != nil {
			fail(c, invalidArgument("body cannot be canonicalized"))
			return
		}
		c.Set(ctxDigest, digest)
		c.Request.Body = io.NopCloser(bytes.NewReader(raw))
		c.Next()
	}
}

// strictDecode parses JSON with duplicate-member, trailing-value and
// non-finite rejection; numbers stay json.Number so nothing is rounded.
func strictDecode(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	v, err := decodeValue(dec)
	if err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid JSON: trailing data after the document")
	}
	return v, nil
}

func decodeValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			obj := map[string]any{}
			for dec.More() {
				keyTok, err := dec.Token()
				if err != nil {
					return nil, err
				}
				key, _ := keyTok.(string)
				if _, dup := obj[key]; dup {
					return nil, fmt.Errorf("duplicate member %q", key)
				}
				val, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				obj[key] = val
			}
			_, err := dec.Token()
			return obj, err
		case '[':
			arr := []any{}
			for dec.More() {
				val, err := decodeValue(dec)
				if err != nil {
					return nil, err
				}
				arr = append(arr, val)
			}
			_, err := dec.Token()
			return arr, err
		}
		return nil, fmt.Errorf("unexpected delimiter %v", t)
	default:
		return t, nil
	}
}

func principal(c *gin.Context) application.Principal {
	p, _ := c.Get(ctxPrincipal)
	pr, _ := p.(application.Principal)
	return pr
}

func requestIDOf(c *gin.Context) string {
	v, _ := c.Get(ctxRequestID)
	s, _ := v.(string)
	return s
}

// ---- error envelope ----

// apiError is the public error decided for a request. Handlers and
// middleware attach it with fail; errorEnvelope renders it.
type apiError struct {
	status    int
	code      string
	message   string
	retryable bool
}

func (e *apiError) Error() string { return e.code + ": " + e.message }

func invalidArgument(message string) *apiError {
	return &apiError{status: http.StatusBadRequest, code: "INVALID_ARGUMENT", message: message}
}

// fail records the error on the context (Gin's error collection) and stops
// the chain; the envelope is written once by errorEnvelope.
func fail(c *gin.Context, err *apiError) {
	_ = c.Error(err)
	c.Abort()
}

// errorEnvelope renders the first recorded apiError as the public envelope
// when no handler has written a response.
func errorEnvelope() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Next()
		if c.Writer.Written() || len(c.Errors) == 0 {
			return
		}
		var ae *apiError
		if !errors.As(c.Errors[0].Err, &ae) {
			ae = &apiError{status: http.StatusServiceUnavailable, code: "DEPENDENCY_UNAVAILABLE", message: "dependency unavailable", retryable: true}
		}
		c.JSON(ae.status, agentapi.ErrorEnvelope{Error: struct {
			Code    agentapi.ErrorCode `json:"code"`
			Details *struct {
				CoveredEventSeq  *agentapi.Sequence `json:"coveredEventSeq,omitempty"`
				CurrentRevision  *agentapi.Revision `json:"currentRevision,omitempty"`
				ExpectedRevision *agentapi.Revision `json:"expectedRevision,omitempty"`
				Field            *string            `json:"field,omitempty"`
			} `json:"details,omitempty"`
			Message   string      `json:"message"`
			RequestId agentapi.Id `json:"requestId"`
			Retryable bool        `json:"retryable"`
		}{Code: agentapi.ErrorCode(ae.code), Message: truncate(ae.message, 512), RequestId: requestIDOf(c), Retryable: ae.retryable}})
	}
}

var httpStatus = map[string]int{
	"INVALID_ARGUMENT": 400, "UNAUTHENTICATED": 401, "FORBIDDEN": 403, "NOT_FOUND": 404,
	"REVISION_CONFLICT": 409, "IDEMPOTENCY_CONFLICT": 409, "STALE_EXECUTION": 409, "PROFILE_UNQUALIFIED": 409,
	"CAPACITY_EXHAUSTED": 429, "BUDGET_EXHAUSTED": 429, "EFFECT_UNCERTAIN": 503, "DEPENDENCY_UNAVAILABLE": 503,
}

// fromControl maps a Control-decided error to the public envelope
// (contracts.md §4).
func fromControl(err error) *apiError {
	var ce *application.ControlError
	if errors.As(err, &ce) {
		status, ok := httpStatus[ce.Code]
		if !ok {
			status = http.StatusServiceUnavailable
		}
		return &apiError{status: status, code: ce.Code, message: ce.Message, retryable: ce.Retryable}
	}
	if errors.Is(err, application.ErrForbidden) {
		return &apiError{status: http.StatusForbidden, code: "FORBIDDEN", message: "not permitted"}
	}
	return &apiError{status: http.StatusServiceUnavailable, code: "DEPENDENCY_UNAVAILABLE", message: "dependency unavailable", retryable: true}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
