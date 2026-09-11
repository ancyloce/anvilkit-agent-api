// Package httpapi is the public HTTP boundary of anvilkit-agent-api and the
// forwarding of permitted public operations to anvilkit-agent-control.
//
// The boundary owns request identity, body ceilings, strict public-value
// decoding and the shared error envelope. It owns no business decision: Control
// canonicalizes definitions, decides semantic validity and remains the sole
// authority for anything an accepted request would change.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"

	definitionvalidationv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/definitionvalidationv1"
)

// DefinitionValidator is the narrow slice of Control this service calls. The
// generated Connect client satisfies it directly, so no adapter stands between
// the contract and its consumer.
type DefinitionValidator interface {
	ValidateDefinition(
		context.Context,
		*connect.Request[definitionvalidationv1.ValidateDefinitionRequest],
	) (*connect.Response[definitionvalidationv1.ValidateDefinitionResponse], error)
}

// Dependencies are the collaborators one API process serves with. They are
// named rather than positional because a boundary that forwards commands,
// checks disclosure and reads projections has several, and a caller that mixes
// two of them up must not compile.
type Dependencies struct {
	Logger *slog.Logger
	// Identities resolves trusted callers. A nil profile authorizes nothing,
	// which is the correct behaviour for a deployment with no configured
	// profile.
	Identities *identity.Profile
	// Validator forwards definition validation to Control.
	Validator DefinitionValidator
	// Disclosure obtains expiring authorization evidence from Control.
	Disclosure DisclosureAuthorizer
	// Commands forwards public commands to Control.
	Commands OperationCommander
	// ServesLocalChecks registers POST /v1/local-checks. It comes from the
	// resolved serving profile, so the route is absent in every deployment that
	// did not explicitly select the controlled local profile.
	ServesLocalChecks bool
	// ReadModel serves committed projections and durable events.
	ReadModel ProjectionReader
	// ControlCallTimeout bounds one private call attempt.
	ControlCallTimeout time.Duration
}

// Server serves the public and private listeners of one API process.
type Server struct {
	logger             *slog.Logger
	identities         *identity.Profile
	control            DefinitionValidator
	disclosure         DisclosureAuthorizer
	commands           OperationCommander
	readModel          ProjectionReader
	servesLocalChecks  bool
	hub                *streamHub
	controlCallTimeout time.Duration
	draining           atomic.Bool
	// readRoleSeenAt is the last instant the projection read role answered.
	// Readiness depends on it, and nothing else does.
	readRoleSeenAt atomic.Int64
	readRoleWasOK  atomic.Bool
}

// NewServer builds the boundary.
func NewServer(dependencies Dependencies) *Server {
	identities := dependencies.Identities
	if identities == nil {
		identities = identity.EmptyProfile()
	}
	server := &Server{
		logger:             dependencies.Logger,
		identities:         identities,
		control:            dependencies.Validator,
		disclosure:         dependencies.Disclosure,
		commands:           dependencies.Commands,
		readModel:          dependencies.ReadModel,
		servesLocalChecks:  dependencies.ServesLocalChecks,
		controlCallTimeout: dependencies.ControlCallTimeout,
	}
	if dependencies.ReadModel != nil {
		server.hub = newStreamHub(dependencies.ReadModel)
	}
	return server
}

// Hub exposes the shared catch-up so the process can run it and wake it from
// the database listener.
func (s *Server) Hub() *streamHub { return s.hub }

// PublicHandler serves the tenant-facing routes. Only declared operations exist:
// there is no generic Control passthrough, and any other path or method is
// answered as an absent operation rather than by describing the surface.
func (s *Server) PublicHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+routeDefinitionValidations, s.handleValidateDefinition)
	mux.HandleFunc(routeDefinitionValidations, s.handleUndeclaredOperation)
	mux.HandleFunc("GET "+routeOperation, s.handleReadOperation)
	mux.HandleFunc(routeOperation, s.handleUndeclaredOperation)
	mux.HandleFunc("GET "+routeOperationSnapshot, s.handleReadSnapshot)
	mux.HandleFunc(routeOperationSnapshot, s.handleUndeclaredOperation)
	mux.HandleFunc("GET "+routeOperationEvents, s.handleReadEvents)
	mux.HandleFunc(routeOperationEvents, s.handleUndeclaredOperation)
	mux.HandleFunc("POST "+routeOperationCancel, s.handleCancelOperation)
	mux.HandleFunc(routeOperationCancel, s.handleUndeclaredOperation)
	// The local-check route is registered only in the controlled local profile.
	// Elsewhere nothing is registered for its path, so it is absent exactly like
	// any other undeclared operation rather than present and denied.
	if s.servesLocalChecks {
		mux.HandleFunc("POST "+routeLocalChecks, s.handleSubmitLocalCheck)
		mux.HandleFunc(routeLocalChecks, s.handleUndeclaredOperation)
		for _, route := range []string{routeOperationHold, routeOperationResume} {
			mux.HandleFunc("POST "+route, s.handleUnsupportedLocalControlCommand)
			mux.HandleFunc(route, s.handleUndeclaredOperation)
		}
		mux.HandleFunc("POST "+routeDefinitionChanges, s.handleUnsupportedLocalControlCommand)
		mux.HandleFunc("GET "+routeDefinitionChanges, s.handleUndeclaredOperation)
	}
	mux.HandleFunc("/", s.handleUndeclaredOperation)
	return s.withRequestBoundary(mux)
}

// PrivateHandler serves liveness and readiness. The architecture keeps these off
// the public listener, so they are never reachable from a browser origin.
func (s *Server) PrivateHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		// Liveness tests process responsiveness only.
		writeJSON(w, http.StatusOK, map[string]string{"status": "alive"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		// Readiness reports only whether this replica can serve its own
		// responsibility: not shutting down, and the projection read role
		// answering recently. It never reports whether any particular request
		// is authorized, and a Control outage is not a readiness input.
		switch {
		case s.draining.Load():
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		case !s.readRoleFresh(time.Now()):
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "read-role-unavailable"})
		default:
			writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
		}
	})
	return mux
}

func (s *Server) handleUndeclaredOperation(w http.ResponseWriter, r *http.Request) {
	s.writeError(w, r, newFault(codeNotFound, "no public operation is declared at this path and method"))
}

// BeginDrain refuses new work and turns readiness false. In-flight requests
// finish inside the caller's grace window, and live subscriptions close with a
// randomized reconnect hint.
func (s *Server) BeginDrain() {
	s.draining.Store(true)
}

// WatchReadRole records whether the projection read role is reachable, so
// readiness reflects an observation rather than a probe run per health check.
// It returns when ctx ends.
func (s *Server) WatchReadRole(ctx context.Context) {
	const probeInterval = 3 * time.Second

	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()
	for {
		s.probeReadRole(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Server) probeReadRole(ctx context.Context) {
	if s.readModel == nil {
		return
	}
	probe, cancel := context.WithTimeout(ctx, config.ReadinessReadRoleWindow)
	defer cancel()

	err := s.readModel.Ping(probe)
	if err == nil {
		s.readRoleSeenAt.Store(time.Now().UnixNano())
	}
	if healthy := err == nil; healthy != s.readRoleWasOK.Swap(healthy) {
		attributes := []slog.Attr{
			slog.String("eventName", logging.EventHealthTransition),
			slog.String("outcome", map[bool]string{true: "ok", false: "unavailable"}[healthy]),
			slog.Group("attributes", slog.String("dependency", "projection-read-role")),
		}
		if !healthy {
			attributes = append(attributes, slog.String("error.type", "unavailable"), slog.String("error.code", codeDependencyUnavailable))
		}
		s.logger.LogAttrs(ctx, slog.LevelWarn, "", attributes...)
	}
}

// readRoleFresh reports whether the read role answered inside the contract's
// readiness window.
func (s *Server) readRoleFresh(now time.Time) bool {
	if s.readModel == nil {
		return true
	}
	seenAt := s.readRoleSeenAt.Load()
	return seenAt != 0 && now.Sub(time.Unix(0, seenAt)) <= config.ReadinessReadRoleWindow
}

// exchange carries the per-request facts the completion record needs. Only
// identifiers and outcomes are kept; no header, body or credential is retained.
type exchange struct {
	mu          sync.Mutex
	requestID   string
	traceSource string
	linkTraceID string
	actorID     string
	tenantID    string
	operationID string
	status      int
	errorCode   string
	errorType   string
}

type exchangeKey struct{}

func exchangeFrom(ctx context.Context) *exchange {
	current, _ := ctx.Value(exchangeKey{}).(*exchange)
	return current
}

func requestIDFrom(ctx context.Context) string {
	if current := exchangeFrom(ctx); current != nil {
		return current.requestID
	}
	return ""
}

// operationIDFrom returns the operation this request is about, once a handler
// has resolved one. An error envelope names it so a caller and the completion
// record agree about which operation failed.
func operationIDFrom(ctx context.Context) string {
	current := exchangeFrom(ctx)
	if current == nil {
		return ""
	}
	current.mu.Lock()
	defer current.mu.Unlock()
	return current.operationID
}

func recordActor(ctx context.Context, actor identity.Actor) {
	if current := exchangeFrom(ctx); current != nil {
		current.mu.Lock()
		current.actorID, current.tenantID = actor.ActorID, actor.TenantID
		current.mu.Unlock()
	}
}

// recordOperation binds the completion record to the operation the request is
// about, which the logging contract requires on every such record.
func recordOperation(ctx context.Context, operationID string) {
	if current := exchangeFrom(ctx); current != nil {
		current.mu.Lock()
		current.operationID = operationID
		current.mu.Unlock()
	}
}

func recordOutcome(ctx context.Context, status int, errorCode, errorType string) {
	if current := exchangeFrom(ctx); current != nil {
		current.mu.Lock()
		current.status, current.errorCode, current.errorType = status, errorCode, errorType
		current.mu.Unlock()
	}
}

// withRequestBoundary assigns the server-owned request identity, refuses work
// while draining, contains panics and emits exactly one completion record per
// executed request.
func (s *Server) withRequestBoundary(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The request identifier is always server-assigned. A client-supplied
		// identifier is diagnostic input at best and is never adopted here.
		traceSource, linkTraceID := classifyTraceInput(r.Header.Get("traceparent"))
		current := &exchange{
			requestID:   newRequestID(),
			traceSource: traceSource,
			linkTraceID: linkTraceID,
		}
		ctx := context.WithValue(r.Context(), exchangeKey{}, current)
		r = r.WithContext(ctx)
		w.Header().Set("X-Request-Id", current.requestID)

		started := time.Now()
		defer func() {
			if recovered := recover(); recovered != nil {
				recordOutcome(ctx, http.StatusServiceUnavailable, codeDependencyUnavailable, "internal")
				writeJSON(w, http.StatusServiceUnavailable, errorEnvelope{
					Code:      codeDependencyUnavailable,
					Message:   "the request could not be completed",
					Retryable: true,
					RequestID: current.requestID,
				})
			}
			s.logCompletion(ctx, current, r, time.Since(started))
		}()

		if s.draining.Load() {
			s.writeError(w, r, newFault(codeDependencyUnavailable, "this replica is draining; reconnect to retry"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) logCompletion(ctx context.Context, current *exchange, r *http.Request, elapsed time.Duration) {
	current.mu.Lock()
	status, errorCode, errorType := current.status, current.errorCode, current.errorType
	actorID, tenantID, operationID := current.actorID, current.tenantID, current.operationID
	traceSource, linkTraceID := current.traceSource, current.linkTraceID
	current.mu.Unlock()

	if status == 0 {
		status = http.StatusOK
	}

	attributes := []slog.Attr{
		slog.String("eventName", logging.EventRPCServerCompleted),
		slog.String("requestId", current.requestID),
		slog.String("protocol", "http"),
		slog.String("caller", callerPeerFor(errorCode)),
		slog.String("callee", logging.ServiceName),
		slog.String("routeTemplate", routeTemplateFor(r)),
		slog.String("outcome", outcomeForStatus(status)),
		slog.Int64("durationMs", logging.DurationMs(elapsed)),
		slog.Int("http.status", status),
		slog.String("trace.source", traceSource),
	}
	if linkTraceID != "" {
		attributes = append(attributes, slog.String("link.traceId", linkTraceID))
	}
	if actorID != "" {
		attributes = append(attributes, slog.String("actorId", actorID), slog.String("tenantId", tenantID))
	}
	if operationID != "" {
		attributes = append(attributes, slog.String("operationId", operationID))
	}
	severity := slog.LevelInfo
	if errorCode != "" {
		severity = slog.LevelWarn
		attributes = append(attributes,
			slog.String("error.code", errorCode),
			slog.String("error.type", errorType),
			slog.Bool("error.retryable", errorFamilies[errorCode].retryable),
		)
	}
	s.logger.LogAttrs(ctx, severity, "", attributes...)
}

// classifyTraceInput records what became of a caller's W3C trace context.
//
// Public trace input is never adopted as this request's trace and never confers
// identity: a well-formed context is kept only as a link, and anything
// malformed is recorded as rejected. Either way the request starts a new trace,
// so a caller cannot join, name or impersonate a server-side trace.
func classifyTraceInput(traceparent string) (source, linkTraceID string) {
	if traceparent == "" {
		return "new", ""
	}
	fields := strings.Split(traceparent, "-")
	if len(fields) != 4 ||
		fields[0] != "00" ||
		!isHexOfLength(fields[1], 32) || isAllZero(fields[1]) ||
		!isHexOfLength(fields[2], 16) || isAllZero(fields[2]) ||
		!isHexOfLength(fields[3], 2) {
		return "rejected", ""
	}
	return "linked", fields[1]
}

func isHexOfLength(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, character := range value {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func isAllZero(value string) bool {
	return strings.Trim(value, "0") == ""
}

// callerPeerFor names the public peer on a completion record. The telemetry
// contract reserves the "unauthenticated" peer for a request that resolved to no
// trusted actor, and requires such a record to be a denial carrying
// UNAUTHENTICATED and no actor or tenant. Every other request is recorded as the
// declared public consumer.
func callerPeerFor(errorCode string) string {
	if errorCode == codeUnauthenticated {
		return "unauthenticated"
	}
	return "browser"
}

// routeTemplateFor records the declared route, never the requested path, so an
// arbitrary caller-supplied path cannot enter a log field or a metric label.
func routeTemplateFor(r *http.Request) string {
	if pattern := r.Pattern; pattern != "" && pattern != "/" {
		return pattern
	}
	return "undeclared"
}

func outcomeForStatus(status int) string {
	switch {
	case status < 400:
		return "ok"
	case status == http.StatusConflict:
		return "conflict"
	case status == http.StatusTooManyRequests:
		return "overloaded"
	case status == http.StatusServiceUnavailable:
		return "unavailable"
	case status < 500:
		return "denied"
	default:
		return "error"
	}
}

// newRequestID produces a values-v1 identifier. Randomness comes from the
// operating system, so identifiers stay unguessable and unique across replicas
// without shared state.
func newRequestID() string {
	buffer := make([]byte, 16)
	if _, err := rand.Read(buffer); err != nil {
		return "req-" + hex.EncodeToString([]byte(time.Now().UTC().Format(time.RFC3339Nano)))
	}
	return "req-" + hex.EncodeToString(buffer)
}
