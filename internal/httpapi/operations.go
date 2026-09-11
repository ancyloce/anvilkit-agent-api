package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"
	"github.com/ancyloce/anvilkit-agent-api/internal/readmodel"
)

// The authorized read surface and the action every one of its routes requires,
// from the Agent OpenAPI's x-anvilkit-required-action.
const (
	routeOperation         = "/v1/operations/{operationId}"
	routeOperationSnapshot = "/v1/operations/{operationId}/snapshot"
	routeOperationEvents   = "/v1/operations/{operationId}/events"
	actionOperationRead    = "operation.read"
)

// eventPage is the JSON representation of GET /v1/operations/{id}/events. The
// two sequences let a client decide for itself whether its cursor still has a
// gapless path forward or needs the snapshot handshake.
type eventPage struct {
	OperationID     string            `json:"operationId"`
	Events          []readmodel.Event `json:"events"`
	CoveredSeq      string            `json:"coveredSeq"`
	RetainedFromSeq string            `json:"retainedFromSeq"`
}

// snapshotRequiredFrame is the only non-durable data frame the SSE contract
// defines. It carries no id, so it cannot advance a durable cursor.
type snapshotRequiredFrame struct {
	OperationID    string `json:"operationId"`
	CoveredSeqHint string `json:"coveredSeqHint"`
}

// handleReadOperation serves GET /v1/operations/{operationId}.
func (s *Server) handleReadOperation(w http.ResponseWriter, r *http.Request) {
	actor, operationID, grant, fault := s.beginProtectedRead(r)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	view, err := s.readModel.ReadOperation(r.Context(), actor.TenantID, operationID)
	if err != nil {
		s.writeError(w, r, projectionFault(err))
		return
	}
	if !grant.usableAt(time.Now()) {
		s.writeError(w, r, newFault(codeDependencyUnavailable, "authorization expired during the protected read"))
		return
	}

	recordOutcome(r.Context(), http.StatusOK, "", "")
	writeJSON(w, http.StatusOK, view)
}

// handleReadSnapshot serves GET /v1/operations/{operationId}/snapshot.
//
// A request without a cursor starts a snapshot at the operation's current
// coveredSeq. A request with one continues that same snapshot: it must arrive
// inside the protection window and the projection must still stand at the
// bound sequence, because pages from two different snapshots would let a client
// assemble a state that never existed.
func (s *Server) handleReadSnapshot(w http.ResponseWriter, r *http.Request) {
	actor, operationID, grant, fault := s.beginProtectedRead(r)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	var cursor snapshotCursor
	if presented := r.URL.Query().Get("stepsCursor"); presented != "" {
		decoded, cursorFault := decodeSnapshotCursor(presented, operationID)
		if cursorFault != nil {
			s.writeError(w, r, cursorFault)
			return
		}
		if time.Since(time.Unix(decoded.IssuedAtUnix, 0)) > config.SnapshotProtection {
			s.writeError(w, r, newFault(codeRestartRequired,
				"the snapshot protection window has passed; restart the handshake with a new snapshot"))
			return
		}
		cursor = decoded
	}

	snapshot, more, err := s.readModel.ReadSnapshot(
		r.Context(), actor.TenantID, operationID, cursor.CoveredSeq, cursor.StepOffset)
	if errors.Is(err, readmodel.ErrSnapshotExpired) {
		s.writeError(w, r, newFault(codeRestartRequired,
			"the projection advanced past this snapshot; restart the handshake with a new snapshot"))
		return
	}
	if err != nil {
		s.writeError(w, r, projectionFault(err))
		return
	}

	if more {
		issuedAt := cursor.IssuedAtUnix
		if issuedAt == 0 {
			issuedAt = time.Now().Unix()
		}
		boundCoveredSeq, parseErr := parseEventSeq(snapshot.CoveredSeq)
		if parseErr != nil {
			s.writeError(w, r, newFault(codeDependencyUnavailable, "the snapshot could not be paginated"))
			return
		}
		snapshot.NextStepsCursor = encodeSnapshotCursor(operationID, snapshotCursor{
			CoveredSeq:   int64(boundCoveredSeq),
			IssuedAtUnix: issuedAt,
			StepOffset:   cursor.StepOffset + len(snapshot.Steps),
		})
	}

	if !grant.usableAt(time.Now()) {
		s.writeError(w, r, newFault(codeDependencyUnavailable, "authorization expired during the protected read"))
		return
	}
	s.logSnapshot(r, operationID, snapshot.CoveredSeq, len(snapshot.Steps), more)
	recordOutcome(r.Context(), http.StatusOK, "", "")
	writeJSON(w, http.StatusOK, snapshot)
}

// handleReadEvents serves GET /v1/operations/{operationId}/events as either a
// JSON replay page or an SSE subscription, chosen by the request's Accept
// header. Both representations read the same committed events.
func (s *Server) handleReadEvents(w http.ResponseWriter, r *http.Request) {
	actor, operationID, grant, fault := s.beginProtectedRead(r)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	presentedCursor := r.Header.Get("Last-Event-ID")
	cursor, fault := resolveEventCursor(presentedCursor, r.URL.Query().Get("afterSeq"), operationID)
	if fault != nil {
		// A cursor this service did not issue is recorded only as an
		// irreversible fingerprint; the presented value never enters a field.
		s.logSubscription(r, logging.EventSSEClosed, operationID, 0, 0, closeReasonClientGone, presentedCursor)
		s.writeError(w, r, fault)
		return
	}

	if acceptsEventStream(r) {
		s.serveEventStream(w, r, actor, operationID, grant, cursor)
		return
	}

	limit, fault := replayLimit(r.URL.Query().Get("limit"))
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	page, err := s.readModel.ReadEvents(r.Context(), actor.TenantID, operationID, storedEventSequence(cursor), limit)
	if err != nil {
		s.writeError(w, r, projectionFault(err))
		return
	}
	if !grant.usableAt(time.Now()) {
		s.writeError(w, r, newFault(codeDependencyUnavailable, "authorization expired during the protected read"))
		return
	}

	recordOutcome(r.Context(), http.StatusOK, "", "")
	writeJSON(w, http.StatusOK, eventPage{
		OperationID:     operationID,
		Events:          page.Events,
		CoveredSeq:      strconv.FormatInt(page.CoveredSeq, 10),
		RetainedFromSeq: strconv.FormatInt(page.RetainedFromSeq, 10),
	})
}

// beginProtectedRead resolves the trusted actor, the operation identifier and
// current disclosure evidence. No projection, event or snapshot page is read
// before all three succeed.
func (s *Server) beginProtectedRead(r *http.Request) (identity.Actor, string, disclosureGrant, *clientFault) {
	actor, err := s.identities.Authorize(r.Header.Get("Authorization"), actionOperationRead)
	if err != nil {
		if errors.Is(err, identity.ErrPermissionDenied) {
			return identity.Actor{}, "", disclosureGrant{},
				newFault(codePermissionDenied, "the authenticated actor does not hold "+actionOperationRead)
		}
		return identity.Actor{}, "", disclosureGrant{},
			newFault(codeUnauthenticated, "the request carries no accepted platform identity")
	}
	recordActor(r.Context(), actor)

	operationID := r.PathValue("operationId")
	if !isIdentifier(operationID) {
		return identity.Actor{}, "", disclosureGrant{},
			newFault(codeInvalidArgument, "the operation identifier is not a values-v1 identifier")
	}
	recordOperation(r.Context(), operationID)

	grant, fault := s.authorizeDisclosure(r.Context(), actor, operationID)
	if fault != nil {
		return identity.Actor{}, "", disclosureGrant{}, fault
	}
	return actor, operationID, grant, nil
}

// resolveEventCursor picks the starting position. A reconnect cursor is
// authoritative when present; otherwise the explicit afterSeq applies, and
// without either the subscription starts before the first event.
func resolveEventCursor(presentedCursor, afterSeq, operationID string) (uint64, *clientFault) {
	if presentedCursor != "" {
		return decodeEventCursor(presentedCursor, operationID)
	}
	if afterSeq == "" {
		return 0, nil
	}
	parsed, err := strconv.ParseUint(afterSeq, 10, 64)
	if err != nil {
		return 0, newFault(codeInvalidArgument, "afterSeq must be a bounded unsigned decimal sequence")
	}
	return parsed, nil
}

// replayLimit bounds a requested JSON page against the contract's ceiling.
func replayLimit(requested string) (int, *clientFault) {
	if requested == "" {
		return config.ReplayPageEvents, nil
	}
	limit, err := strconv.Atoi(requested)
	if err != nil || limit < 1 || limit > config.ReplayPageEvents {
		return 0, newFault(codeInvalidArgument,
			fmt.Sprintf("limit must be between 1 and %d", config.ReplayPageEvents))
	}
	return limit, nil
}

// acceptsEventStream reports whether the caller asked for the streaming
// representation rather than the JSON page.
func acceptsEventStream(r *http.Request) bool {
	for _, offered := range strings.Split(r.Header.Get("Accept"), ",") {
		mediaType, _, _ := strings.Cut(offered, ";")
		if strings.EqualFold(strings.TrimSpace(mediaType), "text/event-stream") {
			return true
		}
	}
	return false
}

// projectionFault maps a read-model failure onto the public envelope. An
// operation the caller's tenant cannot see is absent, which is the same answer
// another tenant's operation gets, so no cross-tenant existence is disclosed.
func projectionFault(err error) *clientFault {
	if errors.Is(err, readmodel.ErrOperationNotFound) {
		return newFault(codeNotFound, "no such operation exists in this scope")
	}
	return newFault(codeDependencyUnavailable, "the projection read role is unavailable")
}

// parseEventSeq reads a bounded uint64 decimal string. Sequences travel as
// strings and are compared numerically, never as text.
func parseEventSeq(value string) (uint64, error) {
	return strconv.ParseUint(value, 10, 64)
}

// A public uint64 cursor beyond PostgreSQL's signed range is ahead of every
// stored event. Clamping preserves that meaning instead of wrapping negative.
func storedEventSequence(sequence uint64) int64 { return int64(min(sequence, math.MaxInt64)) }

// reconnectHint is the randomized pause a drained replica asks a client to
// wait, so the connections one replica sheds do not return together.
func reconnectHint() time.Duration {
	spread := config.ReconnectHintMax - config.ReconnectHintMin
	return config.ReconnectHintMin + time.Duration(rand.Int64N(int64(spread)+1))
}

func (s *Server) logSnapshot(r *http.Request, operationID, coveredSeq string, steps int, more bool) {
	s.logger.LogAttrs(r.Context(), slog.LevelInfo, "",
		slog.String("eventName", logging.EventSSESnapshot),
		slog.String("requestId", requestIDFrom(r.Context())),
		slog.String("operationId", operationID),
		slog.String("eventSeq", coveredSeq),
		slog.Group("attributes",
			slog.Int("stepsInPage", steps),
			slog.Bool("hasNextPage", more),
		),
	)
}

// logSubscription records one subscription's start or end.
//
// The decoded starting and last-sent sequences are recorded; the presented
// cursor never is, in any field. A cursor that did not decode is reduced to an
// irreversible fingerprint so two records for the same bad cursor still
// correlate without the cursor being recoverable from them.
func (s *Server) logSubscription(r *http.Request, eventName, operationID string, fromSeq, lastSentSeq uint64, closeReason, badCursor string) {
	attributes := []slog.Attr{
		slog.String("fromSeq", strconv.FormatUint(fromSeq, 10)),
	}
	if eventName == logging.EventSSEClosed {
		attributes = append(attributes,
			slog.String("lastSentSeq", strconv.FormatUint(lastSentSeq, 10)),
			slog.String("closeReason", closeReason),
		)
	}
	if badCursor != "" {
		attributes = append(attributes, slog.String("cursorFingerprint", cursorFingerprint(badCursor)))
	}

	s.logger.LogAttrs(r.Context(), slog.LevelInfo, "",
		slog.String("eventName", eventName),
		slog.String("requestId", requestIDFrom(r.Context())),
		slog.String("operationId", operationID),
		slog.Any("attributes", slog.GroupValue(attributes...)),
	)
}

// marshalEvent renders one durable event's data field.
func marshalEvent(event readmodel.Event) ([]byte, error) {
	return json.Marshal(event)
}

// marshalSnapshotRequired renders the snapshot-required frame's data field.
func marshalSnapshotRequired(operationID string, coveredSeqHint int64) ([]byte, error) {
	return json.Marshal(snapshotRequiredFrame{
		OperationID:    operationID,
		CoveredSeqHint: strconv.FormatInt(coveredSeqHint, 10),
	})
}
