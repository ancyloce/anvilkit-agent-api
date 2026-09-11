package httpapi

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"
	"github.com/ancyloce/anvilkit-agent-api/internal/readmodel"
)

// serveEventStream subscribes one connection to an operation's durable events.
//
// The connection replays committed events after its cursor, then joins the
// catch-up its operation's subscribers share. Nothing is streamed before the
// cursor and the caller's disclosure evidence have both been accepted, and the
// grant is renewed on its bounded schedule for as long as the connection lives:
// a stream that cannot prove current authorization stops, it does not coast on
// the decision it started with.
func (s *Server) serveEventStream(
	w http.ResponseWriter,
	r *http.Request,
	actor identity.Actor,
	operationID string,
	grant disclosureGrant,
	cursor uint64,
) {
	// The first catch-up read happens before any header is written, so a read
	// role that is unavailable is answered with the error envelope rather than
	// with a stream that immediately ends.
	first, err := s.readModel.ReadEvents(
		r.Context(), actor.TenantID, operationID, storedEventSequence(cursor), config.ReplayPageEvents)
	if err != nil {
		s.writeError(w, r, projectionFault(err))
		return
	}
	if !grant.usableAt(time.Now()) {
		s.writeError(w, r, newFault(codeDependencyUnavailable, "authorization expired during the protected read"))
		return
	}

	stream := newEventStreamWriter(w)
	stream.authorizationDeadline = grant.freshUntil.Add(-config.ClockMaxInterServiceError)
	if err := stream.open(); err != nil {
		recordOutcome(r.Context(), http.StatusOK, "", "")
		return
	}
	recordOutcome(r.Context(), http.StatusOK, "", "")
	s.logSubscription(r, logging.EventSSESubscribed, operationID, cursor, 0, "", "")

	position := cursor
	closeReason := closeReasonClientGone
	defer func() {
		s.logSubscription(r, logging.EventSSEClosed, operationID, cursor, position, closeReason, "")
	}()

	// A cursor below the retention floor cannot be served without skipping a
	// sequence, which is never allowed, so the client is sent to the snapshot
	// handshake instead.
	if first.SnapshotRequired(storedEventSequence(cursor)) {
		_ = stream.writeSnapshotRequired(operationID, first.CoveredSeq)
		closeReason = closeReasonRetentionRestart
		return
	}

	page := first
	for {
		if !grant.usableAt(time.Now()) {
			renewed, fault := s.authorizeDisclosure(r.Context(), actor, operationID)
			if fault != nil {
				closeReason = closeReasonAuthorizationExpiry
				return
			}
			grant = renewed
			stream.authorizationDeadline = grant.freshUntil.Add(-config.ClockMaxInterServiceError)
		}
		writeErr := stream.writeEvents(operationID, page.Events, &position)
		if writeErr != nil {
			closeReason = closeReasonFor(writeErr)
			return
		}
		if len(page.Events) < config.ReplayPageEvents {
			break
		}
		// A read that fails during catch-up is not fatal: the subscriber joins
		// the shared catch-up at its current position, which retries.
		page, err = s.readModel.ReadEvents(
			r.Context(), actor.TenantID, operationID, storedEventSequence(position), config.ReplayPageEvents)
		if err != nil {
			break
		}
	}

	member := s.hub.attach(operationID, actor.TenantID, position)
	defer s.hub.detach(operationID, member)

	closeReason = s.pumpEventStream(r, stream, member, actor, operationID, grant, &position)
}

// pumpEventStream writes buffered events, heartbeats and the connection's end,
// and reports why the connection closed.
func (s *Server) pumpEventStream(
	r *http.Request,
	stream *eventStreamWriter,
	member *subscriber,
	actor identity.Actor,
	operationID string,
	grant disclosureGrant,
	position *uint64,
) string {
	// One guard tick drives every bounded obligation of a live connection:
	// heartbeats, disclosure renewal, the slow-consumer bound and drain. A
	// separate timer for each would only add ways for them to disagree.
	guard := time.NewTicker(config.SharedCatchup)
	defer guard.Stop()

	lastHeartbeat := time.Now()
	renewAt := grant.renewAt(time.Now())

	for {
		select {
		case <-r.Context().Done():
			return closeReasonClientGone
		case <-member.notify:
		case <-guard.C:
		}

		if s.draining.Load() {
			// The hint is randomized so the connections one replica sheds do
			// not all return at the same instant.
			_ = stream.writeRetryHint(reconnectHint())
			return closeReasonDrain
		}

		now := time.Now()
		if !now.Before(renewAt) {
			renewed, fault := s.authorizeDisclosure(r.Context(), actor, operationID)
			if fault != nil {
				return closeReasonAuthorizationExpiry
			}
			renewAt = renewed.renewAt(now)
			stream.authorizationDeadline = renewed.freshUntil.Add(-config.ClockMaxInterServiceError)
		}
		if member.pendingClose() == closeReasonRetentionRestart {
			_ = stream.writeSnapshotRequired(operationID, member.restartHint())
			return closeReasonRetentionRestart
		}

		if buffered := member.drain(); len(buffered) > 0 {
			if err := stream.writeEvents(operationID, buffered, position); err != nil {
				return closeReasonFor(err)
			}
			lastHeartbeat = now
		}

		if member.blockedFor(now) > config.SlowConsumerClose {
			return closeReasonSlowConsumer
		}
		if now.Sub(lastHeartbeat) >= config.Heartbeat {
			if err := stream.writeHeartbeat(); err != nil {
				return closeReasonFor(err)
			}
			lastHeartbeat = now
		}
	}
}

// closeReasonFor separates a consumer that stopped reading from one that went
// away. A write that exhausts its deadline is the slow-consumer bound.
func closeReasonFor(err error) string {
	if errors.Is(err, errDisclosureExpired) {
		return closeReasonAuthorizationExpiry
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return closeReasonSlowConsumer
	}
	return closeReasonClientGone
}

// eventStreamWriter renders SSE frames with a bounded write deadline.
type eventStreamWriter struct {
	response              http.ResponseWriter
	controller            *http.ResponseController
	authorizationDeadline time.Time
}

func newEventStreamWriter(w http.ResponseWriter) *eventStreamWriter {
	return &eventStreamWriter{response: w, controller: http.NewResponseController(w)}
}

// open sends the stream's headers. Buffering by an intermediary would defeat
// both the heartbeat and the slow-consumer bound, so it is refused explicitly.
func (e *eventStreamWriter) open() error {
	header := e.response.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-store")
	header.Set("Connection", "keep-alive")
	header.Set("X-Accel-Buffering", "no")
	e.response.WriteHeader(http.StatusOK)
	return e.controller.Flush()
}

// writeEvents sends one frame per durable event and advances position to the
// last sequence actually written.
func (e *eventStreamWriter) writeEvents(operationID string, events []readmodel.Event, position *uint64) error {
	for _, event := range events {
		sequence, err := parseEventSeq(event.EventSeq)
		if err != nil {
			// A sequence that is not a bounded counter cannot be turned into a
			// cursor, and delivering it would leave the client unable to
			// reconnect from this point.
			return fmt.Errorf("httpapi: event %q carries an unusable sequence", event.EventSeq)
		}
		body, err := marshalEvent(event)
		if err != nil {
			return err
		}
		frame := "event: " + event.Type + "\n" +
			"id: " + encodeEventCursor(operationID, sequence) + "\n" +
			"data: " + string(body) + "\n\n"
		if err := e.write(frame); err != nil {
			return err
		}
		*position = sequence
	}
	return nil
}

// writeHeartbeat sends a comment line. It carries no id, so it advances no
// durable cursor and is never logged.
func (e *eventStreamWriter) writeHeartbeat() error {
	return e.write(":hb\n\n")
}

// writeRetryHint asks the client to wait before reconnecting.
func (e *eventStreamWriter) writeRetryHint(pause time.Duration) error {
	return e.write("retry: " + strconv.FormatInt(pause.Milliseconds(), 10) + "\n\n")
}

// writeSnapshotRequired sends the one non-durable frame the contract defines.
// It carries no id, so a client cannot mistake it for a resumable position.
func (e *eventStreamWriter) writeSnapshotRequired(operationID string, coveredSeqHint int64) error {
	body, err := marshalSnapshotRequired(operationID, coveredSeqHint)
	if err != nil {
		return err
	}
	return e.write("event: snapshot-required\ndata: " + string(body) + "\n\n")
}

func (e *eventStreamWriter) write(frame string) error {
	// The deadline is the slow-consumer bound: a consumer that cannot take a
	// frame within it is disconnected and recovers through replay.
	now := time.Now()
	deadline := now.Add(config.SlowConsumerClose)
	if !e.authorizationDeadline.IsZero() {
		if !now.Before(e.authorizationDeadline) {
			return errDisclosureExpired
		}
		if e.authorizationDeadline.Before(deadline) {
			deadline = e.authorizationDeadline
		}
	}
	_ = e.controller.SetWriteDeadline(deadline)
	if _, err := e.response.Write([]byte(frame)); err != nil {
		if !e.authorizationDeadline.IsZero() && !time.Now().Before(e.authorizationDeadline) {
			return errDisclosureExpired
		}
		return err
	}
	err := e.controller.Flush()
	if err != nil && !e.authorizationDeadline.IsZero() && !time.Now().Before(e.authorizationDeadline) {
		return errDisclosureExpired
	}
	return err
}
