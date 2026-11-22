package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

var sequencePattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)

// canonicalSequence is the canonical-sequence rule of contracts.md §4: an
// unsigned decimal string without leading zeros, bounded by uint64. The
// pattern alone admits 20-digit values above the bound (RB-A23).
func canonicalSequence(s string) bool {
	if !sequencePattern.MatchString(s) {
		return false
	}
	_, err := strconv.ParseUint(s, 10, 64)
	return err == nil
}

// The canonical-sequence rule is registered once on Gin's validator so the
// cursor parameters bind through ShouldBind* like every other request value.
func init() {
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		_ = v.RegisterValidation("sequence", func(fl validator.FieldLevel) bool {
			return canonicalSequence(fl.Field().String())
		})
	}
}

// streamCursor carries the two ways a client names its cursor; `after`
// wins over the header when both are present.
type streamCursor struct {
	After       string `form:"after" binding:"omitempty,sequence"`
	LastEventID string `header:"Last-Event-ID" binding:"omitempty,sequence"`
}

// bindCursor refuses a malformed cursor naming the parameter in details.field
// and never the value (RB-S13).
func bindCursor(c *gin.Context) (string, *apiError) {
	var cur streamCursor
	if err := c.ShouldBindQuery(&cur); err != nil {
		return "", invalidField("after must be a canonical sequence", "after")
	}
	if err := c.ShouldBindHeader(&cur); err != nil {
		return "", invalidField("Last-Event-ID must be a canonical sequence", "Last-Event-ID")
	}
	if cur.After != "" {
		return cur.After, nil
	}
	return cur.LastEventID, nil
}

var errSlowConsumer = errors.New("sse consumer too slow")

const (
	eventReset     = "reset_required"
	heartbeatFrame = ": keepalive\n\n"
)

// streamEvents serves API-04: durable events after `Last-Event-ID` or
// `after` as SSE. Frames are OperationEventFrame documents encoded by
// gin-contrib/sse; a cursor that cannot be honored yields one
// reset_required frame and closes; the stream ends with the operation.
func streamEvents(control application.Control, bounds StreamBounds, limiter *streamLimiter, draining <-chan struct{}) gin.HandlerFunc {
	return func(c *gin.Context) {
		after, refusal := bindCursor(c)
		if refusal != nil {
			fail(c, refusal)
			return
		}
		operationID := c.Param("operationId")
		p := principal(c)
		// Authorization and existence are verified by Control before the
		// first frame; errors before that surface as a normal envelope.
		if _, err := control.GetOperation(c.Request.Context(), p, operationID); err != nil {
			fail(c, fromControl(err))
			return
		}
		serveStream(c, bounds, limiter, draining, eventStream{
			open: func(ctx context.Context, admitted func(), emit func([]byte) error) error {
				return control.StreamEvents(ctx, p, operationID, after, admitted, func(f application.EventFrame) error {
					b, err := encodeFrame(f)
					if err != nil {
						return err
					}
					return emit(b)
				})
			},
			reset:    func(covered string) ([]byte, error) { return encodeReset(operationID, covered) },
			end:      func(reason string) ([]byte, error) { return encodeEnd(operationID, reason) },
			terminal: true,
		})
	}
}

// eventStream is what one route adds to the shared SSE serving: Control's
// stream (emit takes encoded frames), the route's reset and end frames, and
// whether a stream that returns without an error is over (an operation's
// terminal event) rather than ended by the server (a conversation, which
// has no terminal state).
type eventStream struct {
	open     func(ctx context.Context, admitted func(), emit func([]byte) error) error
	reset    func(coveredEventSeq string) ([]byte, error)
	end      func(reason string) ([]byte, error)
	terminal bool
}

// serveStream writes one admitted event stream (API-04, D4a).
//
// Exactly one goroutine (the handler) writes to the response: events and
// heartbeats share it. Control's stream runs in a producer goroutine that
// only encodes frames into a bounded channel; when the buffer stays full
// beyond the grace period, or a write fails or times out, the consumer is
// disconnected and the producer is canceled and joined before the handler
// returns, so nothing touches the response afterwards. Disconnecting never
// affects the operation or the turn: the stream is a read cursor over
// committed events.
//
// sse.Encode (v1.1.1) discards the writer's errors for id/event/[]byte
// data, so frames are encoded into a buffer and the connection write is
// performed and checked here, under the write deadline.
func serveStream(c *gin.Context, bounds StreamBounds, limiter *streamLimiter, draining <-chan struct{}, s eventStream) {
	p := principal(c)
	// The stream caps (P1.4, API-03): per principal and per process, shared
	// by the operation and conversation streams; a refused stream is a
	// retryable 429 with Retry-After.
	release, ok := limiter.acquire(p.TenantID + "/" + p.ActorID)
	if !ok {
		fail(c, &apiError{status: http.StatusTooManyRequests, code: "CAPACITY_EXHAUSTED", message: "too many open event streams", retryable: true})
		return
	}
	defer release()

	ctx, cancel := context.WithCancel(c.Request.Context())
	frames := make(chan []byte, bounds.FrameBuffer)
	done := make(chan error, 1)
	admitted := make(chan struct{})
	go func() {
		done <- s.open(ctx, func() { close(admitted) }, func(b []byte) error {
			grace := time.NewTimer(bounds.SlowConsumerGrace)
			defer grace.Stop()
			select {
			case frames <- b:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-grace.C:
				return errSlowConsumer
			}
		})
	}()
	ended := false
	defer func() {
		cancel()
		if !ended {
			<-done // the producer never outlives the response
		}
	}()

	// The response begins only once Control admitted the stream (D-7): a
	// refusal before that (Control's stream caps) is a retryable error
	// envelope, a 429 with Retry-After, never a 200 that ends without an
	// event, which API-04 reads as a terminal operation.
	var early error
	select {
	case <-admitted:
	case early = <-done:
		ended = true
	case <-draining:
		fail(c, &apiError{status: http.StatusServiceUnavailable, code: "DEPENDENCY_UNAVAILABLE", message: "the server is shutting down", retryable: true})
		return
	case <-ctx.Done():
		return // the client went away
	}
	var reset *application.ResetRequired
	if early != nil && !errors.As(early, &reset) {
		fail(c, fromControl(early))
		return
	}
	w := c.Writer
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Accel-Buffering", "no")
	sse.Event{}.WriteContentType(w)
	w.WriteHeader(http.StatusOK)
	w.Flush()

	controller := http.NewResponseController(w)
	write := func(b []byte) bool {
		if err := controller.SetWriteDeadline(time.Now().Add(bounds.WriteTimeout)); err != nil {
			return false // no deadline support means no bounded write; give the connection up
		}
		if _, err := w.Write(b); err != nil {
			return false
		}
		w.Flush()
		return true
	}
	writeEnd := func() {
		if b, err := s.end("server_shutdown"); err == nil {
			write(b)
		}
	}
	// The reconnect interval EventSource honors, jittered so a restart
	// does not bring every viewer back in the same instant; written and
	// checked like every other frame.
	if !write([]byte(fmt.Sprintf("retry: %d\n\n", retryMillis(bounds.RetryBase)))) {
		return
	}
	if ended {
		// The cursor needed a reset (before any admission), or the stream
		// had nothing to send and is over.
		switch {
		case reset != nil:
			if b, err := s.reset(reset.CoveredEventSeq); err == nil {
				write(b)
			}
		case !s.terminal:
			writeEnd()
		}
		return
	}
	heartbeat := time.NewTicker(bounds.HeartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case b := <-frames:
			if !write(b) {
				return
			}
		case <-heartbeat.C:
			if !write([]byte(heartbeatFrame)) {
				return
			}
		case err := <-done:
			ended = true
			// Frames encoded before the stream ended are still delivered in order.
			for drained := false; !drained; {
				select {
				case b := <-frames:
					if !write(b) {
						return
					}
				default:
					drained = true
				}
			}
			switch {
			case errors.As(err, &reset):
				if b, err := s.reset(reset.CoveredEventSeq); err == nil {
					write(b)
				}
			case errors.Is(err, errSlowConsumer) || c.Request.Context().Err() != nil:
			case err != nil || !s.terminal:
				// Control ended the stream on its own (a connection aged out, a
				// replica restarted): the client resumes from its last event id
				// after the retry interval and loses nothing (API-04).
				writeEnd()
			}
			return
		case <-draining:
			// The server shuts down (P1.7): the stream ends with an end
			// frame naming the reason, and the client reconnects (to
			// another replica) after the advertised retry interval from
			// its last event id.
			writeEnd()
			return
		case <-ctx.Done():
			return // the client went away
		}
	}
}

// streamConversationEvents serves D4a's conversation stream (design 0001
// §8, DEC-070) on the same serving, bounds and limiter as API-04: frames are
// ConversationEventFrame documents; a cursor named twice with different
// values is refused INVALID_ARGUMENT (for conversations only: the operation
// stream keeps `after` winning); a conversation has no terminal state, so
// the stream ends only when the client goes, the server drains, or Control
// ends it (an end frame either way).
func streamConversationEvents(control application.Control, bounds StreamBounds, limiter *streamLimiter, draining <-chan struct{}) gin.HandlerFunc {
	return func(c *gin.Context) {
		after, refusal := bindSingleCursor(c)
		if refusal != nil {
			fail(c, refusal)
			return
		}
		conversationID := c.Param("conversationId")
		p := principal(c)
		if _, err := control.GetConversation(c.Request.Context(), p, conversationID); err != nil {
			fail(c, fromControl(err))
			return
		}
		serveStream(c, bounds, limiter, draining, eventStream{
			open: func(ctx context.Context, admitted func(), emit func([]byte) error) error {
				return control.StreamConversationEvents(ctx, p, conversationID, after, admitted, func(f agentapi.ConversationEventFrame) error {
					b, err := encodeConversationFrame(f)
					if err != nil {
						return err
					}
					return emit(b)
				})
			},
			reset: func(covered string) ([]byte, error) {
				return encodeJSONEvent(eventReset, agentapi.ConversationResetFrame{ConversationId: conversationID, CoveredEventSeq: covered})
			},
			end: func(reason string) ([]byte, error) {
				return encodeJSONEvent(eventEnd, agentapi.ConversationEndFrame{ConversationId: conversationID, Reason: agentapi.ConversationEndFrameReason(reason)})
			},
		})
	}
}

// bindSingleCursor is bindCursor for a stream that refuses a cursor named
// twice with different values (design 0001 §8.1).
func bindSingleCursor(c *gin.Context) (string, *apiError) {
	if _, refusal := bindCursor(c); refusal != nil {
		return "", refusal
	}
	after, header := c.Query("after"), c.GetHeader("Last-Event-ID")
	if after != "" && header != "" && after != header {
		return "", invalidField("after and Last-Event-ID name different cursors", "after")
	}
	if after != "" {
		return after, nil
	}
	return header, nil
}

// encodeConversationFrame renders one ConversationEventFrame as an SSE event
// whose id is the conversation's event sequence and whose event is its type.
func encodeConversationFrame(f agentapi.ConversationEventFrame) ([]byte, error) {
	payload, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	return encode(sse.Event{Id: f.EventSeq, Event: string(f.EventType), Data: payload})
}

// encodeJSONEvent renders a frame without an id (reset_required, end).
func encodeJSONEvent(event string, v any) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return encode(sse.Event{Event: event, Data: payload})
}

// eventEnd is the final event of a stream the server ends on its own
// (P1.7): its data names the operation and the reason.
const eventEnd = "end"

func encodeEnd(operationID, reason string) ([]byte, error) {
	payload, err := json.Marshal(map[string]string{"operationId": operationID, "reason": reason})
	if err != nil {
		return nil, err
	}
	return encode(sse.Event{Event: eventEnd, Data: payload})
}

// encodeFrame renders one OperationEventFrame as an SSE event whose id is
// the durable event sequence. The payload is marshaled once so the wire
// document is canonical JSON; gin-contrib/sse writes the id, event and
// data fields.
func encodeFrame(f application.EventFrame) ([]byte, error) {
	payload, err := json.Marshal(map[string]any{
		"operationId": f.OperationID, "eventSeq": f.EventSeq, "transitionId": f.TransitionID, "eventType": f.EventType,
		"revision": f.Revision, "occurredAt": f.OccurredAt.UTC().Format(time.RFC3339Nano),
		"payload": omitEmpty(map[string]any{
			"lifecycle": f.Lifecycle, "phase": f.Phase, "control": f.Control, "cleanup": f.Cleanup, "finance": f.Finance, "failureCode": f.FailureCode,
			// Attempt-bound facts (plan 0005 Phase 2); absent members are omitted.
			"step": f.Step, "visit": f.Visit, "attemptOrdinal": f.AttemptOrdinal, "verdict": f.Verdict, "verdictCode": f.VerdictCode,
			"node": f.Node, "nodeState": f.NodeState, "round": f.Round, "outcomeCode": f.OutcomeCode,
		}),
	})
	if err != nil {
		return nil, err
	}
	return encode(sse.Event{Id: f.EventSeq, Event: f.EventType, Data: payload})
}

// encodeReset renders the reset_required frame (ResetFrame data).
func encodeReset(operationID, coveredEventSeq string) ([]byte, error) {
	payload, err := json.Marshal(map[string]string{"operationId": operationID, "coveredEventSeq": coveredEventSeq})
	if err != nil {
		return nil, err
	}
	return encode(sse.Event{Event: eventReset, Data: payload})
}

func encode(ev sse.Event) ([]byte, error) {
	var buf bytes.Buffer
	if err := sse.Encode(&buf, ev); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func omitEmpty(m map[string]any) map[string]any {
	for k, v := range m {
		if s, ok := v.(string); ok && s == "" {
			delete(m, k)
		}
	}
	return m
}

// retryMillis is the advertised reconnect interval: base plus up to 50% jitter.
func retryMillis(base time.Duration) int64 {
	if base <= 0 {
		base = 3 * time.Second
	}
	jitter := time.Duration(rand.Int64N(int64(base/2) + 1))
	return (base + jitter).Milliseconds()
}

// streamLimiter bounds the open event streams per process and per
// principal (P1.4); zero limits mean unbounded.
type streamLimiter struct {
	mu         sync.Mutex
	max        int
	perSubject int
	total      int
	bySubject  map[string]int
}

func newStreamLimiter(max, perSubject int) *streamLimiter {
	return &streamLimiter{max: max, perSubject: perSubject, bySubject: map[string]int{}}
}

func (l *streamLimiter) acquire(subject string) (func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if (l.max > 0 && l.total >= l.max) || (l.perSubject > 0 && l.bySubject[subject] >= l.perSubject) {
		return nil, false
	}
	l.total++
	l.bySubject[subject]++
	return func() {
		l.mu.Lock()
		l.total--
		l.bySubject[subject]--
		if l.bySubject[subject] == 0 {
			delete(l.bySubject, subject)
		}
		l.mu.Unlock()
	}, true
}
