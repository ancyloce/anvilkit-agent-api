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
	"sync"
	"time"

	"github.com/gin-contrib/sse"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

var sequencePattern = regexp.MustCompile(`^(0|[1-9][0-9]{0,19})$`)

// The canonical-sequence rule of contracts.md §4 is registered once on
// Gin's validator so the cursor parameters bind through ShouldBind* like
// every other request value.
func init() {
	if v, ok := binding.Validator.Engine().(*validator.Validate); ok {
		_ = v.RegisterValidation("sequence", func(fl validator.FieldLevel) bool {
			return sequencePattern.MatchString(fl.Field().String())
		})
	}
}

// streamCursor carries the two ways a client names its cursor; `after`
// wins over the header when both are present.
type streamCursor struct {
	After       string `form:"after" binding:"omitempty,sequence"`
	LastEventID string `header:"Last-Event-ID" binding:"omitempty,sequence"`
}

func bindCursor(c *gin.Context) (string, error) {
	var cur streamCursor
	if err := c.ShouldBindQuery(&cur); err != nil {
		return "", errors.New("after must be a canonical sequence")
	}
	if err := c.ShouldBindHeader(&cur); err != nil {
		return "", errors.New("Last-Event-ID must be a canonical sequence")
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
// reset_required frame and closes.
//
// Exactly one goroutine (the handler) writes to the response: events and
// heartbeats share it. Control's stream runs in a producer goroutine that
// only encodes frames into a bounded channel; when the buffer stays full
// beyond the grace period, or a write fails or times out, the consumer is
// disconnected and the producer is canceled and joined before the handler
// returns, so nothing touches the response afterwards. Disconnecting never
// affects the operation: the stream is a read cursor over committed events.
//
// sse.Encode (v1.1.1) discards the writer's errors for id/event/[]byte
// data, so frames are encoded into a buffer and the connection write is
// performed and checked here, under the write deadline.
func streamEvents(control application.Control, bounds StreamBounds, limiter *streamLimiter, draining <-chan struct{}) gin.HandlerFunc {
	return func(c *gin.Context) {
		after, err := bindCursor(c)
		if err != nil {
			fail(c, invalidArgument(err.Error()))
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
		// The stream caps (P1.4, API-03): per principal and per process; a
		// refused stream is a retryable 429 with Retry-After.
		release, ok := limiter.acquire(p.TenantID + "/" + p.ActorID)
		if !ok {
			fail(c, &apiError{status: http.StatusTooManyRequests, code: "CAPACITY_EXHAUSTED", message: "too many open event streams", retryable: true})
			return
		}
		defer release()
		w := c.Writer
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Accel-Buffering", "no")
		sse.Event{}.WriteContentType(w)
		w.WriteHeader(http.StatusOK)
		w.Flush()

		ctx, cancel := context.WithCancel(c.Request.Context())
		frames := make(chan []byte, bounds.FrameBuffer)
		done := make(chan error, 1)
		go func() {
			done <- control.StreamEvents(ctx, p, operationID, after, func(f application.EventFrame) error {
				b, err := encodeFrame(f)
				if err != nil {
					return err
				}
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
		// The reconnect interval EventSource honors, jittered so a restart
		// does not bring every viewer back in the same instant; written and
		// checked like every other frame.
		if !write([]byte(fmt.Sprintf("retry: %d\n\n", retryMillis(bounds.RetryBase)))) {
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
				var reset *application.ResetRequired
				if errors.As(err, &reset) {
					if b, err := encodeReset(operationID, reset.CoveredEventSeq); err == nil {
						write(b)
					}
				}
				return
			case <-draining:
				// The server shuts down (P1.7): the stream ends with an end
				// frame naming the reason, and the client reconnects (to
				// another replica) after the advertised retry interval from
				// its last event id.
				if b, err := encodeEnd(operationID, "server_shutdown"); err == nil {
					write(b)
				}
				return
			case <-ctx.Done():
				return // the client went away
			}
		}
	}
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
		"payload": omitEmpty(map[string]any{"lifecycle": f.Lifecycle, "phase": f.Phase, "control": f.Control, "cleanup": f.Cleanup, "finance": f.Finance, "failureCode": f.FailureCode}),
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
