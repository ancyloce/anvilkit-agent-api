package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
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
func streamEvents(control application.Control, bounds StreamBounds) gin.HandlerFunc {
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
			case <-ctx.Done():
				return // the client went away
			}
		}
	}
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
