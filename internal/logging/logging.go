// Package logging produces the structured records the parent telemetry contract
// defines in contracts/telemetry/log-record-v1.schema.json.
//
// The record shape is closed, so this package fixes the required envelope
// fields and lets callers add only the contract's optional fields as attributes.
// Logs here are diagnostic: they never authorize execution or carry request
// bodies, credentials, definition source or raw reconnect cursors.
package logging

import (
	"io"
	"log/slog"
	"time"
)

// ServiceName is the canonical unit name recorded on every record.
const ServiceName = "anvilkit-agent-api"

// Event names this service emits. The contract's eventName enum is closed, so
// they are named here rather than spelled at each call site.
const (
	EventServiceStarted     = "service.started"
	EventServiceStopping    = "service.stopping"
	EventServiceDrained     = "service.drained"
	EventConfigLoaded       = "config.loaded"
	EventRPCServerCompleted = "rpc.server.completed"
	EventRPCClientCompleted = "rpc.client.completed"
	EventSSESubscribed      = "sse.subscribed"
	EventSSESnapshot        = "sse.snapshot"
	EventSSEClosed          = "sse.closed"
	EventHealthTransition   = "health.transition"
)

// Identity carries the deployment facts every record must repeat.
type Identity struct {
	ServiceVersion    string
	ServiceInstanceID string
	Environment       string
}

// New builds a logger whose records satisfy urn:anvilkit:log-record:v1.
func New(out io.Writer, identity Identity, level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(out, &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: renameEnvelopeKeys,
	})
	return slog.New(handler).With(
		slog.Int("schemaVersion", 1),
		slog.String("origin", "service"),
		slog.String("service.name", ServiceName),
		slog.String("service.version", identity.ServiceVersion),
		slog.String("service.instance.id", identity.ServiceInstanceID),
		slog.String("environment", identity.Environment),
	)
}

// renameEnvelopeKeys maps slog's built-in keys onto the contract's field names
// and formats the timestamp as the required UTC RFC 3339 string.
func renameEnvelopeKeys(groups []string, attr slog.Attr) slog.Attr {
	if len(groups) > 0 {
		return attr
	}
	switch attr.Key {
	case slog.TimeKey:
		return slog.String("timestamp", attr.Value.Time().UTC().Format("2006-01-02T15:04:05.000Z"))
	case slog.LevelKey:
		return slog.String("severity", severityOf(attr.Value.Any()))
	case slog.MessageKey:
		if attr.Value.String() == "" {
			return slog.Attr{}
		}
		return slog.String("message", truncate(attr.Value.String(), 512))
	default:
		return attr
	}
}

func severityOf(value any) string {
	level, ok := value.(slog.Level)
	if !ok {
		return "INFO"
	}
	switch {
	case level < slog.LevelInfo:
		return "DEBUG"
	case level < slog.LevelWarn:
		return "INFO"
	case level < slog.LevelError:
		return "WARN"
	default:
		return "ERROR"
	}
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

// DurationMs renders an elapsed time as the contract's durationMs integer.
func DurationMs(elapsed time.Duration) int64 {
	milliseconds := elapsed.Milliseconds()
	if milliseconds < 0 {
		return 0
	}
	return milliseconds
}
