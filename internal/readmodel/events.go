package readmodel

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

// Event is urn:anvilkit:operation-event:v1 carrying its strict
// urn:anvilkit:operation-event-payloads:v1 payload.
//
// The durable stream key is (operationId, eventSeq). The sequence crosses as a
// decimal string so a client compares it numerically rather than through a
// JavaScript number, and the payload stays raw so the committed bytes reach the
// subscriber exactly as Control appended them.
type Event struct {
	SchemaVersion int             `json:"schemaVersion"`
	OperationID   string          `json:"operationId"`
	EventSeq      string          `json:"eventSeq"`
	Type          string          `json:"type"`
	OccurredAt    string          `json:"occurredAt"`
	Payload       json.RawMessage `json:"payload"`
}

// EventPage is one bounded replay range plus the two sequences a client needs
// to decide whether it can continue: what the projection has committed, and the
// oldest event still retained.
type EventPage struct {
	Events []Event
	// CoveredSeq is the highest committed sequence of the operation.
	CoveredSeq int64
	// RetainedFromSeq is the oldest sequence still readable. A cursor below
	// RetainedFromSeq-1 has lost events and needs the snapshot handshake.
	RetainedFromSeq int64
}

// SnapshotRequired reports whether replay after cursor would skip a sequence
// that is no longer retained. Events after the cursor start at cursor+1, so a
// cursor is still usable exactly while that successor is retained.
func (p EventPage) SnapshotRequired(cursor int64) bool {
	return cursor < p.RetainedFromSeq-1
}

// ReadEvents returns committed events strictly after afterSeq, in sequence
// order, at most limit of them.
//
// The events, the covered sequence and the retention floor are read in one
// transaction so a page can never report a coveredSeq that disagrees with the
// events it carries.
func (m *ReadModel) ReadEvents(
	ctx context.Context,
	tenantID, operationID string,
	afterSeq int64,
	limit int,
) (EventPage, error) {
	if limit <= 0 || limit > ReplayPageEvents {
		limit = ReplayPageEvents
	}

	var page EventPage
	err := m.withTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		var coveredSeq int64
		switch err := tx.QueryRow(ctx,
			`SELECT next_event_seq - 1 FROM agent_control.operations WHERE operation_id = $1`,
			operationID).Scan(&coveredSeq); {
		case err == pgx.ErrNoRows:
			return ErrOperationNotFound
		case err != nil:
			return fmt.Errorf("readmodel: reading the covered sequence: %w", err)
		}

		// Retention prunes from the front of an operation's stream, so the
		// oldest surviving row is the retention floor. If no row survives, the
		// next uncommitted sequence is the floor: older cursors still need a
		// snapshot when the entire committed range has been pruned.
		var oldestSeq int64
		if err := tx.QueryRow(ctx,
			`SELECT COALESCE(MIN(event_seq), 0) FROM agent_control.operation_events WHERE operation_id = $1`,
			operationID).Scan(&oldestSeq); err != nil {
			return fmt.Errorf("readmodel: reading the retention floor: %w", err)
		}
		page.CoveredSeq = coveredSeq
		page.RetainedFromSeq = oldestSeq
		if oldestSeq == 0 {
			page.RetainedFromSeq = coveredSeq + 1
		}

		rows, err := tx.Query(ctx, `
			SELECT schema_version, operation_id, event_seq, event_type, occurred_at, body
			FROM agent_control.operation_events
			WHERE operation_id = $1 AND event_seq > $2
			ORDER BY event_seq
			LIMIT $3`, operationID, afterSeq, limit)
		if err != nil {
			return fmt.Errorf("readmodel: reading the event range: %w", err)
		}
		defer rows.Close()

		page.Events = make([]Event, 0, limit)
		for rows.Next() {
			var (
				event      Event
				eventSeq   int64
				occurredAt time.Time
				body       []byte
			)
			if err := rows.Scan(
				&event.SchemaVersion, &event.OperationID, &eventSeq, &event.Type, &occurredAt, &body,
			); err != nil {
				return fmt.Errorf("readmodel: reading an event: %w", err)
			}
			event.EventSeq = decimal(eventSeq)
			event.OccurredAt = instant(occurredAt)
			event.Payload = json.RawMessage(body)
			page.Events = append(page.Events, event)
		}
		return rows.Err()
	})
	return page, err
}

// Listen delivers a wake hint whenever an operation event is committed.
//
// It holds one dedicated connection for the whole process rather than one per
// subscriber, and it never reports what was committed: a notification only
// shortens the wait before the next shared catch-up read, so a lost, duplicated
// or unparsable notification costs nothing but latency. The function returns
// when ctx ends; a connection loss is retried because the periodic catch-up
// keeps working meanwhile.
func (m *ReadModel) Listen(ctx context.Context, wake chan<- struct{}) {
	const reconnectPause = time.Second

	for ctx.Err() == nil {
		if err := m.listenOnce(ctx, wake); err != nil && ctx.Err() == nil {
			select {
			case <-ctx.Done():
			case <-time.After(reconnectPause):
			}
		}
	}
}

func (m *ReadModel) listenOnce(ctx context.Context, wake chan<- struct{}) error {
	connection, err := m.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer connection.Release()

	if _, err := connection.Exec(ctx, "LISTEN "+EventChannel); err != nil {
		return err
	}
	for {
		if _, err := connection.Conn().WaitForNotification(ctx); err != nil {
			return err
		}
		// A pending hint is as good as several: the catch-up it triggers reads
		// everything committed, so hints coalesce instead of queueing.
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// parseDecimal reads a bounded uint64 decimal string back into the counter it
// represents. Public sequences travel as strings, and they are compared as
// numbers, never as text.
func parseDecimal(value string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("readmodel: %q is not a bounded counter", value)
	}
	return parsed, nil
}
