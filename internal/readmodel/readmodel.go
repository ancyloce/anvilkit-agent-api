// Package readmodel reads committed operation projections and durable events
// from the Agent database with the restricted anvilkit_api_ro role.
//
// Two rules shape every read. Each one runs inside a read-only transaction that
// sets anvilkit.tenant_id transaction-locally, so row-level security scopes the
// result to the caller's tenant and a pooled connection can never carry one
// request's tenant into the next. And each returns the public wire shape
// directly rather than a private model an HTTP layer would re-map: the API
// serves the committed projection unchanged, so a second representation could
// only drift from it.
//
// This package holds no authorization decision. Control owns disclosure, and a
// caller reaches these reads only after a current grant bound to the operation.
package readmodel

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrOperationNotFound means no operation with that identifier is visible to
// the request's tenant. The caller cannot distinguish absence from another
// tenant's operation, which is what keeps cross-tenant existence undisclosed.
var ErrOperationNotFound = errors.New("readmodel: the operation is not visible in this tenant scope")

// ErrSnapshotExpired means a snapshot page was requested after the projection
// advanced past the coveredSeq its cursor binds. The pages of one snapshot must
// share a coveredSeq, and an advanced projection cannot be read backwards, so
// the handshake restarts instead of mixing two snapshots.
var ErrSnapshotExpired = errors.New("readmodel: the bound snapshot coveredSeq is no longer current")

// EventChannel is the PostgreSQL NOTIFY channel this service listens on for a
// wake hint. A notification only shortens the wait before the next shared
// catch-up read; a lost one costs at most SharedCatchupInterval, so the channel
// is never a source of truth about what was committed.
const EventChannel = "anvilkit_operation_event"

// Bounds mirrored from contracts/profiles/pilot-limits-v1.json. They are pilot
// defaults recorded by the architecture, not measured capacity.
const (
	// ReplayPageEvents is sse.replayPageEvents.
	ReplayPageEvents = 200
	// SnapshotPageSteps is the snapshot's own steps ceiling
	// (urn:anvilkit:operation-snapshot:v1 steps maxItems).
	SnapshotPageSteps = 200
	// SharedCatchupInterval is sse.sharedCatchupSeconds.
	SharedCatchupInterval = time.Second
)

// ReadModel serves the API's projection and event reads.
type ReadModel struct {
	pool *pgxpool.Pool
}

// Open connects the restricted read role. Pool sizing travels in the DSN
// (pgxpool reads pool_max_conns and its siblings from the connection string),
// because the architecture records connection-pool values as a per-environment
// input rather than a value this service may choose.
func Open(ctx context.Context, dsn string) (*ReadModel, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("readmodel: the read DSN is not usable: %w", err)
	}
	// The read role must never be used for a write, and a read-only session
	// makes that a server-enforced fact rather than a convention this package
	// is trusted to keep.
	config.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("readmodel: the read pool could not be created: %w", err)
	}
	return &ReadModel{pool: pool}, nil
}

// Close releases the pool.
func (m *ReadModel) Close() {
	if m != nil && m.pool != nil {
		m.pool.Close()
	}
}

// Ping reports whether the read role is currently reachable. Readiness depends
// on it, so it runs the cheapest statement that still proves a usable session.
func (m *ReadModel) Ping(ctx context.Context) error {
	return m.pool.Ping(ctx)
}

// Pool exposes the underlying pool for the listener, which needs a dedicated
// connection rather than a transaction.
func (m *ReadModel) Pool() *pgxpool.Pool { return m.pool }

// withTenantTx runs read inside a read-only transaction scoped to tenantID.
//
// set_config's third argument makes the setting transaction-local, so the
// tenant context is discarded when the transaction ends and cannot leak into
// the next request that borrows the same pooled connection. Repeatable read
// gives every statement in one read the same database snapshot, which is what
// lets a view, its coveredSeq and its events agree.
func (m *ReadModel) withTenantTx(ctx context.Context, tenantID string, read func(pgx.Tx) error) error {
	tx, err := m.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return fmt.Errorf("readmodel: starting the read transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, "SELECT set_config('anvilkit.tenant_id', $1, true)", tenantID); err != nil {
		return fmt.Errorf("readmodel: binding the tenant context: %w", err)
	}
	return read(tx)
}

// decimal renders a 64-bit counter as the bounded decimal string every public
// value contract requires. These values cross JavaScript, so they are never
// numbers on the wire.
func decimal(value int64) string {
	if value < 0 {
		return "0"
	}
	return strconv.FormatInt(value, 10)
}

// instant renders a timestamp as the validated RFC 3339 UTC-Z string the value
// contract requires.
func instant(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

// optionalInstant renders a nullable timestamp, returning "" for SQL NULL so
// the field is omitted rather than emitted as null.
func optionalInstant(value *time.Time) string {
	if value == nil {
		return ""
	}
	return instant(*value)
}

// optionalText unwraps a nullable text column the same way.
func optionalText(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
