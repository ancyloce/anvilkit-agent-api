//go:build integration

// Package readmodel_test holds the evidence that needs a real PostgreSQL
// server: the queries, row-level security, snapshot paging, retention and
// notification behaviour of the read role, and the authorized read surface
// running against all of it.
//
// It is behind the integration build tag so an ordinary `go test ./...` never
// requires a database. Run it with:
//
//	ANVILKIT_API_TEST_ADMIN_DSN=... go test -tags integration ./internal/readmodel/
//
// A run without that variable reports SKIP, which is an unexecuted check and
// never evidence that the behaviour holds.
package readmodel_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/ancyloce/anvilkit-agent-api/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-api/internal/readmodel"
)

// The migration files are applied in the exact order their own headers record.
// Reordering them is a migration error, not a test detail.
var migrationOrder = []string{"roles-v1.sql", "activation-v1.sql", "control-v1.sql"}

const (
	apiLoginRole     = "anvilkit_api_integration"
	apiLoginPassword = "integration-only-not-a-deployment-secret"

	tenantA = "tenant-fixture-1"
	tenantB = "tenant-fixture-2"

	operationA = "op-integration-a"
	operationB = "op-integration-b"

	fixtureDigest = "sha256:" +
		"1111111111111111111111111111111111111111111111111111111111111111"
)

// adminDSN returns the provisioning connection or skips the run.
func adminDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("ANVILKIT_API_TEST_ADMIN_DSN")
	if dsn == "" {
		t.Skip("ANVILKIT_API_TEST_ADMIN_DSN is unset; this check is unexecuted, not passing")
	}
	return dsn
}

// provision rebuilds the Agent schema in a disposable database and returns an
// admin connection plus the DSN of the restricted read role.
//
// Everything here runs the parent's own migration files rather than a copy of
// the schema, so what the read model is tested against is the contract itself.
func provision(t *testing.T) (*pgx.Conn, string) {
	t.Helper()
	ctx := context.Background()

	config, err := pgx.ParseConfig(adminDSN(t))
	if err != nil {
		t.Fatalf("parsing the admin DSN: %v", err)
	}
	// The migration files carry several statements each, including function
	// bodies and SET ROLE, so they are sent as simple queries.
	config.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	admin, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		t.Fatalf("connecting as the provisioning role: %v", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })

	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		t.Fatalf("verifying the retained migration inputs: %v", err)
	}

	reset(t, admin)
	for _, name := range migrationOrder {
		if _, err := admin.Exec(ctx, string(inputs[name])); err != nil {
			t.Fatalf("applying %s: %v", name, err)
		}
		if name == "roles-v1.sql" {
			// Steps two and three run SET ROLE anvilkit_control_migrator, which
			// requires the connected role to be a member of it.
			if _, err := admin.Exec(ctx,
				"GRANT anvilkit_control_migrator TO CURRENT_USER"); err != nil {
				t.Fatalf("granting the migrator role: %v", err)
			}
		}
	}

	// The contract's roles are NOLOGIN groups; a deployment grants them to the
	// identity its service authenticates with, and so does this test.
	if _, err := admin.Exec(ctx, fmt.Sprintf(
		"CREATE ROLE %s LOGIN PASSWORD %s; GRANT anvilkit_api_ro TO %s",
		apiLoginRole, quoteLiteral(apiLoginPassword), apiLoginRole)); err != nil {
		t.Fatalf("creating the read login role: %v", err)
	}

	return admin, readRoleDSN(t)
}

func reset(t *testing.T, admin *pgx.Conn) {
	t.Helper()
	statements := []string{
		"DROP SCHEMA IF EXISTS agent_control CASCADE",
		"DROP SCHEMA IF EXISTS definition_contract CASCADE",
		fmt.Sprintf("DROP OWNED BY %s CASCADE", apiLoginRole),
		fmt.Sprintf("DROP ROLE IF EXISTS %s", apiLoginRole),
		"DROP ROLE IF EXISTS anvilkit_api_ro",
		"DROP ROLE IF EXISTS anvilkit_control_rw",
		"DROP ROLE IF EXISTS anvilkit_control_migrator",
	}
	for _, statement := range statements {
		// A role that never existed cannot own anything, so a failure here is
		// only interesting when the object it names is still present, which the
		// migration step below reports.
		_, _ = admin.Exec(context.Background(), statement)
	}
}

// readRoleDSN rewrites the admin DSN to connect as the restricted read role.
func readRoleDSN(t *testing.T) string {
	t.Helper()
	parsed, err := url.Parse(adminDSN(t))
	if err != nil {
		t.Fatalf("the admin DSN is not a URL: %v", err)
	}
	parsed.User = url.UserPassword(apiLoginRole, apiLoginPassword)
	return parsed.String()
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// seedOperation writes one synthetic operation and its activation. CD-02 allows
// explicitly synthetic records for exactly this purpose; nothing here enables
// business admission.
func seedOperation(t *testing.T, admin *pgx.Conn, operationID, tenantID string) {
	t.Helper()
	ctx := context.Background()

	activationID := "act-" + operationID
	if _, err := admin.Exec(ctx, `
		INSERT INTO definition_contract.immutable_records (digest, record_kind, canonical_bytes)
		VALUES ($1, 'definition', $2) ON CONFLICT DO NOTHING`,
		fixtureDigest, []byte(`{"schemaVersion":1}`)); err != nil {
		t.Fatalf("seeding the immutable definition: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		INSERT INTO definition_contract.activations (
			activation_id, family, definition_id, command_id, request_digest, definition_digest,
			runtime_profile_ref, validator_report_ref, activated_by, activated_at)
		VALUES ($1, 'component', $2, $3, $4, $4, 'runtime-fixture', 'report-fixture', 'developer-fixture-1', now())`,
		activationID, "def-"+operationID, "cmd-"+operationID, fixtureDigest); err != nil {
		t.Fatalf("seeding the activation: %v", err)
	}
	if _, err := admin.Exec(ctx, `
		INSERT INTO agent_control.operations (
			operation_id, tenant_id, actor_id, kind, intake_source, command_id, request_digest,
			activation_id, funding_state, public_status, business_stage, control_state, cleanup_state,
			accepted_at, next_event_seq)
		VALUES ($1, $2, 'developer-fixture-1', 'generation', 'api', $3, $4, $5,
			'reservation_confirmed', 'running', 'generating', 'running', 'not_required', now(), 1)`,
		operationID, tenantID, "cmd-"+operationID, fixtureDigest, activationID); err != nil {
		t.Fatalf("seeding the operation: %v", err)
	}
}

// commitEvent appends one durable event and advances the projection in the same
// transaction, which is how Control allocates a sequence under the operation
// row lock. notify chooses whether the wake hint is sent, so a lost
// notification can be tested as its own case.
func commitEvent(t *testing.T, admin *pgx.Conn, operationID string, payloadFiller int, notify bool) int64 {
	t.Helper()
	ctx := context.Background()

	tx, err := admin.Begin(ctx)
	if err != nil {
		t.Fatalf("beginning the append: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var sequence int64
	if err := tx.QueryRow(ctx, `
		UPDATE agent_control.operations SET next_event_seq = next_event_seq + 1, updated_at = now()
		WHERE operation_id = $1 RETURNING next_event_seq - 1`, operationID).Scan(&sequence); err != nil {
		t.Fatalf("allocating the event sequence: %v", err)
	}

	payload := map[string]any{
		"status":            "running",
		"businessStage":     "generating",
		"operationRevision": "1",
		"cleanupState":      "not_required",
	}
	if payloadFiller > 0 {
		payload["reasonCode"] = strings.Repeat("A", payloadFiller)
	}
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("encoding the event payload: %v", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO agent_control.operation_events (
			operation_id, event_seq, transition_id, event_type, schema_version, body, occurred_at)
		VALUES ($1, $2, $3, 'operation.lifecycle', 1, $4, now())`,
		operationID, sequence, fmt.Sprintf("t-%s-%d", operationID, sequence), string(body)); err != nil {
		t.Fatalf("appending the event: %v", err)
	}
	if notify {
		if _, err := tx.Exec(ctx, "SELECT pg_notify($1, $2)", readmodel.EventChannel, operationID); err != nil {
			t.Fatalf("sending the wake hint: %v", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("committing the append: %v", err)
	}
	return sequence
}

func openReadModel(t *testing.T, dsn string) *readmodel.ReadModel {
	t.Helper()
	reader, err := readmodel.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("opening the read model: %v", err)
	}
	t.Cleanup(reader.Close)
	return reader
}

// TestReadsAreScopedToTheRequestTenant proves row-level security answers from
// the tenant bound for that transaction, and that a pooled connection carries
// no tenant into the next request.
func TestReadsAreScopedToTheRequestTenant(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)
	seedOperation(t, admin, operationB, tenantB)

	reader := openReadModel(t, dsn)
	ctx := context.Background()

	if _, err := reader.ReadOperation(ctx, tenantA, operationA); err != nil {
		t.Fatalf("the owning tenant must read its own operation: %v", err)
	}
	if _, err := reader.ReadOperation(ctx, tenantB, operationA); err != readmodel.ErrOperationNotFound {
		t.Fatalf("another tenant's operation must be absent, got %v", err)
	}

	// The same pooled connections are reused here; a tenant setting that
	// outlived its transaction would make this read succeed.
	for attempt := 0; attempt < 20; attempt++ {
		if _, err := reader.ReadOperation(ctx, "tenant-never-seeded", operationA); err != readmodel.ErrOperationNotFound {
			t.Fatalf("an unmapped tenant must never see an operation, got %v", err)
		}
	}
}

// TestEventReplayIsGaplessAcrossPages holds replay to sequence order across
// more events than one page can carry.
func TestEventReplayIsGaplessAcrossPages(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)

	const committed = 450
	for index := 0; index < committed; index++ {
		commitEvent(t, admin, operationA, 0, false)
	}

	reader := openReadModel(t, dsn)
	ctx := context.Background()

	var (
		seen   []int64
		cursor int64
	)
	for {
		page, err := reader.ReadEvents(ctx, tenantA, operationA, cursor, readmodel.ReplayPageEvents)
		if err != nil {
			t.Fatalf("replaying after %d: %v", cursor, err)
		}
		if page.CoveredSeq != committed {
			t.Fatalf("expected coveredSeq %d, got %d", committed, page.CoveredSeq)
		}
		if len(page.Events) == 0 {
			break
		}
		if len(page.Events) > readmodel.ReplayPageEvents {
			t.Fatalf("a page carried %d events, above the contract's ceiling", len(page.Events))
		}
		for _, event := range page.Events {
			var sequence int64
			if _, err := fmt.Sscan(event.EventSeq, &sequence); err != nil {
				t.Fatalf("the event sequence is not a decimal counter: %q", event.EventSeq)
			}
			seen = append(seen, sequence)
			cursor = sequence
		}
	}

	if len(seen) != committed {
		t.Fatalf("expected %d events, replayed %d", committed, len(seen))
	}
	for index, sequence := range seen {
		if sequence != int64(index+1) {
			t.Fatalf("replay is not gapless: position %d carries sequence %d", index, sequence)
		}
	}
}

// TestRetentionFloorForcesTheSnapshotHandshake proves a cursor below the oldest
// retained event is reported as needing a snapshot rather than served with a
// silently skipped range.
func TestRetentionFloorForcesTheSnapshotHandshake(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)
	reader := openReadModel(t, dsn)
	ctx := context.Background()
	empty, err := reader.ReadEvents(ctx, tenantA, operationA, 0, readmodel.ReplayPageEvents)
	if err != nil || empty.RetainedFromSeq != 1 || empty.SnapshotRequired(0) {
		t.Fatalf("an operation with no committed events must permit its initial cursor: %+v, %v", empty, err)
	}
	for index := 0; index < 10; index++ {
		commitEvent(t, admin, operationA, 0, false)
	}

	// Retention prunes from the front of the stream. The append-only trigger
	// protects the running service from doing this, so the test suspends it the
	// way a retention job run by the schema owner would.
	if _, err := admin.Exec(ctx,
		`ALTER TABLE agent_control.operation_events DISABLE TRIGGER operation_events_append_only;
		 DELETE FROM agent_control.operation_events WHERE operation_id = $1 AND event_seq <= 4;
		 ALTER TABLE agent_control.operation_events ENABLE TRIGGER operation_events_append_only`,
		operationA); err != nil {
		t.Fatalf("pruning the retained range: %v", err)
	}

	page, err := reader.ReadEvents(ctx, tenantA, operationA, 0, readmodel.ReplayPageEvents)
	if err != nil {
		t.Fatalf("replaying after the prune: %v", err)
	}
	if page.RetainedFromSeq != 5 {
		t.Fatalf("expected a retention floor of 5, got %d", page.RetainedFromSeq)
	}
	for _, cursor := range []int64{0, 1, 3} {
		if !page.SnapshotRequired(cursor) {
			t.Fatalf("cursor %d predates retention and must require a snapshot", cursor)
		}
	}
	// Everything after sequence four is still retained, so that cursor and any
	// later one continue without a handshake.
	for _, cursor := range []int64{4, 7, 10} {
		if page.SnapshotRequired(cursor) {
			t.Fatalf("cursor %d is still fully retained and must not require a snapshot", cursor)
		}
	}

	if _, err := admin.Exec(ctx,
		`ALTER TABLE agent_control.operation_events DISABLE TRIGGER operation_events_append_only;
		 DELETE FROM agent_control.operation_events WHERE operation_id = $1;
		 ALTER TABLE agent_control.operation_events ENABLE TRIGGER operation_events_append_only`,
		operationA); err != nil {
		t.Fatalf("pruning the entire committed range: %v", err)
	}
	page, err = reader.ReadEvents(ctx, tenantA, operationA, 0, readmodel.ReplayPageEvents)
	if err != nil || len(page.Events) != 0 || page.CoveredSeq != 10 || page.RetainedFromSeq != 11 {
		t.Fatalf("the entirely pruned range must retain its committed position: %+v, %v", page, err)
	}
	if !page.SnapshotRequired(9) || page.SnapshotRequired(10) {
		t.Fatal("after full pruning, only cursors covering every committed event can resume directly")
	}
}

// TestSnapshotPagesAreGaplessAndBindOneCoveredSeq walks a paginated snapshot
// and proves every page reproduces the same pinned result set, that no step is
// repeated or skipped, and that work committed afterwards stays out of it.
func TestSnapshotPagesAreGaplessAndBindOneCoveredSeq(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)

	const steps = 450
	for index := 1; index <= steps; index++ {
		sequence := commitEvent(t, admin, operationA, 0, false)
		seedStep(t, admin, operationA, index, sequence)
	}

	reader := openReadModel(t, dsn)
	ctx := context.Background()

	first, more, err := reader.ReadSnapshot(ctx, tenantA, operationA, 0, 0)
	if err != nil {
		t.Fatalf("reading the first snapshot page: %v", err)
	}
	if !more {
		t.Fatal("expected the snapshot to be paginated")
	}
	boundCoveredSeq := first.CoveredSeq

	// A step committed after the snapshot must not appear in any of its pages.
	late := commitEvent(t, admin, operationA, 0, false)
	seedStep(t, admin, operationA, steps+1, late)

	seen := map[string]bool{}
	collect := func(page readmodel.Snapshot) {
		if page.CoveredSeq != boundCoveredSeq {
			t.Fatalf("a page reports coveredSeq %s, not the bound %s", page.CoveredSeq, boundCoveredSeq)
		}
		for _, step := range page.Steps {
			if seen[step.StepExecutionID] {
				t.Fatalf("step %s appears in two pages", step.StepExecutionID)
			}
			seen[step.StepExecutionID] = true
		}
	}
	collect(first)

	// The projection has advanced, so continuing this snapshot restarts the
	// handshake rather than mixing two of them.
	if _, _, err := reader.ReadSnapshot(ctx, tenantA, operationA, mustParse(t, boundCoveredSeq), len(first.Steps)); err != readmodel.ErrSnapshotExpired {
		t.Fatalf("expected the advanced projection to expire the snapshot, got %v", err)
	}

	// A snapshot taken now completes across its pages with no gap.
	seen = map[string]bool{}
	page, more, err := reader.ReadSnapshot(ctx, tenantA, operationA, 0, 0)
	if err != nil {
		t.Fatalf("restarting the snapshot: %v", err)
	}
	boundCoveredSeq = page.CoveredSeq
	offset := 0
	for {
		collect(page)
		offset += len(page.Steps)
		if !more {
			break
		}
		page, more, err = reader.ReadSnapshot(ctx, tenantA, operationA, mustParse(t, boundCoveredSeq), offset)
		if err != nil {
			t.Fatalf("reading snapshot page at offset %d: %v", offset, err)
		}
	}
	if len(seen) != steps+1 {
		t.Fatalf("expected %d distinct steps across the snapshot, got %d", steps+1, len(seen))
	}
}

func seedStep(t *testing.T, admin *pgx.Conn, operationID string, index int, coveredSeq int64) {
	t.Helper()
	if _, err := admin.Exec(context.Background(), `
		INSERT INTO agent_control.step_executions (
			step_execution_id, operation_id, definition_segment, definition_digest, step_id, visit,
			action_id, action_version, status, started_at, input_refs, output_refs, covered_seq)
		VALUES ($1, $2, 1, $3, $4, 1, 'component.code', '1.0.0', 'started', now(), '{}'::jsonb, '{}'::jsonb, $5)`,
		fmt.Sprintf("se-%s-%d", operationID, index), operationID, fixtureDigest,
		fmt.Sprintf("step-%d", index), coveredSeq); err != nil {
		t.Fatalf("seeding step %d: %v", index, err)
	}
}

func mustParse(t *testing.T, value string) int64 {
	t.Helper()
	var parsed int64
	if _, err := fmt.Sscan(value, &parsed); err != nil {
		t.Fatalf("%q is not a counter: %v", value, err)
	}
	return parsed
}

// TestListenDeliversAWakeHint proves the notification path works, and
// TestReadsSurviveALostNotify in the stream suite proves nothing depends on it.
func TestListenDeliversAWakeHint(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)

	reader := openReadModel(t, dsn)
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	hints := make(chan struct{}, 1)
	go reader.Listen(ctx, hints)

	// The listener needs its connection before a notification can reach it, and
	// a hint sent earlier is simply lost, which is allowed.
	deadline := time.Now().Add(10 * time.Second)
	for {
		commitEvent(t, admin, operationA, 0, true)
		select {
		case <-hints:
			return
		case <-time.After(200 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("no wake hint arrived within the deadline")
		}
	}
}

const localOperation = "op-integration-local"

// seedLocalCheck writes one synthetic succeeded local-check and its typed child
// record. CD-02 allows explicitly synthetic records for exactly this purpose;
// nothing here enables business admission or a real execution.
func seedLocalCheck(t *testing.T, admin *pgx.Conn, operationID, tenantID string, resolved bool) {
	t.Helper()
	ctx := context.Background()

	if _, err := admin.Exec(ctx, `
		INSERT INTO agent_control.operations (
			operation_id, tenant_id, actor_id, kind, intake_source, command_id, request_digest,
			funding_state, public_status, business_stage, control_state, cleanup_state,
			accepted_at, next_event_seq)
		VALUES ($1, $2, 'developer-fixture-1', 'local-check', 'api', $3, $4,
			'not_applicable', $5, $6, 'running', $7, now(), 1)`,
		operationID, tenantID, "cmd-"+operationID, fixtureDigest,
		statusFor(resolved), stageFor(resolved), cleanupFor(resolved)); err != nil {
		t.Fatalf("seeding the local-check operation: %v", err)
	}

	// An unresolved row holds the single local slot and carries no result; a
	// resolved one carries the accepted terminal content of the plain fixture.
	if _, err := admin.Exec(ctx, `
		INSERT INTO agent_control.local_checks (
			operation_id, fixture_id, profile_ref, profile_digest, input_digest, workflow_id, workflow_input,
			temporal_namespace, run_id, admission_execution_generation, admission_recovery_generation,
			start_state, start_attempted_at, start_established_at, start_window_expires_at, resolved,
			accepted_run_id, accepted_terminal_at, result_byte_length, result_content_digest,
			accepted_terminal_event_id, accepted_terminal_type, accepted_terminal_digest)
		VALUES ($1, 'plain-v1', 'local-check-v1', $2, $2, $3, jsonb_build_object('schemaVersion',1,'operationId',$1::text,'fixtureId','plain-v1'),
			'anvilkit-local', $4, 1, 1,
			'start_established', now(), now(), now() + interval '15 minutes', $5,
			$6, $7, $8, $9, CASE WHEN $5 THEN 9 ELSE NULL END, CASE WHEN $5 THEN 'completed' ELSE NULL END, CASE WHEN $5 THEN $2 ELSE NULL END)`,
		operationID, fixtureDigest, "local-check:"+operationID, "run-"+operationID, resolved,
		runIDIf(resolved, operationID), terminalIf(resolved),
		byteLengthIf(resolved), digestIf(resolved)); err != nil {
		t.Fatalf("seeding the local-check record: %v", err)
	}
}

func statusFor(resolved bool) string {
	if resolved {
		return "succeeded"
	}
	return "pending"
}

func stageFor(resolved bool) string {
	if resolved {
		return "ready"
	}
	return "queued"
}

func cleanupFor(resolved bool) string {
	if resolved {
		return "complete"
	}
	return "not_required"
}

func runIDIf(resolved bool, operationID string) *string {
	if !resolved {
		return nil
	}
	value := "run-" + operationID
	return &value
}

func terminalIf(resolved bool) *time.Time {
	if !resolved {
		return nil
	}
	value := time.Now().UTC()
	return &value
}

func byteLengthIf(resolved bool) *int64 {
	if !resolved {
		return nil
	}
	value := int64(8)
	return &value
}

// plainFixtureDigest is the digest CD-04 records for the exact bytes "AnvilKit".
const plainFixtureDigest = "sha256:a000b6af217bfeecf4385d7cede05f1dc904e6ced554ba162cde15a4bdde56fa"

func digestIf(resolved bool) *string {
	if !resolved {
		return nil
	}
	value := plainFixtureDigest
	return &value
}

// TestLocalCheckResultIsReadThroughTheRestrictedRole proves the API's half of
// the local-check read against the real schema: the restricted role can select
// the typed child record through its own tenant policy, the view carries the
// accepted result, and another tenant sees neither the operation nor its result.
func TestLocalCheckResultIsReadThroughTheRestrictedRole(t *testing.T) {
	admin, dsn := provision(t)
	seedLocalCheck(t, admin, localOperation, tenantA, true)

	reader := openReadModel(t, dsn)
	view, err := reader.ReadOperation(context.Background(), tenantA, localOperation)
	if err != nil {
		t.Fatalf("reading the local-check projection: %v", err)
	}
	if view.Kind != "local-check" || view.Status != "succeeded" {
		t.Fatalf("the projection reports %s/%s", view.Kind, view.Status)
	}
	if view.LocalCheckResult == nil {
		t.Fatal("a succeeded local-check discloses no result")
	}
	if view.LocalCheckResult.FixtureID != "plain-v1" ||
		view.LocalCheckResult.ByteLength != "8" ||
		view.LocalCheckResult.ContentDigest != plainFixtureDigest {
		t.Fatalf("the disclosed result is %+v", view.LocalCheckResult)
	}
	// No authority field of a business operation appears on this kind.
	if view.FinancialState != "" || view.FirstPermitAt != "" || view.ActiveDeadline != "" {
		t.Fatalf("the local-check projection carries a business authority field: %+v", view)
	}

	if _, err := reader.ReadOperation(context.Background(), tenantB, localOperation); err == nil {
		t.Fatal("another tenant read the local-check operation")
	}

	// A snapshot of the same operation reports the same committed result.
	snapshot, _, err := reader.ReadSnapshot(context.Background(), tenantA, localOperation, 0, 0)
	if err != nil {
		t.Fatalf("reading the local-check snapshot: %v", err)
	}
	if snapshot.Operation.LocalCheckResult == nil ||
		snapshot.Operation.LocalCheckResult.ContentDigest != plainFixtureDigest {
		t.Fatalf("the snapshot and the view disagree about the result: %+v", snapshot.Operation)
	}
	if len(snapshot.Steps) != 0 {
		t.Fatalf("a local-check snapshot carries %d steps", len(snapshot.Steps))
	}
}

// TestLocalCheckStorageConstraintsHoldAtTheDatabase proves the approved
// local-only exceptions are enforced by the schema rather than by a service
// that could forget them: a business kind cannot borrow the local funding
// state or omit its activation, a local-check cannot carry business authority,
// and only one unresolved local-check exists at a time.
func TestLocalCheckStorageConstraintsHoldAtTheDatabase(t *testing.T) {
	admin, _ := provision(t)
	ctx := context.Background()
	seedLocalCheck(t, admin, localOperation, tenantA, false)

	insertOperation := `
		INSERT INTO agent_control.operations (
			operation_id, tenant_id, actor_id, kind, intake_source, command_id, request_digest,
			activation_id, funding_state, public_status, business_stage, control_state, cleanup_state,
			accepted_at, next_event_seq)
		VALUES ($1, $2, 'developer-fixture-1', $3, 'api', $4, $5, $6, $7, 'pending', 'queued', 'running',
			'not_required', now(), 1)`

	for _, refused := range []struct {
		name         string
		operationID  string
		kind         string
		activationID any
		fundingState string
	}{
		{"generation borrowing the local funding state", "op-refused-1", "generation", "act-op-integration-a", "not_applicable"},
		{"generation without an activation", "op-refused-2", "generation", nil, "reservation_confirmed"},
		{"local-check carrying an activation", "op-refused-3", "local-check", "act-op-integration-a", "not_applicable"},
		{"local-check with a business funding state", "op-refused-4", "local-check", nil, "reservation_confirmed"},
	} {
		t.Run(refused.name, func(t *testing.T) {
			_, err := admin.Exec(ctx, insertOperation, refused.operationID, tenantA, refused.kind,
				"cmd-"+refused.operationID, fixtureDigest, refused.activationID, refused.fundingState)
			if err == nil {
				t.Fatal("the database accepted a combination the approved exceptions forbid")
			}
		})
	}

	t.Run("a second unresolved local-check", func(t *testing.T) {
		second := "op-integration-local-2"
		seedOperationRowForLocalCheck(t, admin, second, tenantA)
		_, err := admin.Exec(ctx, `
			INSERT INTO agent_control.local_checks (
				operation_id, fixture_id, profile_ref, profile_digest, input_digest, workflow_id, workflow_input,
				temporal_namespace, admission_execution_generation, admission_recovery_generation,
				start_state, start_window_expires_at, resolved)
			VALUES ($1, 'newline-v1', 'local-check-v1', $2, $2, $3, jsonb_build_object('schemaVersion',1,'operationId',$1::text,'fixtureId','newline-v1'), 'anvilkit-local', 1, 1,
				'start_unattempted', now() + interval '15 minutes', false)`,
			second, fixtureDigest, "local-check:"+second)
		var constraint *pgconn.PgError
		if !errors.As(err, &constraint) || constraint.Code != "23505" || constraint.ConstraintName != "local_checks_single_unresolved_idx" {
			t.Fatalf("expected the unresolved-slot uniqueness gate, got %v", err)
		}
	})

	t.Run("an accepted result is never overwritten", func(t *testing.T) {
		if _, err := admin.Exec(ctx, `
			UPDATE agent_control.local_checks
			SET resolved = true, accepted_run_id = $2, accepted_terminal_at = now(),
			    result_byte_length = 8, result_content_digest = $3,
			    accepted_terminal_event_id=9, accepted_terminal_type='completed', accepted_terminal_digest=$3
			WHERE operation_id = $1`,
			localOperation, "run-"+localOperation, plainFixtureDigest); err != nil {
			t.Fatalf("accepting the first terminal result: %v", err)
		}
		_, err := admin.Exec(ctx, `
			UPDATE agent_control.local_checks SET result_byte_length = 9, result_content_digest = $2
			WHERE operation_id = $1`, localOperation, fixtureDigest)
		if err == nil {
			t.Fatal("the database overwrote an accepted local-check result")
		}
	})
}

// seedOperationRowForLocalCheck writes only the operation row of a local-check,
// so a test can attempt the child record on its own.
func seedOperationRowForLocalCheck(t *testing.T, admin *pgx.Conn, operationID, tenantID string) {
	t.Helper()
	if _, err := admin.Exec(context.Background(), `
		INSERT INTO agent_control.operations (
			operation_id, tenant_id, actor_id, kind, intake_source, command_id, request_digest,
			funding_state, public_status, business_stage, control_state, cleanup_state,
			accepted_at, next_event_seq)
		VALUES ($1, $2, 'developer-fixture-1', 'local-check', 'api', $3, $4,
			'not_applicable', 'pending', 'queued', 'running', 'not_required', now(), 1)`,
		operationID, tenantID, "cmd-"+operationID, fixtureDigest); err != nil {
		t.Fatalf("seeding the local-check operation row: %v", err)
	}
}
