package readmodel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// OperationView is urn:anvilkit:operation-view:v1#/$defs/OperationView.
//
// Every optional member is omitted rather than emptied, because omission is the
// contract's presence signal: a field that does not apply to an operation must
// not appear at all. cancelRequested is declared const true, so it appears only
// while a cancellation is outstanding.
type OperationView struct {
	OperationID             string            `json:"operationId"`
	Kind                    string            `json:"kind"`
	Status                  string            `json:"status"`
	BusinessStage           string            `json:"businessStage"`
	ControlState            string            `json:"controlState"`
	CleanupState            string            `json:"cleanupState"`
	FinancialState          string            `json:"financialState,omitempty"`
	ChangeState             string            `json:"changeState,omitempty"`
	CoveredSeq              string            `json:"coveredSeq"`
	OperationRevision       string            `json:"operationRevision"`
	DefinitionSegment       string            `json:"definitionSegment"`
	CurrentStep             string            `json:"currentStep,omitempty"`
	StepExecutionID         string            `json:"stepExecutionId,omitempty"`
	AcceptedAt              string            `json:"acceptedAt"`
	QueueExpiresAt          string            `json:"queueExpiresAt,omitempty"`
	FirstPermitAt           string            `json:"firstPermitAt,omitempty"`
	ActiveDeadline          string            `json:"activeDeadline,omitempty"`
	CancelRequested         bool              `json:"cancelRequested,omitempty"`
	ExpiryReason            string            `json:"expiryReason,omitempty"`
	IntendedTerminalOutcome string            `json:"intendedTerminalOutcome,omitempty"`
	LocalCheckResult        *LocalCheckResult `json:"localCheckResult,omitempty"`
	Reconciliation          *Reconciliation   `json:"reconciliation,omitempty"`
	// Test billing and the preparation extension (S0-T04, 2026-09-12). The
	// projection carries the server-bound funding authority, the bound quote
	// reference and the preparation members; this service does not yet read
	// the committed columns that fill them, so they stay omitted until the
	// preparation and test-billing slices are implemented.
	FundingAuthority     string                 `json:"fundingAuthority,omitempty"`
	AuthorizedFundingRef string                 `json:"authorizedFundingRef,omitempty"`
	Preparation          *PreparationProjection `json:"preparation,omitempty"`
}

// PreparationProjection is urn:anvilkit:preparation:v1#/$defs/PreparationProjection:
// the preparation members of the view. Artifact references stay raw so the
// committed reference bytes reach the client exactly as Control wrote them;
// question, answer and brief text never enter the projection.
type PreparationProjection struct {
	Round                string          `json:"round"`
	QuestionSetRef       json.RawMessage `json:"questionSetRef,omitempty"`
	QuestionSetRevision  string          `json:"questionSetRevision,omitempty"`
	QuestionSetExpiresAt string          `json:"questionSetExpiresAt,omitempty"`
	AcceptedAnswerSetRef json.RawMessage `json:"acceptedAnswerSetRef,omitempty"`
	BriefRef             json.RawMessage `json:"briefRef,omitempty"`
	BriefRevision        string          `json:"briefRevision,omitempty"`
	ContentRejections    string          `json:"contentRejections,omitempty"`
}

// LocalCheckResult is urn:anvilkit:operation-view:v1#/$defs/LocalCheckResult:
// the verified content of one successful local-check.
//
// It is present only when the committed local_checks row carries an accepted
// terminal result, which the storage contract permits only for a resolved
// local-check. The retained fixture bytes are never disclosed; only their
// length and digest are.
type LocalCheckResult struct {
	FixtureID     string `json:"fixtureId"`
	ByteLength    string `json:"byteLength"`
	ContentDigest string `json:"contentDigest"`
}

// Reconciliation is the view's uncertain-effect detail.
//
// The API's read role reaches agent_control.operations, step_executions and
// operation_events only; the effect identities this object names live in
// agent_control.effects, which that role cannot select and which the operations
// projection does not carry. No committed record available to this service can
// populate it today, so it is never filled here. The field is kept because it
// belongs to the public contract, and the gap is recorded in this service's
// README rather than closed by inventing a column or a value.
type Reconciliation struct {
	ReasonCode       string   `json:"reasonCode"`
	UnknownEffectIDs []string `json:"unknownEffectIds"`
	Since            string   `json:"since"`
}

// StepExecution is urn:anvilkit:operation-view:v1#/$defs/StepExecution.
//
// inputRefs and outputRefs stay raw so the committed reference bytes reach the
// client exactly as Control wrote them; re-encoding them here could only lose
// or reshape a reference this service is not the authority for.
type StepExecution struct {
	StepExecutionID   string          `json:"stepExecutionId"`
	OperationID       string          `json:"operationId"`
	DefinitionSegment string          `json:"definitionSegment"`
	DefinitionDigest  string          `json:"definitionDigest"`
	StepID            string          `json:"stepId"`
	Visit             string          `json:"visit"`
	ActionID          string          `json:"actionId"`
	ActionVersion     string          `json:"actionVersion"`
	Status            string          `json:"status"`
	Outcome           string          `json:"outcome,omitempty"`
	StartedAt         string          `json:"startedAt"`
	EndedAt           string          `json:"endedAt,omitempty"`
	InputRefs         json.RawMessage `json:"inputRefs"`
	OutputRefs        json.RawMessage `json:"outputRefs"`
	CoveredSeq        string          `json:"coveredSeq"`
	ProfileRef        string          `json:"profileRef,omitempty"`
}

// Snapshot is urn:anvilkit:operation-snapshot:v1#/$defs/OperationSnapshotV1.
type Snapshot struct {
	SchemaVersion   int             `json:"schemaVersion"`
	OperationID     string          `json:"operationId"`
	CoveredSeq      string          `json:"coveredSeq"`
	GeneratedAt     string          `json:"generatedAt"`
	Operation       OperationView   `json:"operation"`
	Steps           []StepExecution `json:"steps"`
	NextStepsCursor string          `json:"nextStepsCursor,omitempty"`
}

// operationSelect reads the projection and, for a local-check, the accepted
// result of its typed child record. The join is left outer because every other
// kind has no such record, and the API read role reaches local_checks only
// through the same tenant policy that guards the operation row itself.
const operationSelect = `
	SELECT o.operation_id, o.kind, o.public_status, o.business_stage, o.control_state, o.cleanup_state,
	       o.financial_state, o.change_state, o.next_event_seq - 1, o.operation_revision, o.definition_segment,
	       o.current_step_id, o.current_step_execution_id, o.accepted_at, o.queue_expires_at, o.first_permit_at,
	       o.active_deadline, o.cancel_requested, o.expiry_reason, o.intended_terminal_outcome,
	       l.fixture_id, l.result_byte_length, l.result_content_digest
	FROM agent_control.operations o
	LEFT JOIN agent_control.local_checks l ON l.operation_id = o.operation_id
	WHERE o.operation_id = $1`

// ReadOperation returns the committed public view of one operation.
func (m *ReadModel) ReadOperation(ctx context.Context, tenantID, operationID string) (OperationView, error) {
	var view OperationView
	err := m.withTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		var scanErr error
		view, scanErr = scanOperation(tx.QueryRow(ctx, operationSelect, operationID))
		return scanErr
	})
	return view, err
}

// ReadSnapshot returns one page of an authorized consistent snapshot and
// reports whether a further page exists.
//
// A zero offset with an unset boundCoveredSeq starts a new snapshot at the
// operation's current coveredSeq. A continuing page carries the coveredSeq its
// first page bound: steps are filtered at that sequence, which pins the ordered
// result set, so an offset into it can neither repeat nor skip a step. The
// operation row must still be at that sequence; an advanced projection cannot
// be read backwards, so it reports ErrSnapshotExpired and the client restarts
// the handshake rather than receiving pages from two different snapshots.
func (m *ReadModel) ReadSnapshot(
	ctx context.Context,
	tenantID, operationID string,
	boundCoveredSeq int64,
	stepOffset int,
) (Snapshot, bool, error) {
	var (
		snapshot Snapshot
		more     bool
	)
	err := m.withTenantTx(ctx, tenantID, func(tx pgx.Tx) error {
		view, err := scanOperation(tx.QueryRow(ctx, operationSelect, operationID))
		if err != nil {
			return err
		}

		coveredSeq, err := parseDecimal(view.CoveredSeq)
		if err != nil {
			return err
		}
		if stepOffset > 0 {
			if coveredSeq != boundCoveredSeq {
				return ErrSnapshotExpired
			}
		} else {
			boundCoveredSeq = coveredSeq
		}

		// One row past the page ceiling distinguishes a full last page from a
		// page with a successor, without a second count query.
		rows, err := tx.Query(ctx, `
			SELECT step_execution_id, operation_id, definition_segment, definition_digest, step_id, visit,
			       action_id, action_version, status, outcome, started_at, ended_at,
			       input_refs, output_refs, covered_seq, profile_ref
			FROM agent_control.step_executions
			WHERE operation_id = $1 AND covered_seq <= $2
			ORDER BY covered_seq, step_execution_id
			LIMIT $3 OFFSET $4`,
			operationID, boundCoveredSeq, SnapshotPageSteps+1, stepOffset)
		if err != nil {
			return fmt.Errorf("readmodel: reading the snapshot steps: %w", err)
		}
		defer rows.Close()

		steps := make([]StepExecution, 0, SnapshotPageSteps)
		for rows.Next() {
			step, scanErr := scanStep(rows)
			if scanErr != nil {
				return scanErr
			}
			if len(steps) == SnapshotPageSteps {
				more = true
				break
			}
			steps = append(steps, step)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("readmodel: reading the snapshot steps: %w", err)
		}

		snapshot = Snapshot{
			SchemaVersion: 1,
			OperationID:   view.OperationID,
			CoveredSeq:    decimal(boundCoveredSeq),
			GeneratedAt:   instant(time.Now()),
			Operation:     view,
			Steps:         steps,
		}
		return nil
	})
	return snapshot, more, err
}

// row is the part of pgx.Row and pgx.Rows this file scans through.
type row interface {
	Scan(destinations ...any) error
}

func scanOperation(source row) (OperationView, error) {
	var (
		view                                                      OperationView
		financialState, changeState, currentStep, stepExecutionID *string
		expiryReason, intendedTerminalOutcome                     *string
		coveredSeq, operationRevision, definitionSegment          int64
		acceptedAt                                                time.Time
		queueExpiresAt, firstPermitAt, activeDeadline             *time.Time
		cancelRequested                                           bool
		localFixtureID, localContentDigest                        *string
		localByteLength                                           *int64
	)
	err := source.Scan(
		&view.OperationID, &view.Kind, &view.Status, &view.BusinessStage, &view.ControlState, &view.CleanupState,
		&financialState, &changeState, &coveredSeq, &operationRevision, &definitionSegment,
		&currentStep, &stepExecutionID, &acceptedAt, &queueExpiresAt, &firstPermitAt,
		&activeDeadline, &cancelRequested, &expiryReason, &intendedTerminalOutcome,
		&localFixtureID, &localByteLength, &localContentDigest,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationView{}, ErrOperationNotFound
	}
	if err != nil {
		return OperationView{}, fmt.Errorf("readmodel: reading the operation projection: %w", err)
	}

	view.FinancialState = optionalText(financialState)
	view.ChangeState = optionalText(changeState)
	view.CoveredSeq = decimal(coveredSeq)
	view.OperationRevision = decimal(operationRevision)
	view.DefinitionSegment = decimal(definitionSegment)
	view.CurrentStep = optionalText(currentStep)
	view.StepExecutionID = optionalText(stepExecutionID)
	view.AcceptedAt = instant(acceptedAt)
	view.QueueExpiresAt = optionalInstant(queueExpiresAt)
	view.FirstPermitAt = optionalInstant(firstPermitAt)
	view.ActiveDeadline = optionalInstant(activeDeadline)
	view.CancelRequested = cancelRequested
	// The database retains the fence after termination; the public flag means
	// cancellation is still outstanding and is forbidden on terminal views.
	switch view.Status {
	case "succeeded", "failed", "canceled", "expired":
		view.CancelRequested = false
	}
	view.ExpiryReason = optionalText(expiryReason)
	view.IntendedTerminalOutcome = optionalText(intendedTerminalOutcome)
	// The public contract allows this member only on a succeeded local-check, so
	// an accepted result is reported only once the projection says the operation
	// succeeded. A result committed ahead of its projection is not disclosed as
	// one, because the view would then describe a state that never existed.
	if localFixtureID != nil && localByteLength != nil && localContentDigest != nil && view.Status == "succeeded" {
		view.LocalCheckResult = &LocalCheckResult{
			FixtureID:     *localFixtureID,
			ByteLength:    decimal(*localByteLength),
			ContentDigest: *localContentDigest,
		}
	}
	return view, nil
}

func scanStep(source row) (StepExecution, error) {
	var (
		step                                 StepExecution
		outcome, profileRef                  *string
		definitionSegment, visit, coveredSeq int64
		startedAt                            time.Time
		endedAt                              *time.Time
		inputRefs, outputRefs                []byte
	)
	if err := source.Scan(
		&step.StepExecutionID, &step.OperationID, &definitionSegment, &step.DefinitionDigest, &step.StepID, &visit,
		&step.ActionID, &step.ActionVersion, &step.Status, &outcome, &startedAt, &endedAt,
		&inputRefs, &outputRefs, &coveredSeq, &profileRef,
	); err != nil {
		return StepExecution{}, fmt.Errorf("readmodel: reading a step projection: %w", err)
	}

	step.DefinitionSegment = decimal(definitionSegment)
	step.Visit = decimal(visit)
	step.Outcome = optionalText(outcome)
	step.StartedAt = instant(startedAt)
	step.EndedAt = optionalInstant(endedAt)
	step.InputRefs = json.RawMessage(inputRefs)
	step.OutputRefs = json.RawMessage(outputRefs)
	step.CoveredSeq = decimal(coveredSeq)
	step.ProfileRef = optionalText(profileRef)
	return step, nil
}
