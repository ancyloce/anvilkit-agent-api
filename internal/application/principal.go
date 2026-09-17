// Package application holds the API's use-case layer: verified principals,
// command identity derivation and the Control-facing port. It has no
// business database and no Temporal, Pagix or provider client (architecture
// communication matrix).
package application

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var (
	ErrUnauthenticated = errors.New("unauthenticated")
	ErrForbidden       = errors.New("forbidden")
)

// Principal is the verified caller. Scope comes from here, never from a
// request body or URL.
type Principal struct {
	TenantID  string
	ProjectID string
	ActorID   string
	Roles     []string
}

// Verifier turns a bearer token into a Principal through the real identity
// protocol (A12). The development fixture is one implementation.
type Verifier interface {
	Verify(ctx context.Context, bearer string) (Principal, error)
}

// CommandIdentity is the durable command identity the API derives from the
// principal and the canonical request body.
type CommandIdentity struct {
	TenantID      string
	CommandID     string
	ActorID       string
	RequestDigest string
}

// CanonicalDigest hashes the canonical JSON form of a decoded body (sorted
// members, no insignificant whitespace) so a client retry with different
// formatting still matches the original command.
func CanonicalDigest(decoded any) (string, error) {
	canonical, err := json.Marshal(decoded)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("sha256:%x", sha256.Sum256(canonical)), nil
}

// OperationView is the public projection returned by the API; it mirrors
// contracts/openapi/agent.yaml#/components/schemas/OperationView.
type OperationView struct {
	OperationID     string
	TenantID        string
	ProjectID       string
	ActorID         string
	Kind            string
	ProfileID       string
	SubjectDigest   string
	BriefID         string
	SourceRevision  string
	Lifecycle       string
	Phase           string
	Control         string
	Cleanup         string
	Finance         string
	Revision        string
	CoveredEventSeq string
	ExecutionEpoch  string
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Deadline        time.Time
	FailureCode     string
	// ActiveDeadline is set once by the first execution permit of a
	// generation; nil until then.
	ActiveDeadline *time.Time
	// Clarification is the open question set of a waiting preparation.
	Clarification *Clarification
}

// Clarification mirrors agent.yaml#/components/schemas/Clarification.
type Clarification struct {
	QuestionSetID       string
	QuestionSetRevision string
	Round               string
	AskedAt             time.Time
	ExpiresAt           time.Time
	Questions           []Question
}

type Question struct {
	QuestionID string
	Text       string
}

// PreparationIntake is what API-01 accepts: the prompt artifact and the
// selected brand/asset references.
type PreparationIntake struct {
	PromptTransferID string
	PromptDigest     string
	BrandReferences  []SourceReference
	AssetReferences  []SourceReference
}

type SourceReference struct {
	SourceID string
	Revision string
}

// AnswerReceipt mirrors agent.yaml#/components/schemas/AnswerReceipt.
type AnswerReceipt struct {
	AnswerID            string
	OperationID         string
	QuestionSetID       string
	QuestionSetRevision string
	UpdateID            string
	AcceptedAt          time.Time
}

type CommandReceipt struct {
	CommandID         string
	OperationID       string
	Kind              string
	Outcome           string
	OperationRevision string
	ReasonCode        string
	AcceptedAt        time.Time
	SettledAt         *time.Time
}

type EventFrame struct {
	OperationID  string
	EventSeq     string
	TransitionID string
	EventType    string
	Revision     string
	OccurredAt   time.Time
	Lifecycle    string
	Phase        string
	Control      string
	Cleanup      string
	Finance      string
	FailureCode  string
}

// TransferIntent is what a caller submits to begin a scoped artifact
// transfer (API-12): the class, the exact bytes it will upload and the
// operation or attempt the artifact belongs to. The deadline is set by the
// API from its reviewed transfer window; Control bounds it further.
type TransferIntent struct {
	Class          string
	MediaType      string
	ExpectedDigest string
	ExpectedSize   string
	OperationID    string
	AttemptID      string
	Deadline       time.Time
}

// UploadCapability is the scoped upload authorization Control issued for
// a begun transfer: one request of Method to URL with exactly Headers and
// the declared bytes, valid until ExpiresAt. It is returned only to the
// authenticated caller that began the transfer, never logged.
type UploadCapability struct {
	URL       string
	Method    string
	Headers   map[string]string
	ExpiresAt time.Time
}

// TransferView is the public projection of a transfer; it mirrors
// contracts/openapi/agent.yaml#/components/schemas/Transfer.
type TransferView struct {
	TransferID     string
	Handle         string
	Class          string
	ExpectedDigest string
	ExpectedSize   string
	State          string
	Deadline       time.Time
	ObjectVersion  string
	ReasonCode     string
	Upload         *UploadCapability
}

// ControlError carries the public error code decided by Control.
type ControlError struct {
	Code      string
	Message   string
	Retryable bool
}

func (e *ControlError) Error() string { return e.Code + ": " + e.Message }

// Control is the port to anvilkit-agent-control's OperationService.
type Control interface {
	CreateOperation(ctx context.Context, cmd CommandIdentity, p Principal, kind string, profileID, subjectDigest, briefID, sourceRevision string) (OperationView, error)
	// CreatePreparation is API-01 over Control's OperationService: the
	// reviewed preparation profile, the subject digest derived from the
	// canonical intake, and the intake itself.
	CreatePreparation(ctx context.Context, cmd CommandIdentity, p Principal, profileID, subjectDigest string, intake PreparationIntake) (OperationView, error)
	// SubmitAnswer is API-06 over Control's PreparationService.
	SubmitAnswer(ctx context.Context, cmd CommandIdentity, p Principal, operationID, questionSetID, questionSetRevision, transferID, digest string) (AnswerReceipt, error)
	GetOperation(ctx context.Context, p Principal, operationID string) (OperationView, error)
	SubmitCommand(ctx context.Context, cmd CommandIdentity, p Principal, operationID, kind, expectedRevision, targetActivation string) (CommandReceipt, error)
	GetCommand(ctx context.Context, p Principal, operationID, commandID string) (CommandReceipt, error)
	// StreamEvents calls emit for every durable event after the cursor until
	// the operation is terminal or ctx ends; a cursor that cannot be honored
	// returns ErrResetRequired with the covered sequence.
	StreamEvents(ctx context.Context, p Principal, operationID, afterSeq string, emit func(EventFrame) error) error
	// BeginTransfer and FinalizeTransfer are API-12 over Control's
	// ArtifactService; the API never touches the object store itself.
	BeginTransfer(ctx context.Context, cmd CommandIdentity, p Principal, intent TransferIntent) (TransferView, error)
	FinalizeTransfer(ctx context.Context, cmd CommandIdentity, p Principal, handle, objectVersion string) (TransferView, error)
}

// ResetRequired tells the SSE handler to send a reset frame.
type ResetRequired struct{ CoveredEventSeq string }

func (r *ResetRequired) Error() string { return "reset required" }
