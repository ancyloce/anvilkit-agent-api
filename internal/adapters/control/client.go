// Package control is the API's grpc-go client adapter for
// anvilkit.control.v1.OperationService. Transport identity (mTLS) is an
// ENV-03 deployment input; the development profile dials plaintext on
// loopback.
package control

import (
	"context"
	"errors"
	"io"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

type Client struct {
	conn *grpc.ClientConn
	ops  controlv1.OperationServiceClient
}

func Dial(address string) (*Client, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, ops: controlv1.NewOperationServiceClient(conn)}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func scope(p application.Principal) *controlv1.Scope {
	return &controlv1.Scope{TenantId: p.TenantID, ProjectId: p.ProjectID, ActorId: p.ActorID}
}

func command(cmd application.CommandIdentity) *controlv1.CommandIdentity {
	return &controlv1.CommandIdentity{TenantId: cmd.TenantID, CommandId: cmd.CommandID, ActorId: cmd.ActorID, RequestDigest: cmd.RequestDigest}
}

var kinds = map[string]controlv1.OperationKind{
	"preparation": controlv1.OperationKind_OPERATION_KIND_PREPARATION, "generation": controlv1.OperationKind_OPERATION_KIND_GENERATION,
	"refinement": controlv1.OperationKind_OPERATION_KIND_REFINEMENT, "preview_build": controlv1.OperationKind_OPERATION_KIND_PREVIEW_BUILD,
	"release": controlv1.OperationKind_OPERATION_KIND_RELEASE, "local_check": controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK,
}

var commandKinds = map[string]controlv1.CommandKind{
	"cancel": controlv1.CommandKind_COMMAND_KIND_CANCEL, "hold": controlv1.CommandKind_COMMAND_KIND_HOLD,
	"resume": controlv1.CommandKind_COMMAND_KIND_RESUME, "change_definition": controlv1.CommandKind_COMMAND_KIND_CHANGE_DEFINITION,
}

// enumWord turns PROTO_ENUM_NAME into the public lowercase word after its prefix.
func enumWord(name string, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}

func toView(v *controlv1.OperationView) application.OperationView {
	return application.OperationView{
		OperationID: v.GetOperationId(), TenantID: v.GetTenantId(), ProjectID: v.GetProjectId(), ActorID: v.GetActorId(),
		Kind: enumWord(v.GetKind().String(), "OPERATION_KIND_"), ProfileID: v.GetSubject().GetProfileId(), SubjectDigest: v.GetSubject().GetSubjectDigest(),
		BriefID: v.GetSubject().GetBriefId(), SourceRevision: v.GetSubject().GetSourceRevision(),
		Lifecycle: enumWord(v.GetLifecycle().String(), "LIFECYCLE_"), Phase: v.GetPhase(), Control: enumWord(v.GetControl().String(), "CONTROL_STATE_"),
		Cleanup: enumWord(v.GetCleanup().String(), "CLEANUP_STATE_"), Finance: enumWord(v.GetFinance().String(), "FINANCE_STATE_"),
		Revision: v.GetRevision(), CoveredEventSeq: v.GetCoveredEventSeq(), ExecutionEpoch: v.GetExecutionEpoch(),
		CreatedAt: v.GetCreatedAt().AsTime(), UpdatedAt: v.GetUpdatedAt().AsTime(), Deadline: v.GetDeadline().AsTime(), FailureCode: v.GetFailureCode(),
	}
}

func toReceipt(r *controlv1.CommandReceipt) application.CommandReceipt {
	out := application.CommandReceipt{
		CommandID: r.GetCommandId(), OperationID: r.GetOperationId(), Kind: enumWord(r.GetKind().String(), "COMMAND_KIND_"),
		Outcome: enumWord(r.GetOutcome().String(), "COMMAND_OUTCOME_"), OperationRevision: r.GetOperationRevision(), ReasonCode: r.GetReasonCode(),
		AcceptedAt: r.GetAcceptedAt().AsTime(),
	}
	if r.SettledAt != nil {
		t := r.GetSettledAt().AsTime()
		out.SettledAt = &t
	}
	return out
}

// mapErr turns a gRPC status into the public error code (contracts.md §4).
func mapErr(err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return &application.ControlError{Code: "DEPENDENCY_UNAVAILABLE", Message: "control unavailable", Retryable: true}
	}
	code, _, _ := strings.Cut(st.Message(), ":")
	switch st.Code() {
	case codes.NotFound:
		return &application.ControlError{Code: "NOT_FOUND", Message: "not found", Retryable: false}
	case codes.InvalidArgument:
		return &application.ControlError{Code: "INVALID_ARGUMENT", Message: st.Message(), Retryable: false}
	case codes.Aborted, codes.FailedPrecondition:
		if code == "" {
			code = "REVISION_CONFLICT"
		}
		return &application.ControlError{Code: code, Message: st.Message(), Retryable: false}
	case codes.ResourceExhausted:
		return &application.ControlError{Code: "CAPACITY_EXHAUSTED", Message: st.Message(), Retryable: true}
	case codes.Unavailable:
		if code == "EFFECT_UNCERTAIN" {
			return &application.ControlError{Code: "EFFECT_UNCERTAIN", Message: st.Message(), Retryable: true}
		}
		return &application.ControlError{Code: "DEPENDENCY_UNAVAILABLE", Message: "control unavailable", Retryable: true}
	case codes.Canceled, codes.DeadlineExceeded:
		return &application.ControlError{Code: "DEPENDENCY_UNAVAILABLE", Message: "control call ended", Retryable: true}
	default:
		return &application.ControlError{Code: "DEPENDENCY_UNAVAILABLE", Message: "control error", Retryable: true}
	}
}

func (c *Client) CreateOperation(ctx context.Context, cmd application.CommandIdentity, p application.Principal, kind, profileID, subjectDigest, briefID, sourceRevision string) (application.OperationView, error) {
	subject := &controlv1.OperationSubject{ProfileId: profileID, SubjectDigest: subjectDigest}
	if briefID != "" {
		subject.BriefId = &briefID
	}
	if sourceRevision != "" {
		subject.SourceRevision = &sourceRevision
	}
	resp, err := c.ops.CreateOperation(ctx, &controlv1.CreateOperationRequest{Command: command(cmd), Scope: scope(p), Kind: kinds[kind], Subject: subject})
	if err != nil {
		return application.OperationView{}, mapErr(err)
	}
	return toView(resp.GetOperation()), nil
}

func (c *Client) GetOperation(ctx context.Context, p application.Principal, operationID string) (application.OperationView, error) {
	resp, err := c.ops.GetOperation(ctx, &controlv1.GetOperationRequest{Scope: scope(p), OperationId: operationID})
	if err != nil {
		return application.OperationView{}, mapErr(err)
	}
	return toView(resp.GetOperation()), nil
}

func (c *Client) SubmitCommand(ctx context.Context, cmd application.CommandIdentity, p application.Principal, operationID, kind, expectedRevision, targetActivation string) (application.CommandReceipt, error) {
	req := &controlv1.SubmitCommandRequest{Command: command(cmd), Scope: scope(p), OperationId: operationID, Kind: commandKinds[kind], ExpectedRevision: expectedRevision}
	if targetActivation != "" {
		req.TargetDefinitionActivation = &targetActivation
	}
	resp, err := c.ops.SubmitCommand(ctx, req)
	if err != nil {
		return application.CommandReceipt{}, mapErr(err)
	}
	return toReceipt(resp.GetReceipt()), nil
}

func (c *Client) GetCommand(ctx context.Context, p application.Principal, operationID, commandID string) (application.CommandReceipt, error) {
	resp, err := c.ops.GetCommand(ctx, &controlv1.GetCommandRequest{Scope: scope(p), OperationId: operationID, CommandId: commandID})
	if err != nil {
		return application.CommandReceipt{}, mapErr(err)
	}
	return toReceipt(resp.GetReceipt()), nil
}

func (c *Client) StreamEvents(ctx context.Context, p application.Principal, operationID, afterSeq string, emit func(application.EventFrame) error) error {
	// Verify the cursor against the committed projection first so an expired
	// or overrun cursor becomes a reset frame instead of a silent gap.
	page, err := c.ops.ListOperationEvents(ctx, &controlv1.ListOperationEventsRequest{Scope: scope(p), OperationId: operationID, AfterEventSeq: afterSeq, Limit: 1})
	if err != nil {
		return mapErr(err)
	}
	if page.GetResetRequired() {
		return &application.ResetRequired{CoveredEventSeq: page.GetCoveredEventSeq()}
	}
	stream, err := c.ops.StreamOperationEvents(ctx, &controlv1.StreamOperationEventsRequest{Scope: scope(p), OperationId: operationID, AfterEventSeq: afterSeq})
	if err != nil {
		return mapErr(err)
	}
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return mapErr(err)
		}
		ev := msg.GetEvent()
		ch := ev.GetOperationChanged()
		frame := application.EventFrame{
			OperationID: ev.GetOperationId(), EventSeq: ev.GetEventSeq(), TransitionID: ev.GetTransitionId(), EventType: ev.GetEventType(),
			Revision: ev.GetRevision(), OccurredAt: ev.GetOccurredAt().AsTime(),
			Lifecycle: enumWord(ch.GetLifecycle().String(), "LIFECYCLE_"), Phase: ch.GetPhase(), Control: enumWord(ch.GetControl().String(), "CONTROL_STATE_"),
			Cleanup: enumWord(ch.GetCleanup().String(), "CLEANUP_STATE_"), Finance: enumWord(ch.GetFinance().String(), "FINANCE_STATE_"), FailureCode: ch.GetFailureCode(),
		}
		if err := emit(frame); err != nil {
			return err
		}
	}
}
