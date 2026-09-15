package http

import (
	"context"
	"crypto/rand"
	"encoding/base32"

	"github.com/gin-gonic/gin"

	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

// strictHandlers implements the generated StrictServerInterface. Operations
// whose owning service is not part of this unit (Knowledge, MCP, artifacts,
// preparation) answer DEPENDENCY_UNAVAILABLE; they are wired by P08/P13/
// P15–P19 without changing the public contract.
type strictHandlers struct {
	control application.Control
}

func newRequestID() string {
	var b [10]byte
	_, _ = rand.Read(b[:])
	return "req_" + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:])
}

// ginContext recovers the *gin.Context the generated strict handler passes
// as the context.
func ginContext(ctx context.Context) *gin.Context {
	c, _ := ctx.(*gin.Context)
	return c
}

func identity(ctx context.Context, commandID string) (application.CommandIdentity, application.Principal) {
	c := ginContext(ctx)
	p := principal(c)
	digest, _ := c.Get(ctxDigest)
	d, _ := digest.(string)
	return application.CommandIdentity{TenantID: p.TenantID, CommandID: commandID, ActorID: p.ActorID, RequestDigest: d}, p
}

func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func viewToPublic(v application.OperationView) agentapi.OperationView {
	out := agentapi.OperationView{
		OperationId: v.OperationID, TenantId: v.TenantID, ActorId: v.ActorID, Kind: agentapi.OperationKind(v.Kind),
		Subject:   agentapi.OperationSubject{ProfileId: v.ProfileID, SubjectDigest: v.SubjectDigest, BriefId: optStr(v.BriefID), SourceRevision: optStr(v.SourceRevision)},
		Lifecycle: agentapi.Lifecycle(v.Lifecycle), Phase: v.Phase, Control: agentapi.ControlState(v.Control), Cleanup: agentapi.CleanupState(v.Cleanup),
		Finance: agentapi.FinanceState(v.Finance), Revision: v.Revision, CoveredEventSeq: v.CoveredEventSeq, ExecutionEpoch: v.ExecutionEpoch,
		CreatedAt: v.CreatedAt.UTC(), UpdatedAt: v.UpdatedAt.UTC(), Deadline: v.Deadline.UTC(), FailureCode: optStr(v.FailureCode), ProjectId: optStr(v.ProjectID),
	}
	return out
}

func receiptToPublic(r application.CommandReceipt) agentapi.CommandReceipt {
	out := agentapi.CommandReceipt{
		CommandId: r.CommandID, OperationId: r.OperationID, Kind: agentapi.CommandKind(r.Kind), Outcome: agentapi.CommandOutcome(r.Outcome),
		OperationRevision: r.OperationRevision, ReasonCode: optStr(r.ReasonCode), AcceptedAt: r.AcceptedAt.UTC(),
	}
	if r.SettledAt != nil {
		t := r.SettledAt.UTC()
		out.SettledAt = &t
	}
	return out
}

func (h *strictHandlers) CreateOperation(ctx context.Context, req agentapi.CreateOperationRequestObject) (agentapi.CreateOperationResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	s := req.Body.Subject
	view, err := h.control.CreateOperation(ctx, cmd, p, string(req.Body.Kind), s.ProfileId, s.SubjectDigest, deref(s.BriefId), deref(s.SourceRevision))
	if err != nil {
		return nil, err
	}
	return agentapi.CreateOperation202JSONResponse(viewToPublic(view)), nil
}

func (h *strictHandlers) GetOperation(ctx context.Context, req agentapi.GetOperationRequestObject) (agentapi.GetOperationResponseObject, error) {
	view, err := h.control.GetOperation(ctx, principal(ginContext(ctx)), req.OperationId)
	if err != nil {
		return nil, err
	}
	return agentapi.GetOperation200JSONResponse(viewToPublic(view)), nil
}

func (h *strictHandlers) SubmitCommand(ctx context.Context, req agentapi.SubmitCommandRequestObject) (agentapi.SubmitCommandResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	r, err := h.control.SubmitCommand(ctx, cmd, p, req.OperationId, string(req.Body.Kind), req.Body.ExpectedRevision, deref(req.Body.TargetDefinitionActivation))
	if err != nil {
		return nil, err
	}
	return agentapi.SubmitCommand202JSONResponse(receiptToPublic(r)), nil
}

func (h *strictHandlers) GetCommand(ctx context.Context, req agentapi.GetCommandRequestObject) (agentapi.GetCommandResponseObject, error) {
	r, err := h.control.GetCommand(ctx, principal(ginContext(ctx)), req.OperationId, req.CommandId)
	if err != nil {
		return nil, err
	}
	return agentapi.GetCommand200JSONResponse(receiptToPublic(r)), nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func notDeployed(service string) error {
	return &application.ControlError{Code: "DEPENDENCY_UNAVAILABLE", Message: service + " is not deployed in this unit", Retryable: false}
}

func (h *strictHandlers) CreatePreparation(context.Context, agentapi.CreatePreparationRequestObject) (agentapi.CreatePreparationResponseObject, error) {
	return nil, notDeployed("preparation (P13)")
}
func (h *strictHandlers) SubmitAnswer(context.Context, agentapi.SubmitAnswerRequestObject) (agentapi.SubmitAnswerResponseObject, error) {
	return nil, notDeployed("preparation (P13)")
}
func (h *strictHandlers) BeginTransfer(context.Context, agentapi.BeginTransferRequestObject) (agentapi.BeginTransferResponseObject, error) {
	return nil, notDeployed("artifact transfer (P08)")
}
func (h *strictHandlers) FinalizeTransfer(context.Context, agentapi.FinalizeTransferRequestObject) (agentapi.FinalizeTransferResponseObject, error) {
	return nil, notDeployed("artifact transfer (P08)")
}
func (h *strictHandlers) Search(context.Context, agentapi.SearchRequestObject) (agentapi.SearchResponseObject, error) {
	return nil, notDeployed("knowledge (P16)")
}
func (h *strictHandlers) RegisterSource(context.Context, agentapi.RegisterSourceRequestObject) (agentapi.RegisterSourceResponseObject, error) {
	return nil, notDeployed("knowledge (P15)")
}
func (h *strictHandlers) DeleteSource(context.Context, agentapi.DeleteSourceRequestObject) (agentapi.DeleteSourceResponseObject, error) {
	return nil, notDeployed("knowledge (P15)")
}
func (h *strictHandlers) GetSource(context.Context, agentapi.GetSourceRequestObject) (agentapi.GetSourceResponseObject, error) {
	return nil, notDeployed("knowledge (P15)")
}
func (h *strictHandlers) ReplaceSourceAccess(context.Context, agentapi.ReplaceSourceAccessRequestObject) (agentapi.ReplaceSourceAccessResponseObject, error) {
	return nil, notDeployed("knowledge (P15)")
}
func (h *strictHandlers) ProposeMemory(context.Context, agentapi.ProposeMemoryRequestObject) (agentapi.ProposeMemoryResponseObject, error) {
	return nil, notDeployed("knowledge memory (P17)")
}
func (h *strictHandlers) DecideMemory(context.Context, agentapi.DecideMemoryRequestObject) (agentapi.DecideMemoryResponseObject, error) {
	return nil, notDeployed("knowledge memory (P17)")
}
func (h *strictHandlers) CreateToolCall(context.Context, agentapi.CreateToolCallRequestObject) (agentapi.CreateToolCallResponseObject, error) {
	return nil, notDeployed("mcp (P19)")
}
func (h *strictHandlers) GetToolCall(context.Context, agentapi.GetToolCallRequestObject) (agentapi.GetToolCallResponseObject, error) {
	return nil, notDeployed("mcp (P19)")
}
func (h *strictHandlers) ListCatalog(context.Context, agentapi.ListCatalogRequestObject) (agentapi.ListCatalogResponseObject, error) {
	return nil, notDeployed("mcp (P18)")
}
func (h *strictHandlers) ReviewDescriptor(context.Context, agentapi.ReviewDescriptorRequestObject) (agentapi.ReviewDescriptorResponseObject, error) {
	return nil, notDeployed("mcp (P18)")
}
func (h *strictHandlers) CreateGrant(context.Context, agentapi.CreateGrantRequestObject) (agentapi.CreateGrantResponseObject, error) {
	return nil, notDeployed("mcp (P18)")
}
func (h *strictHandlers) GetGrant(context.Context, agentapi.GetGrantRequestObject) (agentapi.GetGrantResponseObject, error) {
	return nil, notDeployed("mcp (P18)")
}
func (h *strictHandlers) RevokeGrant(context.Context, agentapi.RevokeGrantRequestObject) (agentapi.RevokeGrantResponseObject, error) {
	return nil, notDeployed("mcp (P18)")
}
