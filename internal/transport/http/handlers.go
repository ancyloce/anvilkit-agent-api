package http

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base32"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

// strictHandlers implements the generated StrictServerInterface: Control
// for operations, preparations, previews and artifacts, the Knowledge and
// MCP gateways for API-07..API-16 (P20).
type strictHandlers struct {
	control application.Control
	// knowledge and mcp are the gateways to the owning services (P20);
	// without a placement they answer DEPENDENCY_UNAVAILABLE.
	knowledge application.Knowledge
	mcp       application.MCP
	// transferWindow is the reviewed deadline the API sets on a transfer
	// it begins; Control bounds it further by the operation and attempt.
	transferWindow time.Duration
	// preparationProfile is the reviewed operation profile of API-01.
	preparationProfile string
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
		Subject: agentapi.OperationSubject{ProfileId: v.ProfileID, SubjectDigest: v.SubjectDigest, BriefId: optStr(v.BriefID), SourceRevision: optStr(v.SourceRevision),
			SourceHandle: optStr(v.SourceHandle)},
		Lifecycle: agentapi.Lifecycle(v.Lifecycle), Phase: v.Phase, Control: agentapi.ControlState(v.Control), Cleanup: agentapi.CleanupState(v.Cleanup),
		Finance: agentapi.FinanceState(v.Finance), Revision: v.Revision, CoveredEventSeq: v.CoveredEventSeq, ExecutionEpoch: v.ExecutionEpoch,
		CreatedAt: v.CreatedAt.UTC(), UpdatedAt: v.UpdatedAt.UTC(), Deadline: v.Deadline.UTC(), FailureCode: optStr(v.FailureCode), ProjectId: optStr(v.ProjectID),
	}
	if v.ActiveDeadline != nil {
		t := v.ActiveDeadline.UTC()
		out.ActiveDeadline = &t
	}
	if c := v.Clarification; c != nil {
		questions := make([]agentapi.Question, 0, len(c.Questions))
		for _, q := range c.Questions {
			questions = append(questions, agentapi.Question{QuestionId: q.QuestionID, Text: q.Text})
		}
		out.Clarification = &agentapi.Clarification{QuestionSetId: c.QuestionSetID, QuestionSetRevision: c.QuestionSetRevision, Round: c.Round, AskedAt: c.AskedAt.UTC(), ExpiresAt: c.ExpiresAt.UTC(), Questions: questions}
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
	view, err := h.control.CreateOperation(ctx, cmd, p, string(req.Body.Kind), application.OperationSubject{
		ProfileID: s.ProfileId, SubjectDigest: s.SubjectDigest, BriefID: deref(s.BriefId), SourceRevision: deref(s.SourceRevision), SourceHandle: deref(s.SourceHandle),
	})
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

// CreatePreparation (API-01) accepts a prompt artifact and the selected
// references into a Preparation: the subject digest is the canonical
// digest of the intake (the same intake under another command is the
// same subject; Control deduplicates the command), the profile is the
// reviewed preparation profile of this deployment.
func (h *strictHandlers) CreatePreparation(ctx context.Context, req agentapi.CreatePreparationRequestObject) (agentapi.CreatePreparationResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	intake := application.PreparationIntake{PromptTransferID: req.Body.Prompt.TransferId, PromptDigest: req.Body.Prompt.Digest}
	if req.Body.BrandReferences != nil {
		for _, r := range *req.Body.BrandReferences {
			intake.BrandReferences = append(intake.BrandReferences, application.SourceReference{SourceID: r.SourceId, Revision: r.Revision})
		}
	}
	if req.Body.AssetReferences != nil {
		for _, r := range *req.Body.AssetReferences {
			intake.AssetReferences = append(intake.AssetReferences, application.SourceReference{SourceID: r.SourceId, Revision: r.Revision})
		}
	}
	subject, err := application.CanonicalDigest(map[string]any{"prompt": req.Body.Prompt, "brandReferences": req.Body.BrandReferences, "assetReferences": req.Body.AssetReferences})
	if err != nil {
		return nil, err
	}
	view, err := h.control.CreatePreparation(ctx, cmd, p, h.preparationProfile, subject, intake)
	if err != nil {
		return nil, err
	}
	return agentapi.CreatePreparation202JSONResponse(viewToPublic(view)), nil
}

// SubmitAnswer (API-06) commits an answer bound to the current question
// set revision; Control relays one tracked Update.
func (h *strictHandlers) SubmitAnswer(ctx context.Context, req agentapi.SubmitAnswerRequestObject) (agentapi.SubmitAnswerResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	r, err := h.control.SubmitAnswer(ctx, cmd, p, req.OperationId, req.Body.QuestionSetId, req.Body.QuestionSetRevision, req.Body.Answer.TransferId, req.Body.Answer.Digest)
	if err != nil {
		return nil, err
	}
	return agentapi.SubmitAnswer202JSONResponse(agentapi.AnswerReceipt{
		AnswerId: r.AnswerID, OperationId: r.OperationID, QuestionSetId: r.QuestionSetID, QuestionSetRevision: r.QuestionSetRevision, UpdateId: r.UpdateID, AcceptedAt: r.AcceptedAt.UTC(),
	}), nil
}
func transferToPublic(v application.TransferView) agentapi.Transfer {
	out := agentapi.Transfer{
		TransferId: v.TransferID, Handle: v.Handle, Class: agentapi.ArtifactClass(v.Class), ExpectedDigest: v.ExpectedDigest, ExpectedSize: v.ExpectedSize,
		State: agentapi.TransferState(v.State), Deadline: v.Deadline.UTC(), ObjectVersion: optStr(v.ObjectVersion), ReasonCode: optStr(v.ReasonCode),
	}
	if v.Upload != nil {
		method := agentapi.TransferUploadMethod(v.Upload.Method)
		headers := v.Upload.Headers
		expires := v.Upload.ExpiresAt.UTC()
		out.UploadCapability, out.UploadMethod, out.UploadHeaders, out.UploadExpiresAt = optStr(v.Upload.URL), &method, &headers, &expires
	}
	return out
}

// BeginTransfer (API-12) begins a scoped transfer for the authenticated
// principal: Control binds tenant, class, expected digest and size and the
// deadline the API sets from its reviewed window. The authenticated caller
// is a trusted flow and receives the upload capability; candidate code
// never holds a token for this endpoint and only ever sees a handle.
func (h *strictHandlers) BeginTransfer(ctx context.Context, req agentapi.BeginTransferRequestObject) (agentapi.BeginTransferResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	view, err := h.control.BeginTransfer(ctx, cmd, p, application.TransferIntent{
		Class: string(req.Body.Class), MediaType: req.Body.MediaType, ExpectedDigest: req.Body.ExpectedDigest, ExpectedSize: req.Body.ExpectedSize,
		OperationID: deref(req.Body.OperationId), AttemptID: deref(req.Body.AttemptId), Deadline: time.Now().Add(h.transferWindow),
	})
	if err != nil {
		return nil, err
	}
	return agentapi.BeginTransfer201JSONResponse(transferToPublic(view)), nil
}

// FinalizeTransfer (API-12) asks Control to verify the object version the
// caller uploaded; the answer carries no capability.
func (h *strictHandlers) FinalizeTransfer(ctx context.Context, req agentapi.FinalizeTransferRequestObject) (agentapi.FinalizeTransferResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	view, err := h.control.FinalizeTransfer(ctx, cmd, p, req.Handle, req.Body.ObjectVersion)
	if err != nil {
		return nil, err
	}
	return agentapi.FinalizeTransfer200JSONResponse(transferToPublic(view)), nil
}

// ---- previews (P20) ----

func (h *strictHandlers) GetPreview(ctx context.Context, req agentapi.GetPreviewRequestObject) (agentapi.GetPreviewResponseObject, error) {
	v, err := h.control.GetPreview(ctx, principal(ginContext(ctx)), req.OperationId)
	if err != nil {
		return nil, err
	}
	return agentapi.GetPreview200JSONResponse(v), nil
}

// ReadPreviewArtifact returns the verified bytes of the preview's module or
// one of its stylesheets; they are served as data, never executed here.
func (h *strictHandlers) ReadPreviewArtifact(ctx context.Context, req agentapi.ReadPreviewArtifactRequestObject) (agentapi.ReadPreviewArtifactResponseObject, error) {
	b, err := h.control.ReadPreviewArtifact(ctx, principal(ginContext(ctx)), req.OperationId, req.Digest)
	if err != nil {
		return nil, err
	}
	if c := ginContext(ctx); c != nil {
		c.Header("Cache-Control", "no-store")
		c.Header("X-Content-Type-Options", "nosniff")
	}
	if b.MediaType == "text/css" {
		return agentapi.ReadPreviewArtifact200TextcssResponse{Body: bytes.NewReader(b.Body), ContentLength: int64(len(b.Body))}, nil
	}
	return agentapi.ReadPreviewArtifact200TextjavascriptResponse{Body: bytes.NewReader(b.Body), ContentLength: int64(len(b.Body))}, nil
}

// ReadSource returns the exact source archive of a generation or a preview
// build; the lineage, digest and revision travel in headers.
func (h *strictHandlers) ReadSource(ctx context.Context, req agentapi.ReadSourceRequestObject) (agentapi.ReadSourceResponseObject, error) {
	src, err := h.control.ReadSource(ctx, principal(ginContext(ctx)), req.OperationId)
	if err != nil {
		return nil, err
	}
	if c := ginContext(ctx); c != nil {
		c.Header("Cache-Control", "no-store")
	}
	return agentapi.ReadSource200ApplicationxTarResponse{Body: bytes.NewReader(src.Body), ContentLength: int64(len(src.Body)),
		Headers: agentapi.ReadSource200ResponseHeaders{AnvilKitSourceLineage: src.Lineage, AnvilKitSourceDigest: src.Digest, AnvilKitSourceRevision: optStr(src.Revision)}}, nil
}

// ---- knowledge (API-07/08/09/13/14) ----

func limitOf(l *agentapi.Limit) int {
	if l == nil {
		return 0
	}
	return int(*l)
}

func (h *strictHandlers) ListSources(ctx context.Context, req agentapi.ListSourcesRequestObject) (agentapi.ListSourcesResponseObject, error) {
	page, err := h.knowledge.ListSources(ctx, principal(ginContext(ctx)), deref(req.Params.Cursor), limitOf(req.Params.Limit))
	if err != nil {
		return nil, err
	}
	return agentapi.ListSources200JSONResponse(page), nil
}

func (h *strictHandlers) RegisterSource(ctx context.Context, req agentapi.RegisterSourceRequestObject) (agentapi.RegisterSourceResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	src, err := h.knowledge.RegisterSource(ctx, cmd, p, *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.RegisterSource202JSONResponse(src), nil
}

func (h *strictHandlers) GetSource(ctx context.Context, req agentapi.GetSourceRequestObject) (agentapi.GetSourceResponseObject, error) {
	src, err := h.knowledge.GetSource(ctx, principal(ginContext(ctx)), req.SourceId)
	if err != nil {
		return nil, err
	}
	return agentapi.GetSource200JSONResponse(src), nil
}

func (h *strictHandlers) DeleteSource(ctx context.Context, req agentapi.DeleteSourceRequestObject) (agentapi.DeleteSourceResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	src, err := h.knowledge.DeleteSource(ctx, cmd, p, req.SourceId, req.Body.ExpectedRevision)
	if err != nil {
		return nil, err
	}
	return agentapi.DeleteSource200JSONResponse(src), nil
}

func (h *strictHandlers) ReplaceSourceAccess(ctx context.Context, req agentapi.ReplaceSourceAccessRequestObject) (agentapi.ReplaceSourceAccessResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	src, err := h.knowledge.ReplaceSourceAccess(ctx, cmd, p, req.SourceId, *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.ReplaceSourceAccess200JSONResponse(src), nil
}

func (h *strictHandlers) Search(ctx context.Context, req agentapi.SearchRequestObject) (agentapi.SearchResponseObject, error) {
	res, err := h.knowledge.Search(ctx, principal(ginContext(ctx)), *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.Search200JSONResponse(res), nil
}

func (h *strictHandlers) ListMemories(ctx context.Context, req agentapi.ListMemoriesRequestObject) (agentapi.ListMemoriesResponseObject, error) {
	page, err := h.knowledge.ListMemories(ctx, principal(ginContext(ctx)), req.Params)
	if err != nil {
		return nil, err
	}
	return agentapi.ListMemories200JSONResponse(page), nil
}

func (h *strictHandlers) ProposeMemory(ctx context.Context, req agentapi.ProposeMemoryRequestObject) (agentapi.ProposeMemoryResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	f, err := h.knowledge.ProposeMemory(ctx, cmd, p, *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.ProposeMemory201JSONResponse(f), nil
}

func (h *strictHandlers) DecideMemory(ctx context.Context, req agentapi.DecideMemoryRequestObject) (agentapi.DecideMemoryResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	f, err := h.knowledge.DecideMemory(ctx, cmd, p, req.FactId, *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.DecideMemory200JSONResponse(f), nil
}

// ---- mcp (API-10/11/15/16) ----

func (h *strictHandlers) ListCatalog(ctx context.Context, req agentapi.ListCatalogRequestObject) (agentapi.ListCatalogResponseObject, error) {
	page, err := h.mcp.ListCatalog(ctx, principal(ginContext(ctx)), req.Params)
	if err != nil {
		return nil, err
	}
	return agentapi.ListCatalog200JSONResponse(page), nil
}

func (h *strictHandlers) DiscoverServer(ctx context.Context, req agentapi.DiscoverServerRequestObject) (agentapi.DiscoverServerResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	d, err := h.mcp.DiscoverServer(ctx, cmd, p, *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.DiscoverServer201JSONResponse(d), nil
}

func (h *strictHandlers) ReviewDescriptor(ctx context.Context, req agentapi.ReviewDescriptorRequestObject) (agentapi.ReviewDescriptorResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	d, err := h.mcp.ReviewDescriptor(ctx, cmd, p, req.ServerId, *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.ReviewDescriptor200JSONResponse(d), nil
}

func (h *strictHandlers) ListGrants(ctx context.Context, req agentapi.ListGrantsRequestObject) (agentapi.ListGrantsResponseObject, error) {
	page, err := h.mcp.ListGrants(ctx, principal(ginContext(ctx)), req.Params)
	if err != nil {
		return nil, err
	}
	return agentapi.ListGrants200JSONResponse(page), nil
}

func (h *strictHandlers) CreateGrant(ctx context.Context, req agentapi.CreateGrantRequestObject) (agentapi.CreateGrantResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	g, err := h.mcp.CreateGrant(ctx, cmd, p, *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.CreateGrant201JSONResponse(g), nil
}

func (h *strictHandlers) GetGrant(ctx context.Context, req agentapi.GetGrantRequestObject) (agentapi.GetGrantResponseObject, error) {
	g, err := h.mcp.GetGrant(ctx, principal(ginContext(ctx)), req.GrantId)
	if err != nil {
		return nil, err
	}
	return agentapi.GetGrant200JSONResponse(g), nil
}

func (h *strictHandlers) GetRevocationProgress(ctx context.Context, req agentapi.GetRevocationProgressRequestObject) (agentapi.GetRevocationProgressResponseObject, error) {
	pr, err := h.mcp.GetRevocationProgress(ctx, principal(ginContext(ctx)), req.GrantId)
	if err != nil {
		return nil, err
	}
	return agentapi.GetRevocationProgress200JSONResponse(pr), nil
}

func (h *strictHandlers) RevokeGrant(ctx context.Context, req agentapi.RevokeGrantRequestObject) (agentapi.RevokeGrantResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	g, err := h.mcp.RevokeGrant(ctx, cmd, p, req.GrantId, *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.RevokeGrant202JSONResponse(g), nil
}

func (h *strictHandlers) CreateToolCall(ctx context.Context, req agentapi.CreateToolCallRequestObject) (agentapi.CreateToolCallResponseObject, error) {
	cmd, p := identity(ctx, req.Body.CommandId)
	c, err := h.mcp.CreateToolCall(ctx, cmd, p, *req.Body)
	if err != nil {
		return nil, err
	}
	return agentapi.CreateToolCall202JSONResponse(c), nil
}

func (h *strictHandlers) GetToolCall(ctx context.Context, req agentapi.GetToolCallRequestObject) (agentapi.GetToolCallResponseObject, error) {
	c, err := h.mcp.GetToolCall(ctx, principal(ginContext(ctx)), req.CallId)
	if err != nil {
		return nil, err
	}
	return agentapi.GetToolCall200JSONResponse(c), nil
}
