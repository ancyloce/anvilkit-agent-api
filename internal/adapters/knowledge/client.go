// Package knowledge is the API's grpc-go gateway to the Knowledge owner
// (API-07/08/09/13/14; P20): public values in, SourceService,
// RetrievalService and MemoryService under the principal's scope and
// command identity, public values out. The transport identity is the
// workload's mTLS certificate (P0.1); plaintext exists only under the
// development guard.
package knowledge

import (
	"context"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	knowledgev1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/knowledge/v1"
)

type Client struct {
	conn      *grpc.ClientConn
	sources   knowledgev1.SourceServiceClient
	snapshots knowledgev1.SnapshotServiceClient
	retrieval knowledgev1.RetrievalServiceClient
	memory    knowledgev1.MemoryServiceClient
	timeout   time.Duration
}

// Dial connects with the given transport credential (the rotating workload
// mTLS identity, or plaintext under the loader's development guard).
func Dial(address string, timeout time.Duration, transport grpc.DialOption) (*Client, error) {
	conn, err := grpc.NewClient(address, grpc.WithStatsHandler(otelgrpc.NewClientHandler()), transport)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, sources: knowledgev1.NewSourceServiceClient(conn), snapshots: knowledgev1.NewSnapshotServiceClient(conn),
		retrieval: knowledgev1.NewRetrievalServiceClient(conn), memory: knowledgev1.NewMemoryServiceClient(conn), timeout: timeout}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func scope(p application.Principal) *knowledgev1.Scope {
	return &knowledgev1.Scope{TenantId: p.TenantID, ProjectId: p.ProjectID, ActorId: p.ActorID, Roles: p.Roles}
}

func command(cmd application.CommandIdentity) *knowledgev1.CommandIdentity {
	return &knowledgev1.CommandIdentity{TenantId: cmd.TenantID, CommandId: cmd.CommandID, ActorId: cmd.ActorID, RequestDigest: cmd.RequestDigest}
}

// word maps a proto enum name to the contract's lowercase word.
func word(name, prefix string) string { return strings.ToLower(strings.TrimPrefix(name, prefix)) }

// enum maps a contract word back to the proto value (0 when unknown).
func enum(values map[string]int32, prefix, w string) int32 { return values[prefix+strings.ToUpper(w)] }

func ts(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.AsTime().UTC()
	return &v
}

func toSource(s *knowledgev1.Source) agentapi.Source {
	access := make([]agentapi.AccessEntry, 0, len(s.GetAccess()))
	for _, a := range s.GetAccess() {
		e := agentapi.AccessEntry{PrincipalType: agentapi.AccessEntryPrincipalType(a.GetPrincipalType()), PrincipalId: a.GetPrincipalId()}
		if a.GetPermission() == "manage" {
			p := agentapi.Manage
			e.Permission = &p
		}
		access = append(access, e)
	}
	out := agentapi.Source{
		SourceId: s.GetSourceId(), TenantId: s.GetTenantId(), Kind: agentapi.SourceKind(word(s.GetKind().String(), "SOURCE_KIND_")), Locator: s.GetLocator(),
		CurrentRevision: s.GetCurrentRevision(), ContentDigest: s.GetContentDigest(), AclRevision: s.GetAclRevision(), Access: access,
		Ingest: agentapi.IngestState(word(s.GetIngest().String(), "INGEST_STATE_")), Deleted: s.GetDeleted(),
		CreatedAt: s.GetCreatedAt().AsTime().UTC(), UpdatedAt: s.GetUpdatedAt().AsTime().UTC(),
	}
	if s.GetProjectId() != "" {
		v := s.GetProjectId()
		out.ProjectId = &v
	}
	return out
}

func toFact(f *knowledgev1.MemoryFact) agentapi.MemoryFact {
	out := agentapi.MemoryFact{
		FactId: f.GetFactId(), TenantId: f.GetTenantId(), SubjectType: f.GetSubjectType(), SubjectId: f.GetSubjectId(), Content: f.GetContent(),
		ContentDigest: f.GetContentDigest(), State: agentapi.FactState(word(f.GetState().String(), "FACT_STATE_")), Revision: f.GetRevision(),
		Proposer: f.GetProposer(), ExpiresAt: ts(f.GetExpiresAt()), CreatedAt: f.GetCreatedAt().AsTime().UTC(), UpdatedAt: f.GetUpdatedAt().AsTime().UTC(),
	}
	if f.Confirmer != nil {
		v := f.GetConfirmer()
		out.Confirmer = &v
	}
	if refs := f.GetSourceRefs(); len(refs) > 0 {
		out.SourceRefs = &refs
	}
	return out
}

func accessOf(in []agentapi.AccessEntry) []*knowledgev1.AccessEntry {
	out := make([]*knowledgev1.AccessEntry, 0, len(in))
	for _, a := range in {
		e := &knowledgev1.AccessEntry{PrincipalType: string(a.PrincipalType), PrincipalId: a.PrincipalId}
		if a.Permission != nil {
			e.Permission = string(*a.Permission)
		}
		out = append(out, e)
	}
	return out
}

func (c *Client) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

func (c *Client) ListSources(ctx context.Context, p application.Principal, cursor string, limit int) (agentapi.SourcePage, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.sources.ListSources(ctx, &knowledgev1.ListSourcesRequest{Scope: scope(p), Cursor: cursor, Limit: uint32(limit)})
	if err != nil {
		return agentapi.SourcePage{}, control.MapError(err, "knowledge")
	}
	out := agentapi.SourcePage{Sources: []agentapi.Source{}}
	for _, s := range resp.GetSources() {
		out.Sources = append(out.Sources, toSource(s))
	}
	if n := resp.GetNextCursor(); n != "" {
		out.NextCursor = &n
	}
	return out, nil
}

func (c *Client) RegisterSource(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.RegisterSourceRequest) (agentapi.Source, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &knowledgev1.RegisterSourceRequest{Command: command(cmd), Scope: scope(p),
		Kind:    knowledgev1.SourceKind(enum(knowledgev1.SourceKind_value, "SOURCE_KIND_", string(req.Kind))),
		Locator: req.Locator, ContentDigest: req.ContentDigest, MediaType: req.MediaType, SizeBytes: req.SizeBytes}
	if req.Access != nil {
		in.Access = accessOf(*req.Access)
	}
	resp, err := c.sources.RegisterSource(ctx, in)
	if err != nil {
		return agentapi.Source{}, control.MapError(err, "knowledge")
	}
	return toSource(resp.GetSource()), nil
}

func (c *Client) GetSource(ctx context.Context, p application.Principal, sourceID string) (agentapi.Source, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.sources.GetSource(ctx, &knowledgev1.GetSourceRequest{Scope: scope(p), SourceId: sourceID})
	if err != nil {
		return agentapi.Source{}, control.MapError(err, "knowledge")
	}
	return toSource(resp.GetSource()), nil
}

func (c *Client) DeleteSource(ctx context.Context, cmd application.CommandIdentity, p application.Principal, sourceID, expectedRevision string) (agentapi.Source, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.sources.DeleteSource(ctx, &knowledgev1.DeleteSourceRequest{Command: command(cmd), Scope: scope(p), SourceId: sourceID, ExpectedRevision: expectedRevision})
	if err != nil {
		return agentapi.Source{}, control.MapError(err, "knowledge")
	}
	return toSource(resp.GetSource()), nil
}

func (c *Client) ReplaceSourceAccess(ctx context.Context, cmd application.CommandIdentity, p application.Principal, sourceID string, req agentapi.ReplaceSourceAccessRequest) (agentapi.Source, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.sources.UpdateSourceAccess(ctx, &knowledgev1.UpdateSourceAccessRequest{Command: command(cmd), Scope: scope(p), SourceId: sourceID,
		ExpectedAclRevision: req.ExpectedAclRevision, Access: accessOf(req.Access)})
	if err != nil {
		return agentapi.Source{}, control.MapError(err, "knowledge")
	}
	return toSource(resp.GetSource()), nil
}

func (c *Client) Search(ctx context.Context, p application.Principal, req agentapi.SearchRequest) (agentapi.SearchResponse, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &knowledgev1.SearchRequest{Scope: scope(p), SnapshotId: req.SnapshotId, Query: req.Query, RetrievalProfileId: req.RetrievalProfileId,
		Deadline: timestamppb.New(time.Now().Add(c.timeout))}
	if req.MaxContextItems != nil {
		in.MaxContextItems = uint32(*req.MaxContextItems)
	}
	resp, err := c.retrieval.Search(ctx, in)
	if err != nil {
		return agentapi.SearchResponse{}, control.MapError(err, "knowledge")
	}
	out := agentapi.SearchResponse{Items: []agentapi.ContextItem{}, NoAnswer: resp.GetNoAnswer(), IndexGeneration: resp.GetIndexGeneration()}
	for _, it := range resp.GetItems() {
		ct := it.GetCitation()
		out.Items = append(out.Items, agentapi.ContextItem{Text: it.GetText(), Citation: agentapi.Citation{Ordinal: ct.GetOrdinal(), SourceId: ct.GetSourceId(),
			SourceRevision: ct.GetSourceRevision(), ChunkId: ct.GetChunkId(), Locator: ct.GetLocator(), ContentDigest: ct.GetContentDigest()}})
	}
	return out, nil
}

func (c *Client) ListMemories(ctx context.Context, p application.Principal, params agentapi.ListMemoriesParams) (agentapi.MemoryPage, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &knowledgev1.ListFactsRequest{Scope: scope(p)}
	if params.SubjectType != nil {
		in.SubjectType = *params.SubjectType
	}
	if params.SubjectId != nil {
		in.SubjectId = *params.SubjectId
	}
	if params.State != nil {
		in.State = knowledgev1.FactState(enum(knowledgev1.FactState_value, "FACT_STATE_", string(*params.State)))
	}
	if params.Cursor != nil {
		in.Cursor = *params.Cursor
	}
	if params.Limit != nil {
		in.Limit = uint32(*params.Limit)
	}
	resp, err := c.memory.ListFacts(ctx, in)
	if err != nil {
		return agentapi.MemoryPage{}, control.MapError(err, "knowledge")
	}
	out := agentapi.MemoryPage{Facts: []agentapi.MemoryFact{}}
	for _, f := range resp.GetFacts() {
		out.Facts = append(out.Facts, toFact(f))
	}
	if n := resp.GetNextCursor(); n != "" {
		out.NextCursor = &n
	}
	return out, nil
}

// ProposeMemory proposes as the user the principal is: the API serves
// people, never a model or a worker identity.
func (c *Client) ProposeMemory(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.ProposeMemoryRequest) (agentapi.MemoryFact, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &knowledgev1.ProposeFactRequest{Command: command(cmd), Scope: scope(p), SubjectType: req.SubjectType, SubjectId: req.SubjectId, Content: req.Content,
		Origin: knowledgev1.FactOrigin_FACT_ORIGIN_USER}
	if req.SourceRefs != nil {
		in.SourceRefs = *req.SourceRefs
	}
	if req.ExpiresAt != nil {
		in.ExpiresAt = timestamppb.New(*req.ExpiresAt)
	}
	resp, err := c.memory.ProposeFact(ctx, in)
	if err != nil {
		return agentapi.MemoryFact{}, control.MapError(err, "knowledge")
	}
	return toFact(resp.GetFact()), nil
}

func (c *Client) DecideMemory(ctx context.Context, cmd application.CommandIdentity, p application.Principal, factID string, req agentapi.MemoryDecisionRequest) (agentapi.MemoryFact, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &knowledgev1.DecideFactRequest{Command: command(cmd), Scope: scope(p), FactId: factID, ExpectedRevision: req.ExpectedRevision,
		Decision: knowledgev1.FactDecision(enum(knowledgev1.FactDecision_value, "FACT_DECISION_", string(req.Decision)))}
	if req.ReasonCode != nil {
		in.ReasonCode = req.ReasonCode
	}
	resp, err := c.memory.DecideFact(ctx, in)
	if err != nil {
		return agentapi.MemoryFact{}, control.MapError(err, "knowledge")
	}
	return toFact(resp.GetFact()), nil
}

// BeginSourceUpload asks Knowledge for a single-use upload capability of one
// document (P2.6). The capability is returned to the authenticated caller
// only; whoever performs the PUT sends exactly its signed headers.
func (c *Client) BeginSourceUpload(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.BeginSourceUploadRequest) (agentapi.SourceUpload, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.sources.BeginUpload(ctx, &knowledgev1.BeginUploadRequest{Command: command(cmd), Scope: scope(p), ContentDigest: req.ContentDigest,
		SizeBytes: req.SizeBytes, MediaType: req.MediaType})
	if err != nil {
		return agentapi.SourceUpload{}, control.MapError(err, "knowledge")
	}
	headers := map[string]string{}
	for k, v := range resp.GetUploadHeaders() {
		headers[k] = v
	}
	return agentapi.SourceUpload{UploadId: resp.GetUploadId(), UploadUrl: resp.GetUploadUrl(), UploadMethod: agentapi.SourceUploadUploadMethod(resp.GetUploadMethod()),
		UploadHeaders: headers, ExpiresAt: resp.GetExpiresAt().AsTime().UTC()}, nil
}

func toSnapshot(s *knowledgev1.Snapshot) agentapi.Snapshot {
	out := agentapi.Snapshot{SnapshotId: s.GetSnapshotId(), TenantId: s.GetTenantId(), IndexGeneration: s.GetIndexGeneration(),
		SourceRevisions: append([]agentapi.SourceRef{}, s.GetSourceRevisions()...), ContentDigest: s.GetContentDigest(), CreatedAt: s.GetCreatedAt().AsTime().UTC()}
	if s.GetProjectId() != "" {
		v := s.GetProjectId()
		out.ProjectId = &v
	}
	return out
}

func (c *Client) CreateSnapshot(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateSnapshotRequest) (agentapi.Snapshot, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.snapshots.CreateSnapshot(ctx, &knowledgev1.CreateSnapshotRequest{Command: command(cmd), Scope: scope(p), SourceIds: req.SourceIds})
	if err != nil {
		return agentapi.Snapshot{}, control.MapError(err, "knowledge")
	}
	return toSnapshot(resp.GetSnapshot()), nil
}

func (c *Client) GetSnapshot(ctx context.Context, p application.Principal, snapshotID string) (agentapi.Snapshot, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.snapshots.GetSnapshot(ctx, &knowledgev1.GetSnapshotRequest{Scope: scope(p), SnapshotId: snapshotID})
	if err != nil {
		return agentapi.Snapshot{}, control.MapError(err, "knowledge")
	}
	return toSnapshot(resp.GetSnapshot()), nil
}

// RecallMemories recalls the principal's facts (P2.6); Knowledge asks
// Control's commercial authorization of the disclosure first.
func (c *Client) RecallMemories(ctx context.Context, p application.Principal, req agentapi.RecallMemoriesRequest) (agentapi.RecallMemoriesResponse, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &knowledgev1.RecallFactsRequest{Scope: scope(p), Query: req.Query, RetrievalProfileId: req.RetrievalProfileId, MaxFacts: 5,
		Deadline: timestamppb.New(time.Now().Add(c.timeout))}
	if req.MaxFacts != nil {
		in.MaxFacts = uint32(*req.MaxFacts)
	}
	resp, err := c.memory.RecallFacts(ctx, in)
	if err != nil {
		return agentapi.RecallMemoriesResponse{}, control.MapError(err, "knowledge")
	}
	out := agentapi.RecallMemoriesResponse{Facts: []agentapi.MemoryFact{}, NoAnswer: resp.GetNoAnswer(), IndexGeneration: resp.GetIndexGeneration()}
	for _, f := range resp.GetFacts() {
		out.Facts = append(out.Facts, toFact(f))
	}
	return out, nil
}
