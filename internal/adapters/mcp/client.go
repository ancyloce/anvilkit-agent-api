// Package mcp is the API's grpc-go gateway to the MCP owner (API-10/11/
// 15/16; P20): public values in, CatalogService, GrantService and
// CallService under the principal's scope and command identity, public
// values out. Catalog visibility is never a grant; MCP decides every role.
// Transport identity (mTLS) is ENV-03; the development profile dials
// plaintext.
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	mcpv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/mcp/v1"
)

// maxArguments bounds a tool call's serialized argument object (the
// CallService bound).
const maxArguments = 64 << 10

type Client struct {
	conn    *grpc.ClientConn
	catalog mcpv1.CatalogServiceClient
	grants  mcpv1.GrantServiceClient
	calls   mcpv1.CallServiceClient
	timeout time.Duration
}

func Dial(address string, timeout time.Duration) (*Client, error) {
	conn, err := grpc.NewClient(address, grpc.WithStatsHandler(otelgrpc.NewClientHandler()), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, catalog: mcpv1.NewCatalogServiceClient(conn), grants: mcpv1.NewGrantServiceClient(conn), calls: mcpv1.NewCallServiceClient(conn), timeout: timeout}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

func scope(p application.Principal) *mcpv1.Scope {
	return &mcpv1.Scope{TenantId: p.TenantID, ProjectId: p.ProjectID, ActorId: p.ActorID}
}

func command(cmd application.CommandIdentity) *mcpv1.CommandIdentity {
	return &mcpv1.CommandIdentity{TenantId: cmd.TenantID, CommandId: cmd.CommandID, ActorId: cmd.ActorID, RequestDigest: cmd.RequestDigest}
}

func word(name, prefix string) string { return strings.ToLower(strings.TrimPrefix(name, prefix)) }

func enum(values map[string]int32, prefix, w string) int32 { return values[prefix+strings.ToUpper(w)] }

func money(m *mcpv1.Money) agentapi.Money {
	return agentapi.Money{Currency: m.GetCurrency(), Amount: m.GetAmount()}
}

func optTime(t *timestamppb.Timestamp) *time.Time {
	if t == nil {
		return nil
	}
	v := t.AsTime().UTC()
	return &v
}

func toDescriptor(d *mcpv1.Descriptor) agentapi.Descriptor {
	tools := make([]agentapi.ToolSchema, 0, len(d.GetTools()))
	for _, t := range d.GetTools() {
		ts := agentapi.ToolSchema{Method: t.GetMethod(), InputSchemaDigest: t.GetInputSchemaDigest(), OutputSchemaDigest: t.GetOutputSchemaDigest(), SideEffecting: t.GetSideEffecting()}
		if t.GetUnitPrice() != nil {
			m := money(t.GetUnitPrice())
			ts.UnitPrice = &m
		}
		tools = append(tools, ts)
	}
	return agentapi.Descriptor{
		ServerId: d.GetServerId(), Revision: d.GetRevision(), CanonicalResource: d.GetCanonicalResource(), Transport: agentapi.DescriptorTransport(d.GetTransport()),
		ProtocolVersion: d.GetProtocolVersion(), DescriptorDigest: d.GetDescriptorDigest(), Tools: tools,
		State: agentapi.CatalogState(word(d.GetState().String(), "CATALOG_STATE_")), CreatedAt: d.GetCreatedAt().AsTime().UTC(), UpdatedAt: d.GetUpdatedAt().AsTime().UTC(),
	}
}

// toGrant maps a grant; without a progress read, blocked admission and
// convergence follow from the state (admission is blocked in every state
// but active; only a revoked grant has converged senders).
func toGrant(g *mcpv1.Grant) agentapi.Grant {
	state := word(g.GetState().String(), "GRANT_STATE_")
	return agentapi.Grant{
		GrantId: g.GetGrantId(), Revision: g.GetRevision(), TenantId: g.GetTenantId(), SubjectType: agentapi.GrantSubjectType(g.GetSubjectType()),
		SubjectId: g.GetSubjectId(), ServerId: g.GetServerId(), DescriptorRevision: g.GetDescriptorRevision(), DescriptorDigest: g.GetDescriptorDigest(),
		Methods: append([]string{}, g.GetMethods()...), Purpose: g.GetPurpose(), CostCap: money(g.GetCostCap()), State: agentapi.GrantState(state),
		NewAdmissionBlocked: state != "active", SendersConverged: state == "revoked", ExpiresAt: optTime(g.GetExpiresAt()),
		CreatedAt: g.GetCreatedAt().AsTime().UTC(), UpdatedAt: g.GetUpdatedAt().AsTime().UTC(),
	}
}

func toCall(t *mcpv1.ToolCall) agentapi.ToolCall {
	out := agentapi.ToolCall{
		CallId: t.GetCallId(), TenantId: t.GetTenantId(), GrantId: t.GetGrantId(), GrantRevision: t.GetGrantRevision(), ServerId: t.GetServerId(),
		Method: t.GetMethod(), ArgumentDigest: t.GetArgumentDigest(), State: agentapi.CallState(word(t.GetState().String(), "CALL_STATE_")),
		FailureCode: t.FailureCode, ResultDigest: t.ResultDigest, CreatedAt: t.GetCreatedAt().AsTime().UTC(), UpdatedAt: t.GetUpdatedAt().AsTime().UTC(),
	}
	if len(t.GetResult()) > 0 {
		var r map[string]any
		if json.Unmarshal(t.GetResult(), &r) == nil {
			out.Result = &r
		}
	}
	return out
}

func (c *Client) ListCatalog(ctx context.Context, p application.Principal, params agentapi.ListCatalogParams) (agentapi.CatalogPage, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &mcpv1.ListCatalogRequest{Scope: scope(p)}
	if params.State != nil {
		in.State = mcpv1.CatalogState(enum(mcpv1.CatalogState_value, "CATALOG_STATE_", string(*params.State)))
	}
	if params.Cursor != nil {
		in.Cursor = *params.Cursor
	}
	if params.Limit != nil {
		in.Limit = uint32(*params.Limit)
	}
	resp, err := c.catalog.ListCatalog(ctx, in)
	if err != nil {
		return agentapi.CatalogPage{}, control.MapError(err, "mcp")
	}
	out := agentapi.CatalogPage{Descriptors: []agentapi.Descriptor{}}
	for _, d := range resp.GetDescriptors() {
		out.Descriptors = append(out.Descriptors, toDescriptor(d))
	}
	if n := resp.GetNextCursor(); n != "" {
		out.NextCursor = &n
	}
	return out, nil
}

func (c *Client) DiscoverServer(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.DiscoverServerRequest) (agentapi.Descriptor, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &mcpv1.DiscoverServerRequest{Command: command(cmd), Scope: scope(p), CanonicalResource: req.CanonicalResource, Transport: string(req.Transport),
		ProtocolVersion: req.ProtocolVersion, Provenance: req.Provenance, DataClass: string(req.DataClass)}
	for _, t := range req.Tools {
		ts := &mcpv1.ToolSchema{Method: t.Method, SideEffecting: t.SideEffecting, UnitPrice: &mcpv1.Money{Currency: t.UnitPrice.Currency, Amount: t.UnitPrice.Amount}}
		if t.Idempotent != nil {
			ts.Idempotent = *t.Idempotent
		}
		if t.QuerySupported != nil {
			ts.QuerySupported = *t.QuerySupported
		}
		in.Tools = append(in.Tools, ts)
	}
	if req.DescriptorDigest != nil {
		in.DescriptorDigest = *req.DescriptorDigest
	}
	if req.NetworkScope != nil {
		in.NetworkScope = *req.NetworkScope
	}
	if req.Licenses != nil {
		in.Licenses = *req.Licenses
	}
	resp, err := c.catalog.DiscoverServer(ctx, in)
	if err != nil {
		return agentapi.Descriptor{}, control.MapError(err, "mcp")
	}
	return toDescriptor(resp.GetDescriptor_()), nil
}

// ReviewDescriptor maps approve/reject to the review decision and disable
// to the disablement (which revokes the revision's grants at MCP).
func (c *Client) ReviewDescriptor(ctx context.Context, cmd application.CommandIdentity, p application.Principal, serverID string, req agentapi.ReviewDescriptorRequest) (agentapi.Descriptor, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	if req.Decision == agentapi.ReviewDescriptorRequestDecisionDisable {
		resp, err := c.catalog.DisableDescriptor(ctx, &mcpv1.DisableDescriptorRequest{Command: command(cmd), Scope: scope(p), ServerId: serverID, Revision: req.Revision, ReasonCode: req.ReasonCode})
		if err != nil {
			return agentapi.Descriptor{}, control.MapError(err, "mcp")
		}
		return toDescriptor(resp.GetDescriptor_()), nil
	}
	resp, err := c.catalog.ReviewDescriptor(ctx, &mcpv1.ReviewDescriptorRequest{Command: command(cmd), Scope: scope(p), ServerId: serverID, Revision: req.Revision,
		DescriptorDigest: req.DescriptorDigest, Decision: mcpv1.ReviewDecision(enum(mcpv1.ReviewDecision_value, "REVIEW_DECISION_", string(req.Decision))), ReasonCode: req.ReasonCode})
	if err != nil {
		return agentapi.Descriptor{}, control.MapError(err, "mcp")
	}
	return toDescriptor(resp.GetDescriptor_()), nil
}

func (c *Client) ListGrants(ctx context.Context, p application.Principal, params agentapi.ListGrantsParams) (agentapi.GrantPage, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &mcpv1.ListGrantsRequest{Scope: scope(p)}
	if params.State != nil {
		in.State = mcpv1.GrantState(enum(mcpv1.GrantState_value, "GRANT_STATE_", string(*params.State)))
	}
	if params.Cursor != nil {
		in.Cursor = *params.Cursor
	}
	if params.Limit != nil {
		in.Limit = uint32(*params.Limit)
	}
	resp, err := c.grants.ListGrants(ctx, in)
	if err != nil {
		return agentapi.GrantPage{}, control.MapError(err, "mcp")
	}
	out := agentapi.GrantPage{Grants: []agentapi.Grant{}}
	for _, g := range resp.GetGrants() {
		out.Grants = append(out.Grants, toGrant(g))
	}
	if n := resp.GetNextCursor(); n != "" {
		out.NextCursor = &n
	}
	return out, nil
}

func (c *Client) CreateGrant(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateGrantRequest) (agentapi.Grant, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	in := &mcpv1.CreateGrantRequest{Command: command(cmd), Scope: scope(p), SubjectType: string(req.SubjectType), SubjectId: req.SubjectId, ServerId: req.ServerId,
		DescriptorRevision: req.DescriptorRevision, DescriptorDigest: req.DescriptorDigest, Methods: req.Methods, Purpose: req.Purpose,
		CostCap: &mcpv1.Money{Currency: req.CostCap.Currency, Amount: req.CostCap.Amount}}
	if req.ExpiresAt != nil {
		in.ExpiresAt = timestamppb.New(*req.ExpiresAt)
	}
	resp, err := c.grants.CreateGrant(ctx, in)
	if err != nil {
		return agentapi.Grant{}, control.MapError(err, "mcp")
	}
	return toGrant(resp.GetGrant()), nil
}

func (c *Client) progress(ctx context.Context, p application.Principal, grantID string) (*mcpv1.GetRevocationProgressResponse, error) {
	resp, err := c.grants.GetRevocationProgress(ctx, &mcpv1.GetRevocationProgressRequest{Scope: scope(p), GrantId: grantID})
	if err != nil {
		return nil, control.MapError(err, "mcp")
	}
	return resp, nil
}

func (c *Client) GetGrant(ctx context.Context, p application.Principal, grantID string) (agentapi.Grant, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	pr, err := c.progress(ctx, p, grantID)
	if err != nil {
		return agentapi.Grant{}, err
	}
	g := toGrant(pr.GetGrant())
	g.NewAdmissionBlocked, g.SendersConverged = pr.GetNewAdmissionBlocked(), pr.GetSendersConverged()
	return g, nil
}

func (c *Client) GetRevocationProgress(ctx context.Context, p application.Principal, grantID string) (agentapi.RevocationProgress, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	pr, err := c.progress(ctx, p, grantID)
	if err != nil {
		return agentapi.RevocationProgress{}, err
	}
	g := toGrant(pr.GetGrant())
	g.NewAdmissionBlocked, g.SendersConverged = pr.GetNewAdmissionBlocked(), pr.GetSendersConverged()
	state := pr.GetControlState()
	if state == "" {
		state = "none"
	}
	return agentapi.RevocationProgress{Grant: g, NewAdmissionBlocked: pr.GetNewAdmissionBlocked(), SendersConverged: pr.GetSendersConverged(),
		InFlightCalls: zeroIfEmpty(pr.GetInFlightCalls()), UnknownCalls: zeroIfEmpty(pr.GetUnknownCalls()), OpenCalls: zeroIfEmpty(pr.GetOpenCalls()),
		ControlState: agentapi.RevocationProgressControlState(state)}, nil
}

func zeroIfEmpty(s string) string {
	if s == "" {
		return "0"
	}
	return s
}

func (c *Client) RevokeGrant(ctx context.Context, cmd application.CommandIdentity, p application.Principal, grantID string, req agentapi.RevokeGrantRequest) (agentapi.Grant, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.grants.RevokeGrant(ctx, &mcpv1.RevokeGrantRequest{Command: command(cmd), Scope: scope(p), GrantId: grantID, ExpectedRevision: req.ExpectedRevision, ReasonCode: req.ReasonCode})
	if err != nil {
		return agentapi.Grant{}, control.MapError(err, "mcp")
	}
	return toGrant(resp.GetGrant()), nil
}

// CreateToolCall serializes the argument object once; MCP digests exactly
// these bytes and validates them against the grant revision's schema.
func (c *Client) CreateToolCall(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateToolCallRequest) (agentapi.ToolCall, error) {
	args, err := json.Marshal(req.Arguments)
	if err != nil || len(args) > maxArguments {
		return agentapi.ToolCall{}, &application.ControlError{Code: "INVALID_ARGUMENT", Message: fmt.Sprintf("arguments must be an object of at most %d bytes", maxArguments)}
	}
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	digest := application.DigestBytes(args)
	resp, err := c.calls.CreateCall(ctx, &mcpv1.CreateCallRequest{Command: command(cmd), Scope: scope(p), GrantId: req.GrantId, GrantRevision: req.GrantRevision,
		Method: req.Method, ArgumentRef: "inline", ArgumentDigest: digest, Arguments: args, OperationId: req.OperationId, AttemptId: req.AttemptId,
		ExecutionEpoch: req.ExecutionEpoch, Deadline: timestamppb.New(time.Now().Add(10 * time.Minute))})
	if err != nil {
		return agentapi.ToolCall{}, control.MapError(err, "mcp")
	}
	return toCall(resp.GetCall()), nil
}

func (c *Client) GetToolCall(ctx context.Context, p application.Principal, callID string) (agentapi.ToolCall, error) {
	ctx, cancel := c.ctx(ctx)
	defer cancel()
	resp, err := c.calls.GetCall(ctx, &mcpv1.GetCallRequest{Scope: scope(p), CallId: callID})
	if err != nil {
		return agentapi.ToolCall{}, control.MapError(err, "mcp")
	}
	return toCall(resp.GetCall()), nil
}
