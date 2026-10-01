package application

import (
	"context"

	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

// Knowledge and MCP are the API's thin gateways to the owning services
// (API-07..API-16, P20): the public contract's values in, the owner's gRPC
// call under the authenticated principal's scope and command identity,
// the public values out. The owners decide every authorization; the API
// adds no business rule, cache or fallback. Without a placement a gateway
// answers DEPENDENCY_UNAVAILABLE.
type Knowledge interface {
	ListSources(ctx context.Context, p Principal, cursor string, limit int) (agentapi.SourcePage, error)
	RegisterSource(ctx context.Context, cmd CommandIdentity, p Principal, req agentapi.RegisterSourceRequest) (agentapi.Source, error)
	GetSource(ctx context.Context, p Principal, sourceID string) (agentapi.Source, error)
	DeleteSource(ctx context.Context, cmd CommandIdentity, p Principal, sourceID, expectedRevision string) (agentapi.Source, error)
	ReplaceSourceAccess(ctx context.Context, cmd CommandIdentity, p Principal, sourceID string, req agentapi.ReplaceSourceAccessRequest) (agentapi.Source, error)
	Search(ctx context.Context, p Principal, req agentapi.SearchRequest) (agentapi.SearchResponse, error)
	ListMemories(ctx context.Context, p Principal, params agentapi.ListMemoriesParams) (agentapi.MemoryPage, error)
	ProposeMemory(ctx context.Context, cmd CommandIdentity, p Principal, req agentapi.ProposeMemoryRequest) (agentapi.MemoryFact, error)
	DecideMemory(ctx context.Context, cmd CommandIdentity, p Principal, factID string, req agentapi.MemoryDecisionRequest) (agentapi.MemoryFact, error)
}

type MCP interface {
	ListCatalog(ctx context.Context, p Principal, params agentapi.ListCatalogParams) (agentapi.CatalogPage, error)
	DiscoverServer(ctx context.Context, cmd CommandIdentity, p Principal, req agentapi.DiscoverServerRequest) (agentapi.Descriptor, error)
	ReviewDescriptor(ctx context.Context, cmd CommandIdentity, p Principal, serverID string, req agentapi.ReviewDescriptorRequest) (agentapi.Descriptor, error)
	ListGrants(ctx context.Context, p Principal, params agentapi.ListGrantsParams) (agentapi.GrantPage, error)
	CreateGrant(ctx context.Context, cmd CommandIdentity, p Principal, req agentapi.CreateGrantRequest) (agentapi.Grant, error)
	GetGrant(ctx context.Context, p Principal, grantID string) (agentapi.Grant, error)
	GetRevocationProgress(ctx context.Context, p Principal, grantID string) (agentapi.RevocationProgress, error)
	RevokeGrant(ctx context.Context, cmd CommandIdentity, p Principal, grantID string, req agentapi.RevokeGrantRequest) (agentapi.Grant, error)
	CreateToolCall(ctx context.Context, cmd CommandIdentity, p Principal, req agentapi.CreateToolCallRequest) (agentapi.ToolCall, error)
	GetToolCall(ctx context.Context, p Principal, callID string) (agentapi.ToolCall, error)
}

// PreviewView is the committed preview of a preview_build operation.
type PreviewView = agentapi.Preview

// ReleaseView is the public release projection (P21).
type ReleaseView = agentapi.Release

// SourceBytes are the verified bytes of an operation's source archive, its
// lineage and the revision they are (empty when unknown).
type SourceBytes struct {
	Lineage  string
	Revision string
	Digest   string
	Body     []byte
}

// PreviewBytes are the verified bytes of one preview artifact.
type PreviewBytes struct {
	MediaType string
	Body      []byte
}

// Unavailable is the gateway without a placement: every call answers
// DEPENDENCY_UNAVAILABLE naming the service.
type Unavailable struct{ Service string }

func (u Unavailable) err() error {
	return &ControlError{Code: "DEPENDENCY_UNAVAILABLE", Message: u.Service + " is not placed for this API", Retryable: false}
}

func (u Unavailable) ListSources(context.Context, Principal, string, int) (agentapi.SourcePage, error) {
	return agentapi.SourcePage{}, u.err()
}
func (u Unavailable) RegisterSource(context.Context, CommandIdentity, Principal, agentapi.RegisterSourceRequest) (agentapi.Source, error) {
	return agentapi.Source{}, u.err()
}
func (u Unavailable) GetSource(context.Context, Principal, string) (agentapi.Source, error) {
	return agentapi.Source{}, u.err()
}
func (u Unavailable) DeleteSource(context.Context, CommandIdentity, Principal, string, string) (agentapi.Source, error) {
	return agentapi.Source{}, u.err()
}
func (u Unavailable) ReplaceSourceAccess(context.Context, CommandIdentity, Principal, string, agentapi.ReplaceSourceAccessRequest) (agentapi.Source, error) {
	return agentapi.Source{}, u.err()
}
func (u Unavailable) Search(context.Context, Principal, agentapi.SearchRequest) (agentapi.SearchResponse, error) {
	return agentapi.SearchResponse{}, u.err()
}
func (u Unavailable) ListMemories(context.Context, Principal, agentapi.ListMemoriesParams) (agentapi.MemoryPage, error) {
	return agentapi.MemoryPage{}, u.err()
}
func (u Unavailable) ProposeMemory(context.Context, CommandIdentity, Principal, agentapi.ProposeMemoryRequest) (agentapi.MemoryFact, error) {
	return agentapi.MemoryFact{}, u.err()
}
func (u Unavailable) DecideMemory(context.Context, CommandIdentity, Principal, string, agentapi.MemoryDecisionRequest) (agentapi.MemoryFact, error) {
	return agentapi.MemoryFact{}, u.err()
}
func (u Unavailable) ListCatalog(context.Context, Principal, agentapi.ListCatalogParams) (agentapi.CatalogPage, error) {
	return agentapi.CatalogPage{}, u.err()
}
func (u Unavailable) DiscoverServer(context.Context, CommandIdentity, Principal, agentapi.DiscoverServerRequest) (agentapi.Descriptor, error) {
	return agentapi.Descriptor{}, u.err()
}
func (u Unavailable) ReviewDescriptor(context.Context, CommandIdentity, Principal, string, agentapi.ReviewDescriptorRequest) (agentapi.Descriptor, error) {
	return agentapi.Descriptor{}, u.err()
}
func (u Unavailable) ListGrants(context.Context, Principal, agentapi.ListGrantsParams) (agentapi.GrantPage, error) {
	return agentapi.GrantPage{}, u.err()
}
func (u Unavailable) CreateGrant(context.Context, CommandIdentity, Principal, agentapi.CreateGrantRequest) (agentapi.Grant, error) {
	return agentapi.Grant{}, u.err()
}
func (u Unavailable) GetGrant(context.Context, Principal, string) (agentapi.Grant, error) {
	return agentapi.Grant{}, u.err()
}
func (u Unavailable) GetRevocationProgress(context.Context, Principal, string) (agentapi.RevocationProgress, error) {
	return agentapi.RevocationProgress{}, u.err()
}
func (u Unavailable) RevokeGrant(context.Context, CommandIdentity, Principal, string, agentapi.RevokeGrantRequest) (agentapi.Grant, error) {
	return agentapi.Grant{}, u.err()
}
func (u Unavailable) CreateToolCall(context.Context, CommandIdentity, Principal, agentapi.CreateToolCallRequest) (agentapi.ToolCall, error) {
	return agentapi.ToolCall{}, u.err()
}
func (u Unavailable) GetToolCall(context.Context, Principal, string) (agentapi.ToolCall, error) {
	return agentapi.ToolCall{}, u.err()
}
