package http_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	httptransport "github.com/ancyloce/anvilkit-agent-api/internal/transport/http"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

// fakeKnowledge and fakeMCP record what the gateways receive; every other
// call answers as unavailable.
type fakeKnowledge struct {
	application.Unavailable
	mu     sync.Mutex
	scopes []application.Principal
}

func (f *fakeKnowledge) ListSources(_ context.Context, p application.Principal, cursor string, limit int) (agentapi.SourcePage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scopes = append(f.scopes, p)
	now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	return agentapi.SourcePage{Sources: []agentapi.Source{{SourceId: "src_1", TenantId: p.TenantID, Kind: "document", Locator: "doc://a", CurrentRevision: "1",
		ContentDigest: digest, AclRevision: "1", Access: []agentapi.AccessEntry{}, Ingest: "indexed", CreatedAt: now, UpdatedAt: now}}}, nil
}

type fakeMCP struct {
	application.Unavailable
	mu    sync.Mutex
	cmds  []application.CommandIdentity
	calls []agentapi.CreateToolCallRequest
}

func (f *fakeMCP) CreateGrant(_ context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateGrantRequest) (agentapi.Grant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = append(f.cmds, cmd)
	return agentapi.Grant{}, &application.ControlError{Code: "FORBIDDEN", Message: "FORBIDDEN: gus is no grant manager"}
}

func (f *fakeMCP) CreateToolCall(_ context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateToolCallRequest) (agentapi.ToolCall, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, req)
	now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
	return agentapi.ToolCall{CallId: "call_1", TenantId: p.TenantID, GrantId: req.GrantId, GrantRevision: req.GrantRevision, ServerId: "srv_1", Method: req.Method,
		ArgumentDigest: digest, State: "succeeded", CreatedAt: now, UpdatedAt: now}, nil
}

func gatewayServer(t *testing.T, fake *fakeControl, gw httptransport.Gateways) *httptest.Server {
	t.Helper()
	srv, err := httptransport.NewServer(testOptions(), verifier{}, fake, gw, func(context.Context) error { return nil })
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func TestGateways(t *testing.T) {
	const tokenA = "token-tenant-a-0123456789"

	t.Run("an unplaced owner answers DEPENDENCY_UNAVAILABLE", func(t *testing.T) {
		ts := gatewayServer(t, newFake(), httptransport.Gateways{})
		for _, path := range []string{"/api/v1/knowledge/sources", "/api/v1/mcp/catalog", "/api/v1/mcp/grants", "/api/v1/memories"} {
			status, body, _ := do(t, ts, http.MethodGet, path, tokenA, "")
			require.Equal(t, http.StatusServiceUnavailable, status, path)
			require.Equal(t, "DEPENDENCY_UNAVAILABLE", body["error"].(map[string]any)["code"], path)
		}
	})

	t.Run("the scope is the authenticated principal's; owner refusals keep their code", func(t *testing.T) {
		k, m := &fakeKnowledge{}, &fakeMCP{}
		ts := gatewayServer(t, newFake(), httptransport.Gateways{Knowledge: k, MCP: m})
		status, body, _ := do(t, ts, http.MethodGet, "/api/v1/knowledge/sources?limit=10", tokenA, "")
		require.Equal(t, http.StatusOK, status)
		require.Len(t, body["sources"], 1)
		require.Equal(t, []application.Principal{{TenantID: "tenant_a", ActorID: "user_a"}}, k.scopes)
		grant := `{"commandId":"cmd_g1","subjectType":"project","subjectId":"proj_a","serverId":"srv_1","descriptorRevision":"1","descriptorDigest":"` + digest +
			`","methods":["search_issues"],"purpose":"triage","costCap":{"currency":"USD","amount":"1000"}}`
		status, body, _ = do(t, ts, http.MethodPost, "/api/v1/mcp/grants", tokenA, grant)
		require.Equal(t, http.StatusForbidden, status)
		require.Equal(t, "FORBIDDEN", body["error"].(map[string]any)["code"])
		require.Len(t, m.cmds, 1)
		require.Equal(t, "cmd_g1", m.cmds[0].CommandID)
		require.Equal(t, "tenant_a", m.cmds[0].TenantID)
		require.NotEmpty(t, m.cmds[0].RequestDigest, "the command binds the body digest")
	})

	t.Run("a tool call is validated against the contract before it reaches MCP", func(t *testing.T) {
		m := &fakeMCP{}
		ts := gatewayServer(t, newFake(), httptransport.Gateways{MCP: m})
		status, _, _ := do(t, ts, http.MethodPost, "/api/v1/mcp/calls", tokenA, `{"commandId":"c1","grantId":"g1","grantRevision":"1","method":"m","arguments":{}}`)
		require.Equal(t, http.StatusBadRequest, status, "the execution binding is required")
		status, body, _ := do(t, ts, http.MethodPost, "/api/v1/mcp/calls", tokenA,
			`{"commandId":"c1","grantId":"g1","grantRevision":"1","method":"search_issues","arguments":{"query":"open"},"operationId":"op_1","attemptId":"att_1","executionEpoch":"1"}`)
		require.Equal(t, http.StatusAccepted, status)
		require.Equal(t, "succeeded", body["state"])
		require.Len(t, m.calls, 1)
		require.Equal(t, map[string]any{"query": "open"}, m.calls[0].Arguments)
	})

	t.Run("preview artifacts are served as exact, non-sniffable, uncached bytes of the tenant's preview", func(t *testing.T) {
		fake := newFake()
		now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
		rev := "4"
		fake.previews["op_prv"] = agentapi.Preview{OperationId: "op_prv", SubjectDigest: digest, BaseRevision: "3", State: "ready", SourceRevision: &rev, SourceDigest: digest,
			Module: &agentapi.PreviewArtifact{Digest: digest, SizeBytes: "18", MediaType: "text/javascript"}, Styles: []agentapi.PreviewArtifact{},
			BuildProfileId: "build-support-dev-v1", HostProfileId: "host-abi-dev-v1", Revision: "3", UpdatedAt: now}
		fake.artifacts["op_prv/"+digest] = application.PreviewBytes{MediaType: "text/javascript", Body: []byte("export default {};")}
		ts := gatewayServer(t, fake, httptransport.Gateways{})
		status, body, _ := do(t, ts, http.MethodGet, "/api/v1/operations/op_prv/preview", tokenA, "")
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "ready", body["state"])
		req, err := http.NewRequest(http.MethodGet, ts.URL+"/api/v1/operations/op_prv/preview/artifacts/"+digest, nil)
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+tokenA)
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, string(raw))
		require.Equal(t, "export default {};", string(raw))
		require.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "text/javascript"))
		require.Equal(t, "nosniff", resp.Header.Get("X-Content-Type-Options"))
		require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
		status, _, _ = do(t, ts, http.MethodGet, "/api/v1/operations/op_prv/preview", "token-tenant-b-0123456789", "")
		require.Equal(t, http.StatusNotFound, status, "another tenant never sees the preview")
	})
}
