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

// P2.6: the upload capability, snapshots and recall answer as Knowledge does.
func (f *fakeKnowledge) BeginSourceUpload(_ context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.BeginSourceUploadRequest) (agentapi.SourceUpload, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scopes = append(f.scopes, p)
	return agentapi.SourceUpload{UploadId: "upl-0123456789abcdef0123456789abcdef", UploadUrl: "https://objects.example.invalid/u?X-Amz-Signature=00", UploadMethod: "PUT",
		UploadHeaders: map[string]string{"content-length": req.SizeBytes, "content-type": req.MediaType}, ExpiresAt: time.Date(2026, 10, 13, 12, 15, 0, 0, time.UTC)}, nil
}

func (f *fakeKnowledge) CreateSnapshot(_ context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateSnapshotRequest) (agentapi.Snapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scopes = append(f.scopes, p)
	refs := []agentapi.SourceRef{}
	for _, id := range req.SourceIds {
		refs = append(refs, id+"@1")
	}
	return agentapi.Snapshot{SnapshotId: "snap_1", TenantId: p.TenantID, IndexGeneration: "1", SourceRevisions: refs, ContentDigest: digest, CreatedAt: time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)}, nil
}

func (f *fakeKnowledge) RecallMemories(_ context.Context, p application.Principal, req agentapi.RecallMemoriesRequest) (agentapi.RecallMemoriesResponse, error) {
	if req.Query == "denied" {
		return agentapi.RecallMemoriesResponse{}, &application.ControlError{Code: "FORBIDDEN", Message: "FORBIDDEN: commercial authorization denied"}
	}
	return agentapi.RecallMemoriesResponse{Facts: []agentapi.MemoryFact{}, NoAnswer: true, IndexGeneration: "1"}, nil
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
	mu         sync.Mutex
	cmds       []application.CommandIdentity
	calls      []agentapi.CreateToolCallRequest
	reconciled []string
}

// ReconcileToolCall answers as MCP does: the call-operator role decides.
func (f *fakeMCP) ReconcileToolCall(_ context.Context, p application.Principal, callID string) (agentapi.ToolCall, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reconciled = append(f.reconciled, p.TenantID+"/"+p.ActorID+"/"+callID)
	if callID == "call_forbidden" {
		return agentapi.ToolCall{}, &application.ControlError{Code: "FORBIDDEN", Message: "FORBIDDEN: no call operator"}
	}
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	return agentapi.ToolCall{CallId: callID, TenantId: p.TenantID, GrantId: "g1", GrantRevision: "1", ServerId: "srv_1", Method: "create_issue",
		ArgumentDigest: digest, State: "failed", CreatedAt: now, UpdatedAt: now}, nil
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

	t.Run("P2.6: upload capability, snapshot and recall routes carry the principal; refusals keep their code", func(t *testing.T) {
		k := &fakeKnowledge{}
		ts := gatewayServer(t, newFake(), httptransport.Gateways{Knowledge: k})
		status, body, _ := do(t, ts, http.MethodPost, "/api/v1/knowledge/sources/uploads", tokenA, `{"commandId":"up_1","contentDigest":"`+digest+`","sizeBytes":"12","mediaType":"text/markdown"}`)
		require.Equal(t, http.StatusCreated, status)
		require.Equal(t, "PUT", body["uploadMethod"])
		require.Equal(t, map[string]any{"content-length": "12", "content-type": "text/markdown"}, body["uploadHeaders"])
		status, body, _ = do(t, ts, http.MethodPost, "/api/v1/knowledge/snapshots", tokenA, `{"commandId":"snap_1","sourceIds":["src_1"]}`)
		require.Equal(t, http.StatusCreated, status)
		require.Equal(t, []any{"src_1@1"}, body["sourceRevisions"])
		status, _, _ = do(t, ts, http.MethodPost, "/api/v1/knowledge/snapshots", tokenA, `{"commandId":"snap_2","sourceIds":[]}`)
		require.Equal(t, http.StatusBadRequest, status, "a snapshot of nothing")
		status, body, _ = do(t, ts, http.MethodPost, "/api/v1/memories/recalls", tokenA, `{"query":"brand","retrievalProfileId":"hybrid-bge-m3-dev-v1"}`)
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, true, body["noAnswer"])
		status, body, _ = do(t, ts, http.MethodPost, "/api/v1/memories/recalls", tokenA, `{"query":"denied","retrievalProfileId":"hybrid-bge-m3-dev-v1"}`)
		require.Equal(t, http.StatusForbidden, status)
		require.Equal(t, "FORBIDDEN", body["error"].(map[string]any)["code"])
		status, _, _ = do(t, ts, http.MethodPost, "/api/v1/memories", tokenA, `{"commandId":"m1","subjectType":"project","subjectId":"proj_a","content":"teal","sourceRefs":["src_1"]}`)
		require.Equal(t, http.StatusBadRequest, status, "CON-02: a bare source id is not provenance")
		status, _, _ = do(t, ts, http.MethodPost, "/api/v1/knowledge/sources", tokenA, `{"commandId":"s1","kind":"url","locator":"https://example.org/","contentDigest":"`+digest+`","mediaType":"text/html","sizeBytes":"1"}`)
		require.Equal(t, http.StatusBadRequest, status, "an unimplemented kind is not public")
		require.Len(t, k.scopes, 2)
		require.Equal(t, application.Principal{TenantID: "tenant_a", ActorID: "user_a"}, k.scopes[0])
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

	t.Run("P2.5: the operator reconciliation passes the principal to MCP, which decides the role", func(t *testing.T) {
		m := &fakeMCP{}
		ts := gatewayServer(t, newFake(), httptransport.Gateways{MCP: m})
		status, body, _ := do(t, ts, http.MethodPost, "/api/v1/mcp/calls/call_7/reconciliations", tokenA, "")
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "failed", body["state"])
		status, body, _ = do(t, ts, http.MethodPost, "/api/v1/mcp/calls/call_forbidden/reconciliations", tokenA, "")
		require.Equal(t, http.StatusForbidden, status)
		require.Equal(t, "FORBIDDEN", body["error"].(map[string]any)["code"])
		require.Equal(t, []string{"tenant_a/user_a/call_7", "tenant_a/user_a/call_forbidden"}, m.reconciled)
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

	t.Run("a release is requested by its source operation and read with its lock only when activated", func(t *testing.T) {
		fake := newFake()
		ts := gatewayServer(t, fake, httptransport.Gateways{})
		status, _, _ := do(t, ts, http.MethodPost, "/api/v1/operations", tokenA,
			`{"commandId":"cmd_rel","kind":"release","subject":{"profileId":"release-v1","subjectDigest":"`+digest+`","sourceRevision":"4","sourceOperationId":"op_prv","packageVersion":"1.0.0","npmRegistry":"https://attacker.invalid/"}}`)
		require.Equal(t, http.StatusBadRequest, status, "a caller never names a destination")
		status, body, _ := do(t, ts, http.MethodPost, "/api/v1/operations", tokenA,
			`{"commandId":"cmd_rel","kind":"release","subject":{"profileId":"release-v1","subjectDigest":"`+digest+`","sourceRevision":"4","sourceOperationId":"op_prv","packageVersion":"1.0.0"}}`)
		require.Equal(t, http.StatusAccepted, status, body)
		require.Len(t, fake.subjects, 1)
		require.Equal(t, "op_prv", fake.subjects[0].SourceOperationID)
		require.Equal(t, "1.0.0", fake.subjects[0].PackageVersion)

		now := time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)
		rel := "rel_1"
		manifest := digest
		subject := &agentapi.ReleaseSubject{ComponentId: "cmp_hero", PuckType: "Hero", SourceRevision: "4", SourceDigest: digest, PackageName: "@anvilkit/hero", Version: "1.0.0",
			Npm: agentapi.ReleaseArtifactDigest{Digest: digest, SizeBytes: "10"}, Browser: agentapi.ReleaseArtifactDigest{Digest: digest, SizeBytes: "10"},
			Css: []agentapi.ReleaseArtifactDigest{{Digest: digest, SizeBytes: "3"}}, BuildProfileId: "build-support-dev-v1", BuildProfileDigest: digest,
			ValidatorProfileId: "validator-dev-v1", ValidatorProfileDigest: digest, HostAbi: "host-abi-dev-v1", HostAbiDigest: digest,
			CertificationEvidenceDigest: digest, SubjectDigest: digest}
		subject.Destinations.NpmRegistry, subject.Destinations.BrowserOrigin = "https://registry.anvilkit.invalid/", "https://components.anvilkit.invalid"
		fake.releases["op_rel"] = agentapi.Release{OperationId: "op_rel", Lineage: digest, SourceRevision: "4", State: "activated", Subject: subject, ReleaseId: &rel,
			Npm: agentapi.ReleaseTarget{State: "succeeded"}, Browser: agentapi.ReleaseTarget{State: "succeeded", ManifestDigest: &manifest},
			Activation: agentapi.ReleaseTarget{State: "succeeded"}, Lock: &agentapi.ReleaseLock{SchemaVersion: agentapi.N1, ComponentId: "cmp_hero", PuckType: "Hero",
				ReleaseId: rel, PackageName: "@anvilkit/hero", PackageVersion: "1.0.0", BrowserManifestDigest: manifest, HostProfileId: "host-abi-dev-v1"},
			Revision: "6", UpdatedAt: now}
		status, body, _ = do(t, ts, http.MethodGet, "/api/v1/operations/op_rel/release", tokenA, "")
		require.Equal(t, http.StatusOK, status)
		require.Equal(t, "activated", body["state"])
		require.Equal(t, "rel_1", body["lock"].(map[string]any)["releaseId"])
		status, _, _ = do(t, ts, http.MethodGet, "/api/v1/operations/op_rel/release", "token-tenant-b-0123456789", "")
		require.Equal(t, http.StatusNotFound, status, "another tenant never sees the release")
	})
}
