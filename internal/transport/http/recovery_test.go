package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	httptransport "github.com/ancyloce/anvilkit-agent-api/internal/transport/http"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

// API-17 (F-P3.7-1): the operator's recovery routes. The fake answers like
// Control's RecoveryService for the transport concerns under test (the
// operator role, run disclosure, command idempotency, one run per scope);
// the business rules themselves are proven in Control.

const (
	tokenOperatorA = "token-operator-a-0123456789"
	tokenOperatorB = "token-operator-b-0123456789"
	tokenPlatform  = "token-platform-a-0123456789"
)

type operatorVerifier struct{}

func (operatorVerifier) Verify(ctx context.Context, bearer string) (application.Principal, error) {
	switch bearer {
	case tokenOperatorA:
		return application.Principal{TenantID: "tenant_a", ActorID: "op_a", Roles: []string{"operator"}}, nil
	case tokenOperatorB:
		return application.Principal{TenantID: "tenant_b", ActorID: "op_b", Roles: []string{"operator"}}, nil
	case tokenPlatform:
		return application.Principal{TenantID: "tenant_a", ActorID: "plat_a", Roles: []string{"platform_operator"}}, nil
	}
	return verifier{}.Verify(ctx, bearer)
}

type recoveryCommand struct {
	digest string
	run    agentapi.RecoveryRun
}

type fakeRecovery struct {
	begun        map[string]recoveryCommand // tenant/commandId
	open         map[string]string          // scope key -> run id
	runs         map[string]agentapi.RecoveryRun
	dispositions map[string]recoveryCommand
	principals   []application.Principal
}

func (f *fakeControl) recovery() *fakeRecovery {
	if f.rec == nil {
		f.rec = &fakeRecovery{begun: map[string]recoveryCommand{}, open: map[string]string{}, runs: map[string]agentapi.RecoveryRun{}, dispositions: map[string]recoveryCommand{}}
	}
	return f.rec
}

func operatorOf(p application.Principal, scopeKey string) bool {
	if scopeKey == "*" {
		return slices.Contains(p.Roles, "platform_operator")
	}
	return p.TenantID == scopeKey && (slices.Contains(p.Roles, "operator") || slices.Contains(p.Roles, "platform_operator"))
}

func (f *fakeControl) BeginRecovery(_ context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.BeginRecoveryRequest) (agentapi.RecoveryRun, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.recovery()
	r.principals = append(r.principals, p)
	scopeKey := p.TenantID
	if req.AllTenants != nil && *req.AllTenants {
		scopeKey = "*"
	}
	if !operatorOf(p, scopeKey) {
		return agentapi.RecoveryRun{}, false, &application.ControlError{Code: "FORBIDDEN", Message: "the operator role is required"}
	}
	key := cmd.TenantID + "/" + cmd.CommandID
	if prior, ok := r.begun[key]; ok {
		if prior.digest != cmd.RequestDigest {
			return agentapi.RecoveryRun{}, false, &application.ControlError{Code: "IDEMPOTENCY_CONFLICT", Message: "recovery command " + cmd.CommandID}
		}
		return prior.run, true, nil
	}
	if _, busy := r.open[scopeKey]; busy {
		return agentapi.RecoveryRun{}, false, &application.ControlError{Code: "STALE_EXECUTION", Message: "RECOVERY_IN_PROGRESS"}
	}
	run := agentapi.RecoveryRun{RunId: "rec_" + cmd.CommandID, ScopeKey: scopeKey, RecoveryEpoch: "1", Phase: agentapi.RecoveryPhaseFenced,
		WindowStart: req.WindowStart, WindowEnd: req.WindowEnd, FencedOperations: "0", UnsettledFindings: "0", Progress: []agentapi.ClassProgress{}, CreatedAt: time.Now().UTC()}
	r.begun[key], r.open[scopeKey], r.runs[run.RunId] = recoveryCommand{digest: cmd.RequestDigest, run: run}, run.RunId, run
	return run, false, nil
}

func (f *fakeControl) GetRecovery(_ context.Context, p application.Principal, runID string) (agentapi.RecoveryRun, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.recovery().runs[runID]
	if !ok || !operatorOf(p, run.ScopeKey) {
		return agentapi.RecoveryRun{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
	}
	return run, nil
}

func (f *fakeControl) ListRecoveryFindings(_ context.Context, p application.Principal, runID string, params agentapi.ListRecoveryFindingsParams) (agentapi.FindingPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	run, ok := f.recovery().runs[runID]
	if !ok || !operatorOf(p, run.ScopeKey) {
		return agentapi.FindingPage{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
	}
	next := "model-dispatch/dsp_1"
	if params.Cursor != nil {
		return agentapi.FindingPage{Findings: []agentapi.Finding{}, Complete: true}, nil
	}
	return agentapi.FindingPage{Findings: []agentapi.Finding{{FindingId: "fnd_1", RunId: runID, Class: agentapi.ObligationClass("model-dispatch"), ObligationId: "dsp_1",
		TenantId: "tenant_a", InventoryVersion: "v1", RecordedAt: time.Now().UTC(), Status: agentapi.Unresolved}}, NextCursor: &next}, nil
}

func (f *fakeControl) DisposeObligation(_ context.Context, cmd application.CommandIdentity, p application.Principal, runID string, req agentapi.DisposeObligationRequest) (agentapi.Disposition, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.recovery()
	if !operatorOf(p, p.TenantID) {
		return agentapi.Disposition{}, false, &application.ControlError{Code: "FORBIDDEN", Message: "the operator role is required"}
	}
	key := cmd.TenantID + "/" + cmd.CommandID
	d := agentapi.Disposition{DispositionId: "dsp_" + cmd.CommandID, RunId: runID, Class: req.Class, ObligationId: req.ObligationId, Decision: req.Decision,
		EvidenceRef: req.EvidenceRef, EvidenceDigest: req.EvidenceDigest, RecoveryEpoch: req.RecoveryEpoch, DecidedAt: time.Now().UTC()}
	if prior, ok := r.dispositions[key]; ok {
		if prior.digest != cmd.RequestDigest {
			return agentapi.Disposition{}, false, &application.ControlError{Code: "IDEMPOTENCY_CONFLICT", Message: "disposition command " + cmd.CommandID}
		}
		return d, true, nil
	}
	r.dispositions[key] = recoveryCommand{digest: cmd.RequestDigest}
	return d, false, nil
}

func recoveryServer(t *testing.T) (*httptest.Server, *fakeControl) {
	t.Helper()
	fake := newFake()
	srv, err := httptransport.NewServer(testOptions(), operatorVerifier{}, fake, httptransport.Gateways{}, func(context.Context) error { return nil })
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, fake
}

func TestOperatorRecoveryRoutes(t *testing.T) {
	ts, fake := recoveryServer(t)
	begin := `{"commandId":"incident-1","windowStart":"2026-10-18T10:00:00Z","windowEnd":"2026-10-18T11:00:00Z","clockUncertaintySeconds":5,"reason":"PITR drill"}`

	code, out, _ := do(t, ts, http.MethodPost, "/api/v1/recoveries", "", begin)
	require.Equal(t, http.StatusUnauthorized, code, "no bearer token")
	require.Equal(t, "UNAUTHENTICATED", errorCode(out))

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries", "token-tenant-a-0123456789", begin)
	require.Equal(t, http.StatusForbidden, code, "a tenant member without the operator role")
	require.Equal(t, "FORBIDDEN", errorCode(out))

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries", tokenOperatorA, strings.Replace(begin, `"commandId"`, `"tenantId":"tenant_b","commandId"`, 1))
	require.Equal(t, http.StatusBadRequest, code, "the scope comes from the principal, never the body")
	require.Equal(t, "INVALID_ARGUMENT", errorCode(out))

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries", tokenOperatorA, strings.Replace(begin, `"clockUncertaintySeconds":5`, `"clockUncertaintySeconds":86401`, 1))
	require.Equal(t, http.StatusBadRequest, code, "an unbounded clock uncertainty")

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries", tokenOperatorA, begin)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, "tenant_a", out["scopeKey"], "the principal's tenant")
	require.Equal(t, "fenced", out["phase"])
	runID := out["runId"].(string)

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries", tokenOperatorA, " "+strings.ReplaceAll(begin, ",", " , "))
	require.Equal(t, http.StatusOK, code, "the same command with another formatting returns the original run")
	require.Equal(t, runID, out["runId"])

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries", tokenOperatorA, strings.Replace(begin, "PITR drill", "other", 1))
	require.Equal(t, http.StatusConflict, code, "the same command with another body")
	require.Equal(t, "IDEMPOTENCY_CONFLICT", errorCode(out))

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries", tokenOperatorA, strings.Replace(begin, "incident-1", "incident-2", 1))
	require.Equal(t, http.StatusConflict, code, "one run per scope")
	require.Equal(t, "STALE_EXECUTION", errorCode(out))

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries", tokenOperatorA, strings.Replace(begin, `"commandId":"incident-1"`, `"commandId":"all-1","allTenants":true`, 1))
	require.Equal(t, http.StatusForbidden, code, "every tenant needs platform_operator")

	code, out, _ = do(t, ts, http.MethodGet, "/api/v1/recoveries/"+runID, tokenOperatorA, "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, runID, out["runId"])
	assertFrameSchema(t, "RecoveryRun", mustJSON(t, out))

	code, out, _ = do(t, ts, http.MethodGet, "/api/v1/recoveries/"+runID, tokenOperatorB, "")
	require.Equal(t, http.StatusNotFound, code, "another tenant's operator learns nothing")
	code, _, _ = do(t, ts, http.MethodGet, "/api/v1/recoveries/"+runID+"/findings", tokenOperatorB, "")
	require.Equal(t, http.StatusNotFound, code)

	code, out, _ = do(t, ts, http.MethodGet, "/api/v1/recoveries/"+runID+"/findings?status=unresolved&limit=1", tokenOperatorA, "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, false, out["complete"])
	assertFrameSchema(t, "FindingPage", mustJSON(t, out))
	code, out, _ = do(t, ts, http.MethodGet, "/api/v1/recoveries/"+runID+"/findings?cursor="+out["nextCursor"].(string), tokenOperatorA, "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, true, out["complete"])
	code, _, _ = do(t, ts, http.MethodGet, "/api/v1/recoveries/"+runID+"/findings?status=lost", tokenOperatorA, "")
	require.Equal(t, http.StatusBadRequest, code, "an undeclared status")

	dispose := `{"commandId":"dispose-1","class":"model-dispatch","obligationId":"dsp_1","decision":"confirm_not_sent","evidenceRef":"proxy:not-sent:dsp_1","evidenceDigest":"` + digest + `","recoveryEpoch":"1"}`
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries/"+runID+"/dispositions", tokenOperatorA, strings.Replace(dispose, `,"evidenceDigest":"`+digest+`"`, "", 1))
	require.Equal(t, http.StatusBadRequest, code, "a disposition without evidence")
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries/"+runID+"/dispositions", tokenOperatorA, strings.Replace(dispose, "confirm_not_sent", "resend", 1))
	require.Equal(t, http.StatusBadRequest, code, "an undeclared decision")
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries/"+runID+"/dispositions", "token-tenant-a-0123456789", dispose)
	require.Equal(t, http.StatusForbidden, code, "a disposition needs the operator role")
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries/"+runID+"/dispositions", tokenOperatorA, dispose)
	require.Equal(t, http.StatusCreated, code)
	require.Equal(t, runID, out["runId"])
	assertFrameSchema(t, "Disposition", mustJSON(t, out))
	code, _, _ = do(t, ts, http.MethodPost, "/api/v1/recoveries/"+runID+"/dispositions", tokenOperatorA, dispose)
	require.Equal(t, http.StatusOK, code, "the same disposition command is idempotent")

	for _, p := range fake.rec.principals {
		if p.ActorID == "op_a" {
			require.Equal(t, []string{"operator"}, p.Roles, "the verified roles reach Control")
		}
	}
}
