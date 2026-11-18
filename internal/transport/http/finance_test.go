package http_test

import (
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

// The platform operator's pool routes (DEC-066). The fake stands in for
// Control's BudgetPoolService for the transport concerns under test (the
// strict request shapes, the role refusal Control answers, 200 for a pool
// Control already held, 201 for a new one, revision conflicts); the
// business rules themselves are proven in Control.

type fakeFinance struct {
	budget map[string]agentapi.BudgetPool // scope key -> pool
	credit map[string]agentapi.CreditPool
}

func (f *fakeControl) pools() *fakeFinance {
	if f.finance == nil {
		f.finance = &fakeFinance{budget: map[string]agentapi.BudgetPool{}, credit: map[string]agentapi.CreditPool{}}
	}
	return f.finance
}

func poolKey(level string, tenant, actor *string, start agentapi.Timestamp) string {
	deref := func(s *string) string {
		if s == nil {
			return ""
		}
		return *s
	}
	return level + "/" + deref(tenant) + "/" + deref(actor) + "/" + start.UTC().String()
}

var forbidden = &application.ControlError{Code: "FORBIDDEN", Message: "the platform_operator role is required"}

func platformOperatorOf(p application.Principal) bool {
	return slices.Contains(p.Roles, "platform_operator")
}

func (f *fakeControl) CreateBudgetPool(_ context.Context, _ application.CommandIdentity, p application.Principal, req agentapi.CreateBudgetPoolRequest) (agentapi.BudgetPool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !platformOperatorOf(p) {
		return agentapi.BudgetPool{}, false, forbidden
	}
	key := poolKey(string(req.Level), req.TenantId, req.ActorId, req.PeriodStart)
	if prior, ok := f.pools().budget[key]; ok {
		if prior.Cap != req.Cap || !prior.PeriodEnd.Equal(req.PeriodEnd) {
			return agentapi.BudgetPool{}, false, &application.ControlError{Code: "IDEMPOTENCY_CONFLICT", Message: "other terms"}
		}
		return prior, true, nil
	}
	pool := agentapi.BudgetPool{PoolId: fmt.Sprintf("pool_%d", len(f.pools().budget)+1), Level: req.Level, TenantId: req.TenantId, ActorId: req.ActorId,
		PeriodStart: req.PeriodStart, PeriodEnd: req.PeriodEnd, Cap: req.Cap, Allocated: agentapi.Money{Currency: req.Cap.Currency, Amount: "0"}, Available: req.Cap, Revision: "1"}
	f.pools().budget[key] = pool
	return pool, false, nil
}

func (f *fakeControl) FindBudgetPool(_ context.Context, p application.Principal, params agentapi.FindBudgetPoolParams) (agentapi.BudgetPool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !platformOperatorOf(p) {
		return agentapi.BudgetPool{}, forbidden
	}
	pool, ok := f.pools().budget[poolKey(string(params.Level), params.TenantId, params.ActorId, params.PeriodStart)]
	if !ok {
		return agentapi.BudgetPool{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
	}
	return pool, nil
}

func (f *fakeControl) AdjustBudgetPool(_ context.Context, _ application.CommandIdentity, p application.Principal, poolID string, req agentapi.AdjustBudgetPoolRequest) (agentapi.BudgetPool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !platformOperatorOf(p) {
		return agentapi.BudgetPool{}, forbidden
	}
	for key, pool := range f.pools().budget {
		if pool.PoolId != poolID {
			continue
		}
		if pool.Revision != req.ExpectedRevision {
			return agentapi.BudgetPool{}, &application.ControlError{Code: "REVISION_CONFLICT", Message: "pool at revision " + pool.Revision}
		}
		pool.Cap, pool.Available, pool.Revision = req.Cap, req.Cap, "2"
		f.pools().budget[key] = pool
		return pool, nil
	}
	return agentapi.BudgetPool{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
}

func (f *fakeControl) CreateCreditPool(_ context.Context, _ application.CommandIdentity, p application.Principal, req agentapi.CreateCreditPoolRequest) (agentapi.CreditPool, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !platformOperatorOf(p) {
		return agentapi.CreditPool{}, false, forbidden
	}
	key := poolKey(string(req.Level), &req.TenantId, req.ActorId, req.PeriodStart)
	if prior, ok := f.pools().credit[key]; ok {
		if prior.Limit != req.Limit || !prior.PeriodEnd.Equal(req.PeriodEnd) {
			return agentapi.CreditPool{}, false, &application.ControlError{Code: "IDEMPOTENCY_CONFLICT", Message: "other terms"}
		}
		return prior, true, nil
	}
	pool := agentapi.CreditPool{CreditPoolId: fmt.Sprintf("crp_%d", len(f.pools().credit)+1), Level: req.Level, TenantId: req.TenantId, ActorId: req.ActorId,
		PeriodStart: req.PeriodStart, PeriodEnd: req.PeriodEnd, Limit: req.Limit, Available: req.Limit, Revision: "1"}
	f.pools().credit[key] = pool
	return pool, false, nil
}

func (f *fakeControl) FindCreditPool(_ context.Context, p application.Principal, params agentapi.FindCreditPoolParams) (agentapi.CreditPool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !platformOperatorOf(p) && !(p.TenantID == params.TenantId && slices.Contains(p.Roles, "operator")) {
		return agentapi.CreditPool{}, forbidden
	}
	pool, ok := f.pools().credit[poolKey(string(params.Level), &params.TenantId, params.ActorId, params.PeriodStart)]
	if !ok {
		return agentapi.CreditPool{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
	}
	return pool, nil
}

func (f *fakeControl) AdjustCreditPool(_ context.Context, _ application.CommandIdentity, p application.Principal, creditPoolID string, req agentapi.AdjustCreditPoolRequest) (agentapi.CreditPool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !platformOperatorOf(p) {
		return agentapi.CreditPool{}, forbidden
	}
	for key, pool := range f.pools().credit {
		if pool.CreditPoolId != creditPoolID {
			continue
		}
		if pool.Revision != req.ExpectedRevision {
			return agentapi.CreditPool{}, &application.ControlError{Code: "REVISION_CONFLICT", Message: "credit pool at revision " + pool.Revision}
		}
		pool.Limit, pool.Available, pool.Revision = req.Limit, max(0, req.Limit-pool.Used), "2"
		f.pools().credit[key] = pool
		return pool, nil
	}
	return agentapi.CreditPool{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
}

func TestPlatformOperatorPoolRoutes(t *testing.T) {
	ts, _ := recoveryServer(t)
	credit := `{"commandId":"credit-1","level":"actor","tenantId":"tenant_a","actorId":"user_a","periodStart":"2026-12-01T00:00:00Z","periodEnd":"2027-01-01T00:00:00Z","limit":20}`

	code, out, _ := do(t, ts, http.MethodPost, "/api/v1/credit-pools", "token-tenant-a-0123456789", credit)
	require.Equal(t, http.StatusForbidden, code, "a tenant member without platform_operator")
	require.Equal(t, "FORBIDDEN", errorCode(out))

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/credit-pools", tokenPlatform, strings.Replace(credit, `"level":"actor"`, `"level":"platform"`, 1))
	require.Equal(t, http.StatusBadRequest, code, "there is no platform credit pool")
	require.Equal(t, "INVALID_ARGUMENT", errorCode(out))
	code, _, _ = do(t, ts, http.MethodPost, "/api/v1/credit-pools", tokenPlatform, strings.Replace(credit, `"limit":20`, `"limit":"20"`, 1))
	require.Equal(t, http.StatusBadRequest, code, "credits are a JSON integer")
	code, _, _ = do(t, ts, http.MethodPost, "/api/v1/credit-pools", tokenPlatform, strings.Replace(credit, `"limit":20`, `"limit":1000000001`, 1))
	require.Equal(t, http.StatusBadRequest, code, "a limit above the bound")

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/credit-pools", tokenPlatform, credit)
	require.Equal(t, http.StatusCreated, code)
	assertFrameSchema(t, "CreditPool", mustJSON(t, out))
	poolID := out["creditPoolId"].(string)
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/credit-pools", tokenPlatform, strings.Replace(credit, "credit-1", "credit-2", 1))
	require.Equal(t, http.StatusOK, code, "the same terms return the existing pool")
	require.Equal(t, poolID, out["creditPoolId"])
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/credit-pools", tokenPlatform, strings.Replace(strings.Replace(credit, "credit-1", "credit-3", 1), `"limit":20`, `"limit":30`, 1))
	require.Equal(t, http.StatusConflict, code, "other terms for the same scope and period")
	require.Equal(t, "IDEMPOTENCY_CONFLICT", errorCode(out))

	find := "/api/v1/credit-pools?level=actor&tenantId=tenant_a&actorId=user_a&periodStart=2026-12-01T00:00:00Z"
	code, out, _ = do(t, ts, http.MethodGet, find, tokenOperatorA, "")
	require.Equal(t, http.StatusOK, code, "an operator reads its own tenant's credit pool")
	require.Equal(t, poolID, out["creditPoolId"])
	code, _, _ = do(t, ts, http.MethodGet, find, tokenOperatorB, "")
	require.Equal(t, http.StatusForbidden, code, "another tenant's operator")
	code, _, _ = do(t, ts, http.MethodGet, "/api/v1/credit-pools?level=actor&tenantId=tenant_a&actorId=user_b&periodStart=2026-12-01T00:00:00Z", tokenPlatform, "")
	require.Equal(t, http.StatusNotFound, code)
	code, _, _ = do(t, ts, http.MethodGet, "/api/v1/credit-pools?level=actor&periodStart=2026-12-01T00:00:00Z", tokenPlatform, "")
	require.Equal(t, http.StatusBadRequest, code, "a credit pool is always a tenant's")

	adjust := `{"commandId":"credit-adjust-1","expectedRevision":"2","limit":25}`
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/credit-pools/"+poolID+"/adjustments", tokenPlatform, adjust)
	require.Equal(t, http.StatusConflict, code)
	require.Equal(t, "REVISION_CONFLICT", errorCode(out))
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/credit-pools/"+poolID+"/adjustments", tokenPlatform, strings.Replace(adjust, `"expectedRevision":"2"`, `"expectedRevision":"1"`, 1))
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, float64(25), out["limit"])
	assertFrameSchema(t, "CreditPool", mustJSON(t, out))

	budget := `{"commandId":"pool-1","level":"platform","periodStart":"2026-12-01T00:00:00Z","periodEnd":"2027-01-01T00:00:00Z","cap":{"currency":"USD","amount":"30000000"}}`
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/budget-pools", tokenOperatorA, budget)
	require.Equal(t, http.StatusForbidden, code, "an operator does not fund pools")
	code, _, _ = do(t, ts, http.MethodPost, "/api/v1/budget-pools", tokenPlatform, strings.Replace(budget, `"amount":"30000000"`, `"amount":30`, 1))
	require.Equal(t, http.StatusBadRequest, code, "money is a decimal string")
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/budget-pools", tokenPlatform, budget)
	require.Equal(t, http.StatusCreated, code)
	assertFrameSchema(t, "BudgetPool", mustJSON(t, out))
	budgetID := out["poolId"].(string)
	code, out, _ = do(t, ts, http.MethodGet, "/api/v1/budget-pools?level=platform&periodStart=2026-12-01T00:00:00Z", tokenPlatform, "")
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, budgetID, out["poolId"])
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/budget-pools/"+budgetID+"/adjustments", tokenPlatform, `{"commandId":"pool-adjust-1","expectedRevision":"1","cap":{"currency":"USD","amount":"40000000"}}`)
	require.Equal(t, http.StatusOK, code)
	require.Equal(t, "40000000", out["cap"].(map[string]any)["amount"])
}
