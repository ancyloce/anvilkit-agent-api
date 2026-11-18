package control

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// DEC-066: the platform operator's pool routes over Control's
// BudgetPoolService. Every call carries the principal's scope; Control
// decides platform_operator for a change and operator for a read of the
// principal's own tenant. The pool's scope (level, tenant, actor) is the
// administered pool's, named by the request; it never widens the caller's.

var budgetLevels = map[agentapi.BudgetLevel]controlv1.BudgetLevel{
	agentapi.BudgetLevelPlatform: controlv1.BudgetLevel_BUDGET_LEVEL_PLATFORM,
	agentapi.BudgetLevelTenant:   controlv1.BudgetLevel_BUDGET_LEVEL_TENANT,
	agentapi.BudgetLevelActor:    controlv1.BudgetLevel_BUDGET_LEVEL_ACTOR,
}

var creditLevels = map[agentapi.CreditLevel]controlv1.BudgetLevel{
	agentapi.CreditLevelTenant: controlv1.BudgetLevel_BUDGET_LEVEL_TENANT,
	agentapi.CreditLevelActor:  controlv1.BudgetLevel_BUDGET_LEVEL_ACTOR,
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func toMoney(m agentapi.Money) *controlv1.Money {
	return &controlv1.Money{Currency: m.Currency, Amount: m.Amount}
}

func fromMoney(m *controlv1.Money) agentapi.Money {
	return agentapi.Money{Currency: m.GetCurrency(), Amount: m.GetAmount()}
}

func toBudgetPool(p *controlv1.BudgetPool) agentapi.BudgetPool {
	return agentapi.BudgetPool{
		PoolId: p.GetPoolId(), Level: agentapi.BudgetLevel(enumWord(p.GetLevel().String(), "BUDGET_LEVEL_")),
		TenantId: optional(p.GetTenantId()), ActorId: optional(p.GetActorId()),
		PeriodStart: p.GetPeriodStart().AsTime().UTC(), PeriodEnd: p.GetPeriodEnd().AsTime().UTC(),
		Cap: fromMoney(p.GetCap()), Allocated: fromMoney(p.GetAllocated()), Available: fromMoney(p.GetAvailable()), Revision: p.GetRevision(),
	}
}

func toCreditPool(p *controlv1.CreditPool) agentapi.CreditPool {
	return agentapi.CreditPool{
		CreditPoolId: p.GetCreditPoolId(), Level: agentapi.CreditLevel(enumWord(p.GetLevel().String(), "BUDGET_LEVEL_")),
		TenantId: p.GetTenantId(), ActorId: optional(p.GetActorId()),
		PeriodStart: p.GetPeriodStart().AsTime().UTC(), PeriodEnd: p.GetPeriodEnd().AsTime().UTC(),
		Limit: int(p.GetLimit()), Used: int(p.GetUsed()), Available: int(p.GetAvailable()), Revision: p.GetRevision(),
	}
}

func (c *Client) CreateBudgetPool(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateBudgetPoolRequest) (agentapi.BudgetPool, bool, error) {
	resp, err := controlv1.NewBudgetPoolServiceClient(c.conn).CreatePool(ctx, &controlv1.CreatePoolRequest{
		Command: command(cmd), Scope: scope(p), Level: budgetLevels[req.Level], TenantId: deref(req.TenantId), ActorId: deref(req.ActorId),
		PeriodStart: timestamppb.New(req.PeriodStart), PeriodEnd: timestamppb.New(req.PeriodEnd), Cap: toMoney(req.Cap),
	})
	if err != nil {
		return agentapi.BudgetPool{}, false, mapErr(err)
	}
	return toBudgetPool(resp.GetPool()), resp.GetExisting(), nil
}

func (c *Client) FindBudgetPool(ctx context.Context, p application.Principal, params agentapi.FindBudgetPoolParams) (agentapi.BudgetPool, error) {
	resp, err := controlv1.NewBudgetPoolServiceClient(c.conn).FindPool(ctx, &controlv1.FindPoolRequest{
		Scope: scope(p), Level: budgetLevels[params.Level], TenantId: deref(params.TenantId), ActorId: deref(params.ActorId), PeriodStart: timestamppb.New(params.PeriodStart),
	})
	if err != nil {
		return agentapi.BudgetPool{}, mapErr(err)
	}
	return toBudgetPool(resp.GetPool()), nil
}

func (c *Client) AdjustBudgetPool(ctx context.Context, cmd application.CommandIdentity, p application.Principal, poolID string, req agentapi.AdjustBudgetPoolRequest) (agentapi.BudgetPool, error) {
	resp, err := controlv1.NewBudgetPoolServiceClient(c.conn).AdjustPool(ctx, &controlv1.AdjustPoolRequest{
		Command: command(cmd), Scope: scope(p), PoolId: poolID, ExpectedRevision: req.ExpectedRevision, Cap: toMoney(req.Cap),
	})
	if err != nil {
		return agentapi.BudgetPool{}, mapErr(err)
	}
	return toBudgetPool(resp.GetPool()), nil
}

func (c *Client) CreateCreditPool(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateCreditPoolRequest) (agentapi.CreditPool, bool, error) {
	resp, err := controlv1.NewBudgetPoolServiceClient(c.conn).CreateCreditPool(ctx, &controlv1.CreateCreditPoolRequest{
		Command: command(cmd), Scope: scope(p), Level: creditLevels[req.Level], TenantId: req.TenantId, ActorId: deref(req.ActorId),
		PeriodStart: timestamppb.New(req.PeriodStart), PeriodEnd: timestamppb.New(req.PeriodEnd), Limit: int64(req.Limit),
	})
	if err != nil {
		return agentapi.CreditPool{}, false, mapErr(err)
	}
	return toCreditPool(resp.GetPool()), resp.GetExisting(), nil
}

func (c *Client) FindCreditPool(ctx context.Context, p application.Principal, params agentapi.FindCreditPoolParams) (agentapi.CreditPool, error) {
	resp, err := controlv1.NewBudgetPoolServiceClient(c.conn).FindCreditPool(ctx, &controlv1.FindCreditPoolRequest{
		Scope: scope(p), Level: creditLevels[params.Level], TenantId: params.TenantId, ActorId: deref(params.ActorId), PeriodStart: timestamppb.New(params.PeriodStart),
	})
	if err != nil {
		return agentapi.CreditPool{}, mapErr(err)
	}
	return toCreditPool(resp.GetPool()), nil
}

func (c *Client) AdjustCreditPool(ctx context.Context, cmd application.CommandIdentity, p application.Principal, creditPoolID string, req agentapi.AdjustCreditPoolRequest) (agentapi.CreditPool, error) {
	resp, err := controlv1.NewBudgetPoolServiceClient(c.conn).AdjustCreditPool(ctx, &controlv1.AdjustCreditPoolRequest{
		Command: command(cmd), Scope: scope(p), CreditPoolId: creditPoolID, ExpectedRevision: req.ExpectedRevision, Limit: int64(req.Limit),
	})
	if err != nil {
		return agentapi.CreditPool{}, mapErr(err)
	}
	return toCreditPool(resp.GetPool()), nil
}
