package control

import (
	"context"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// API-17 (F-P3.7-1): the operator's recovery routes over Control's
// RecoveryService. Every call carries the principal's scope; Control
// decides the operator role and the run's disclosure. The recovered scope
// is the principal's tenant, or every tenant ("*") when the operator asks
// for it — never a tenant named by the request.

func recoveryScopeKey(p application.Principal, allTenants *bool) string {
	if allTenants != nil && *allTenants {
		return "*"
	}
	return p.TenantID
}

func toRecoveryRun(r *controlv1.RecoveryRun) agentapi.RecoveryRun {
	out := agentapi.RecoveryRun{
		RunId: r.GetRunId(), ScopeKey: r.GetScopeKey(), RecoveryEpoch: r.GetRecoveryEpoch(),
		Phase:       agentapi.RecoveryPhase(enumWord(r.GetPhase().String(), "RECOVERY_PHASE_")),
		WindowStart: r.GetWindowStart().AsTime().UTC(), WindowEnd: r.GetWindowEnd().AsTime().UTC(),
		ClockUncertaintySeconds: int(r.GetClockUncertainty().AsDuration() / time.Second),
		FencedOperations:        r.GetFencedOperations(), UnsettledFindings: r.GetUnsettledFindings(),
		Progress: []agentapi.ClassProgress{}, CreatedAt: r.GetCreatedAt().AsTime().UTC(),
	}
	for _, p := range r.GetProgress() {
		out.Progress = append(out.Progress, agentapi.ClassProgress{Class: agentapi.ObligationClass(p.GetClass()), Complete: p.GetComplete(), Seen: p.GetSeen()})
	}
	if r.ReopenedAt != nil {
		t := r.GetReopenedAt().AsTime().UTC()
		out.ReopenedAt = &t
	}
	return out
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func toFinding(f *controlv1.Finding) agentapi.Finding {
	return agentapi.Finding{
		FindingId: f.GetFindingId(), RunId: f.GetRunId(), Class: agentapi.ObligationClass(f.GetClass()), ObligationId: f.GetObligationId(),
		TenantId: f.GetTenantId(), InventoryVersion: f.GetInventoryVersion(), RecordedAt: f.GetRecordedAt().AsTime().UTC(),
		Status:  agentapi.FindingStatus(enumWord(f.GetStatus().String(), "FINDING_STATUS_")),
		Outcome: optional(f.GetOutcome()), EvidenceRef: optional(f.GetEvidenceRef()), Detail: optional(f.GetDetail()), LaunchKey: optional(f.GetLaunchKey()),
	}
}

func (c *Client) BeginRecovery(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.BeginRecoveryRequest) (agentapi.RecoveryRun, bool, error) {
	in := &controlv1.BeginRecoveryRequest{
		Command: command(cmd), Scope: scope(p), ScopeKey: recoveryScopeKey(p, req.AllTenants),
		WindowStart: timestamppb.New(req.WindowStart), WindowEnd: timestamppb.New(req.WindowEnd),
	}
	if req.ClockUncertaintySeconds != nil {
		in.ClockUncertainty = durationpb.New(time.Duration(*req.ClockUncertaintySeconds) * time.Second)
	}
	if req.Reason != nil {
		in.Reason = *req.Reason
	}
	resp, err := controlv1.NewRecoveryServiceClient(c.conn).BeginRecovery(ctx, in)
	if err != nil {
		return agentapi.RecoveryRun{}, false, mapErr(err)
	}
	return toRecoveryRun(resp.GetRun()), resp.GetExisting(), nil
}

func (c *Client) GetRecovery(ctx context.Context, p application.Principal, runID string) (agentapi.RecoveryRun, error) {
	resp, err := controlv1.NewRecoveryServiceClient(c.conn).GetRecovery(ctx, &controlv1.GetRecoveryRequest{RunId: runID, Scope: scope(p)})
	if err != nil {
		return agentapi.RecoveryRun{}, mapErr(err)
	}
	return toRecoveryRun(resp.GetRun()), nil
}

func (c *Client) ListRecoveryFindings(ctx context.Context, p application.Principal, runID string, params agentapi.ListRecoveryFindingsParams) (agentapi.FindingPage, error) {
	in := &controlv1.ListFindingsRequest{RunId: runID, Scope: scope(p)}
	if params.Status != nil {
		in.Status = controlv1.FindingStatus(controlv1.FindingStatus_value["FINDING_STATUS_"+strings.ToUpper(string(*params.Status))])
	}
	if params.Cursor != nil {
		in.Cursor = *params.Cursor
	}
	if params.Limit != nil {
		in.Limit = int32(*params.Limit)
	}
	resp, err := controlv1.NewRecoveryServiceClient(c.conn).ListFindings(ctx, in)
	if err != nil {
		return agentapi.FindingPage{}, mapErr(err)
	}
	out := agentapi.FindingPage{Findings: []agentapi.Finding{}, Complete: resp.GetComplete(), NextCursor: optional(resp.GetNextCursor())}
	for _, f := range resp.GetFindings() {
		out.Findings = append(out.Findings, toFinding(f))
	}
	return out, nil
}

func (c *Client) DisposeObligation(ctx context.Context, cmd application.CommandIdentity, p application.Principal, runID string, req agentapi.DisposeObligationRequest) (agentapi.Disposition, bool, error) {
	in := &controlv1.DisposeObligationRequest{
		Command: command(cmd), Scope: scope(p), Class: string(req.Class), ObligationId: req.ObligationId,
		Decision:    controlv1.DispositionDecision(controlv1.DispositionDecision_value["DISPOSITION_DECISION_"+strings.ToUpper(string(req.Decision))]),
		EvidenceRef: req.EvidenceRef, EvidenceDigest: req.EvidenceDigest, RecoveryEpoch: req.RecoveryEpoch, RunId: &runID,
	}
	if req.Reason != nil {
		in.Reason = *req.Reason
	}
	resp, err := controlv1.NewRecoveryServiceClient(c.conn).DisposeObligation(ctx, in)
	if err != nil {
		return agentapi.Disposition{}, false, mapErr(err)
	}
	d := resp.GetDisposition()
	out := agentapi.Disposition{
		DispositionId: d.GetDispositionId(), RunId: d.GetRunId(), Class: agentapi.ObligationClass(d.GetClass()), ObligationId: d.GetObligationId(),
		Decision:    agentapi.DispositionDecision(enumWord(d.GetDecision().String(), "DISPOSITION_DECISION_")),
		EvidenceRef: d.GetEvidenceRef(), EvidenceDigest: d.GetEvidenceDigest(), RecoveryEpoch: d.GetRecoveryEpoch(), DecidedAt: d.GetDecidedAt().AsTime().UTC(),
	}
	if out.RunId == "" {
		out.RunId = runID
	}
	return out, resp.GetExisting(), nil
}
