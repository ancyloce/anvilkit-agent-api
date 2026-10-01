package control

import (
	"context"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// Releases (P21): the committed release projection read from Control's
// ReleaseService and mapped to the public shape. Only an activated release
// names the lock entry a saved page pins; it is derived from the recorded
// subject, the release id and the verified browser manifest digest.

func toReleaseTarget(t *controlv1.ReleaseTarget) agentapi.ReleaseTarget {
	out := agentapi.ReleaseTarget{State: agentapi.TargetState(enumWord(t.GetState().String(), "TARGET_STATE_")), ReceiptId: t.ReceiptId, ReceiptDigest: t.ReceiptDigest,
		Destination: t.Destination, ManifestDigest: t.ManifestDigest, FailureCode: t.FailureCode}
	return out
}

func toRelease(r *controlv1.Release) agentapi.Release {
	out := agentapi.Release{
		OperationId: r.GetOperationId(), Lineage: r.GetLineage(), SourceRevision: r.GetSourceRevision(),
		State: agentapi.ReleaseState(enumWord(r.GetState().String(), "RELEASE_STATE_")), ReleaseId: r.ReleaseId,
		Npm: toReleaseTarget(r.GetNpm()), Browser: toReleaseTarget(r.GetBrowser()), Activation: toReleaseTarget(r.GetActivation()),
		CatalogRevision: r.CatalogRevision, FailureCode: r.FailureCode, Revision: r.GetRevision(), UpdatedAt: r.GetUpdatedAt().AsTime().UTC(),
	}
	if r.ApprovalDeadline != nil {
		t := r.GetApprovalDeadline().AsTime().UTC()
		out.ApprovalDeadline = &t
	}
	if a := r.GetApproval(); a != nil {
		ap := agentapi.Approval{State: agentapi.ApprovalState(enumWord(a.GetState().String(), "APPROVAL_STATE_")), SubjectDigest: a.GetSubjectDigest(), ApproverId: a.ApproverId, ReasonCode: a.ReasonCode}
		if a.DecidedAt != nil {
			t := a.GetDecidedAt().AsTime().UTC()
			ap.DecidedAt = &t
		}
		out.Approval = &ap
	}
	if s := r.GetSubject(); s != nil {
		ref := func(d *controlv1.ArtifactDigest) agentapi.ReleaseArtifactDigest {
			return agentapi.ReleaseArtifactDigest{Digest: d.GetDigest(), SizeBytes: d.GetSizeBytes()}
		}
		subj := agentapi.ReleaseSubject{
			ComponentId: s.GetComponentId(), PuckType: s.GetPuckType(), SourceRevision: s.GetSourceRevision(), SourceDigest: s.GetSourceDigest(),
			PackageName: s.GetPackageName(), Version: s.GetVersion(), Npm: ref(s.GetNpm()), Browser: ref(s.GetBrowser()), Css: []agentapi.ReleaseArtifactDigest{},
			BuildProfileId: s.GetBuildProfileId(), BuildProfileDigest: s.GetBuildProfileDigest(), ValidatorProfileId: s.GetValidatorProfileId(),
			ValidatorProfileDigest: s.GetValidatorProfileDigest(), HostAbi: s.GetHostAbi(), HostAbiDigest: s.GetHostAbiDigest(),
			CertificationEvidenceDigest: s.GetCertificationEvidenceDigest(), SubjectDigest: s.GetSubjectDigest(),
		}
		subj.Destinations.NpmRegistry, subj.Destinations.BrowserOrigin = s.GetNpmRegistry(), s.GetBrowserOrigin()
		for _, c := range s.GetCss() {
			subj.Css = append(subj.Css, ref(c))
		}
		out.Subject = &subj
		if r.GetState() == controlv1.ReleaseState_RELEASE_STATE_ACTIVATED && r.ReleaseId != nil && r.GetBrowser().ManifestDigest != nil {
			out.Lock = &agentapi.ReleaseLock{SchemaVersion: agentapi.N1, ComponentId: s.GetComponentId(), PuckType: s.GetPuckType(), ReleaseId: r.GetReleaseId(),
				PackageName: s.GetPackageName(), PackageVersion: s.GetVersion(), BrowserManifestDigest: r.GetBrowser().GetManifestDigest(), HostProfileId: s.GetHostAbi()}
		}
	}
	return out
}

func (c *Client) GetRelease(ctx context.Context, p application.Principal, operationID string) (application.ReleaseView, error) {
	resp, err := controlv1.NewReleaseServiceClient(c.conn).GetRelease(ctx, &controlv1.GetReleaseRequest{TenantId: p.TenantID, OperationId: operationID})
	if err != nil {
		return application.ReleaseView{}, mapErr(err)
	}
	return toRelease(resp.GetRelease()), nil
}
