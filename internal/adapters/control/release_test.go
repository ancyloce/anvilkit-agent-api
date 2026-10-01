package control

import (
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// Only an activated release names the lock entry a page pins; a published
// or partially published one never does (P21).
func TestReleaseLockOnlyWhenActivated(t *testing.T) {
	id, manifest := "rel_1", "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	subject := &controlv1.ReleaseSubject{ComponentId: "cmp", PuckType: "Hero", PackageName: "@anvilkit/hero", Version: "1.0.0", HostAbi: "host-abi-dev-v1",
		Npm: &controlv1.ArtifactDigest{}, Browser: &controlv1.ArtifactDigest{}}
	target := &controlv1.ReleaseTarget{State: controlv1.TargetState_TARGET_STATE_SUCCEEDED, ManifestDigest: &manifest}
	for state, want := range map[controlv1.ReleaseState]bool{
		controlv1.ReleaseState_RELEASE_STATE_ACTIVATED: true, controlv1.ReleaseState_RELEASE_STATE_PUBLISHED: false,
		controlv1.ReleaseState_RELEASE_STATE_PARTIALLY_PUBLISHED: false, controlv1.ReleaseState_RELEASE_STATE_RECONCILING: false,
	} {
		out := toRelease(&controlv1.Release{OperationId: "op", State: state, Subject: subject, ReleaseId: &id, Npm: target, Browser: target, Activation: target, UpdatedAt: timestamppb.Now()})
		if (out.Lock != nil) != want {
			t.Fatalf("%s: lock %v", state, out.Lock)
		}
		if want && (out.Lock.ReleaseId != id || out.Lock.BrowserManifestDigest != manifest || out.Lock.PackageVersion != "1.0.0" || out.Lock.HostProfileId != "host-abi-dev-v1") {
			t.Fatalf("lock %+v", *out.Lock)
		}
	}
}
