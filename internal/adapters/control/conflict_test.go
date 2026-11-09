package control_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

// RB-A23: a Control revision conflict's ErrorInfo detail gives the public
// error both revisions; only Control's reason and domain count, only
// canonical uint64 decimals pass, and another owner's conflict carries none.
func TestRevisionConflictDetailsComeFromControlOnly(t *testing.T) {
	conflict := func(t *testing.T, reason, domain string, md map[string]string) error {
		t.Helper()
		st, err := status.New(codes.Aborted, "REVISION_CONFLICT: revision conflict: expected 3, current 5").
			WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: domain, Metadata: md})
		require.NoError(t, err)
		return st.Err()
	}
	mapped := func(t *testing.T, err error, service string) *application.ControlError {
		t.Helper()
		var ce *application.ControlError
		require.True(t, errors.As(control.MapError(err, service), &ce))
		require.Equal(t, "REVISION_CONFLICT", ce.Code)
		require.False(t, ce.Retryable)
		return ce
	}
	both := map[string]string{"expectedRevision": "3", "currentRevision": "5"}

	ce := mapped(t, conflict(t, "REVISION_CONFLICT", "anvilkit.control.v1", both), "control")
	require.Equal(t, "3", ce.ExpectedRevision)
	require.Equal(t, "5", ce.CurrentRevision)
	require.Equal(t, "REVISION_CONFLICT: revision conflict: expected 3, current 5", ce.Message)

	ce = mapped(t, conflict(t, "REVISION_CONFLICT", "anvilkit.control.v1",
		map[string]string{"expectedRevision": "18446744073709551615", "currentRevision": "0"}), "control")
	require.Equal(t, "18446744073709551615", ce.ExpectedRevision, "the uint64 maximum is a revision")
	require.Equal(t, "0", ce.CurrentRevision)

	for name, md := range map[string]map[string]string{
		"a leading zero":       {"expectedRevision": "03", "currentRevision": "5"},
		"a sign":               {"expectedRevision": "+3", "currentRevision": "5"},
		"above the uint64 max": {"expectedRevision": "18446744073709551616", "currentRevision": "5"},
		"not a number":         {"expectedRevision": "three", "currentRevision": "5"},
		"one value missing":    {"expectedRevision": "3"},
	} {
		ce := mapped(t, conflict(t, "REVISION_CONFLICT", "anvilkit.control.v1", md), "control")
		require.Empty(t, ce.ExpectedRevision, name)
		require.Empty(t, ce.CurrentRevision, name)
	}
	for name, err := range map[string]error{
		"another domain":              conflict(t, "REVISION_CONFLICT", "anvilkit.mcp.v1", both),
		"another reason":              conflict(t, "STALE", "anvilkit.control.v1", both),
		"no detail":                   status.Error(codes.Aborted, "REVISION_CONFLICT: release changed concurrently"),
		"an owner's own precondition": status.Error(codes.FailedPrecondition, "DESCRIPTOR_MISMATCH: live tools"),
	} {
		ce := mapped(t, err, "control")
		require.Empty(t, ce.ExpectedRevision, name)
		require.Empty(t, ce.CurrentRevision, name)
	}
	ce = mapped(t, conflict(t, "REVISION_CONFLICT", "anvilkit.control.v1", both), "mcp")
	require.Empty(t, ce.ExpectedRevision, "only Control's conflicts carry its revisions")
}
