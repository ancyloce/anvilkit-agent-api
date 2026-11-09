package http_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

// RB-A23: a revision conflict whose revisions Control reported carries them in
// details.expectedRevision and details.currentRevision; one without them
// carries no details; another code never shows revisions.
func TestRevisionConflictEnvelopeNamesBothRevisions(t *testing.T) {
	ts, fake := newServer(t)
	fake.failOn = map[string]error{
		"op_conflict": &application.ControlError{Code: "REVISION_CONFLICT", Message: "REVISION_CONFLICT: revision conflict: expected 3, current 5",
			ExpectedRevision: "3", CurrentRevision: "5"},
		"op_concurrent": &application.ControlError{Code: "REVISION_CONFLICT", Message: "REVISION_CONFLICT: release changed concurrently"},
		"op_stale": &application.ControlError{Code: "STALE_EXECUTION", Message: "STALE_EXECUTION: fenced",
			ExpectedRevision: "3", CurrentRevision: "5"},
	}
	token := "token-tenant-a-0123456789"
	details := func(out map[string]any) map[string]any {
		d, _ := out["error"].(map[string]any)["details"].(map[string]any)
		return d
	}

	status, out, hdr := do(t, ts, "GET", "/api/v1/operations/op_conflict", token, "")
	require.Equal(t, 409, status, "%v", out)
	require.Equal(t, "REVISION_CONFLICT", errorCode(out))
	require.Equal(t, map[string]any{"expectedRevision": "3", "currentRevision": "5"}, details(out), "decimal strings, as every revision")
	require.Equal(t, false, out["error"].(map[string]any)["retryable"])
	require.Empty(t, hdr.Get("Retry-After"))

	status, out, _ = do(t, ts, "GET", "/api/v1/operations/op_concurrent", token, "")
	require.Equal(t, 409, status, "%v", out)
	require.Nil(t, details(out), "a conflict without both revisions has no details")

	status, out, _ = do(t, ts, "GET", "/api/v1/operations/op_stale", token, "")
	require.Equal(t, 409, status, "%v", out)
	require.Equal(t, "STALE_EXECUTION", errorCode(out))
	require.Nil(t, details(out), "only a revision conflict names revisions")
}
