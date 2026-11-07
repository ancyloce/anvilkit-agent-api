package control_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

// TestMapError checks that MapError keeps every code on the public
// contract's list: an owner's own precondition code stays in the message
// of a conflict.
func TestMapError(t *testing.T) {
	for in, want := range map[error]string{
		status.Error(codes.PermissionDenied, "FORBIDDEN: not a grant manager"):    "FORBIDDEN",
		status.Error(codes.AlreadyExists, "COMMAND_CONFLICT: another request"):    "IDEMPOTENCY_CONFLICT",
		status.Error(codes.FailedPrecondition, "DESCRIPTOR_MISMATCH: live tools"): "REVISION_CONFLICT",
		status.Error(codes.FailedPrecondition, "STALE_EXECUTION: fenced"):         "STALE_EXECUTION",
		status.Error(codes.Aborted, "REVISION_CONFLICT: moved"):                   "REVISION_CONFLICT",
		status.Error(codes.NotFound, "NOT_FOUND"):                                 "NOT_FOUND",
		status.Error(codes.Unavailable, "down"):                                   "DEPENDENCY_UNAVAILABLE",
	} {
		var ce *application.ControlError
		require.True(t, errors.As(control.MapError(in, "mcp"), &ce))
		require.Equal(t, want, ce.Code, in.Error())
	}
}

// TestEffectUncertainIsNeverRetryable (RB-A24): the EFFECT_UNCERTAIN code is
// non-retryable under every gRPC code that may carry it, today's (UNAVAILABLE,
// FAILED_PRECONDITION) and any Control may switch to.
func TestEffectUncertainIsNeverRetryable(t *testing.T) {
	for _, c := range []codes.Code{codes.Unavailable, codes.FailedPrecondition, codes.Aborted, codes.Internal, codes.Unknown} {
		var ce *application.ControlError
		require.True(t, errors.As(control.MapError(status.Error(c, "EFFECT_UNCERTAIN: intake inventory write uncertain"), "control"), &ce))
		require.Equal(t, "EFFECT_UNCERTAIN", ce.Code, c.String())
		require.False(t, ce.Retryable, "%s carried EFFECT_UNCERTAIN as retryable", c)
	}
	// The control: an outage without the code stays a retryable dependency failure.
	var ce *application.ControlError
	require.True(t, errors.As(control.MapError(status.Error(codes.Unavailable, "down"), "control"), &ce))
	require.Equal(t, "DEPENDENCY_UNAVAILABLE", ce.Code)
	require.True(t, ce.Retryable)
}
