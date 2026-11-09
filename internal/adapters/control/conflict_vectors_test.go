package control_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

// TestRevisionConflictDetailFollowsTheContractVectors is the consumer side
// of the conflict-detail contract (RB-A23). testdata/status-details.fixtures.json
// is a verbatim copy of the contracts repository's
// proto/status-details.fixtures.json (the parent's tools/check-contracts.py
// compares the bytes), the file Control's producer test reads: for every
// case, a Control status carrying the vector's google.rpc.ErrorInfo maps to
// REVISION_CONFLICT with exactly the revisions the contract says are copied,
// or none.
//
// The two version-skew directions are part of it. An older Control sends the
// same status without the detail: the conflict keeps its code and message
// and carries no revisions. An older API never reads the detail: what it
// sees (the public code, the message, retryable) is what this API maps from
// the same status with the detail removed, for every vector.
func TestRevisionConflictDetailFollowsTheContractVectors(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "status-details.fixtures.json"))
	require.NoError(t, err)
	var vectors struct {
		ErrorInfo struct {
			Code, MessagePrefix, ExpectedKey, CurrentKey string
		} `json:"errorInfo"`
		Cases []struct {
			Name      string            `json:"name"`
			Detail    json.RawMessage   `json:"detail"`
			Revisions map[string]string `json:"revisions"`
		} `json:"cases"`
	}
	require.NoError(t, json.Unmarshal(raw, &vectors))
	info := vectors.ErrorInfo
	require.Equal(t, "ABORTED", info.Code, "the status below is codes.Aborted")
	require.NotEmpty(t, vectors.Cases)

	message := info.MessagePrefix + "revision conflict: expected 3, current 5"
	mapped := func(t *testing.T, err error) *application.ControlError {
		t.Helper()
		var ce *application.ControlError
		require.True(t, errors.As(control.MapError(err, "control"), &ce))
		return ce
	}
	// An older Control: the status without the detail.
	older := mapped(t, status.Error(codes.Aborted, message))
	require.Equal(t, "REVISION_CONFLICT", older.Code)
	require.Equal(t, message, older.Message)
	require.False(t, older.Retryable)
	require.Empty(t, older.ExpectedRevision, "an older Control sends no detail: no revisions")
	require.Empty(t, older.CurrentRevision)

	copied, refused := 0, 0
	for _, cs := range vectors.Cases {
		t.Run(cs.Name, func(t *testing.T) {
			var detail errdetails.ErrorInfo
			require.NoError(t, (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(cs.Detail, &detail))
			st, err := status.New(codes.Aborted, message).WithDetails(&detail)
			require.NoError(t, err)
			ce := mapped(t, st.Err())
			// What an API that predates the detail answers is unchanged.
			require.Equal(t, older.Code, ce.Code)
			require.Equal(t, older.Message, ce.Message)
			require.Equal(t, older.Retryable, ce.Retryable)
			if cs.Revisions == nil {
				refused++
				require.Empty(t, ce.ExpectedRevision, "the contract copies no revision")
				require.Empty(t, ce.CurrentRevision, "the contract copies no revision")
				return
			}
			copied++
			require.Len(t, cs.Revisions, 2)
			require.NotEmpty(t, cs.Revisions[info.ExpectedKey])
			require.NotEmpty(t, cs.Revisions[info.CurrentKey])
			require.Equal(t, cs.Revisions[info.ExpectedKey], ce.ExpectedRevision)
			require.Equal(t, cs.Revisions[info.CurrentKey], ce.CurrentRevision)
			// Only Control's conflicts carry its revisions.
			var other *application.ControlError
			require.True(t, errors.As(control.MapError(st.Err(), "mcp"), &other))
			require.Empty(t, other.ExpectedRevision)
			require.Empty(t, other.CurrentRevision)
		})
	}
	require.Positive(t, copied, "the contract names a copied case")
	require.Positive(t, refused, "and a refused one")
}
