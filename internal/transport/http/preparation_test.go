package http_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

// API-01 and API-06 over the strict transport: the prompt binding and the
// references reach Control as the intake, the waiting projection carries
// the open question set, an answer is bound to the question set revision
// and a stale revision is a public REVISION_CONFLICT.
func TestPreparationEndpoints(t *testing.T) {
	ts, control := newServer(t)
	tokenA := "token-tenant-a-0123456789"
	digest := "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"

	status, out, _ := do(t, ts, "POST", "/api/v1/preparations", tokenA, `{"commandId":"p1","prompt":{"transferId":"xfer_prompt","digest":"`+digest+`"},"brandReferences":[{"sourceId":"brand_1","revision":"3"}]}`)
	require.Equal(t, http.StatusAccepted, status, out)
	require.Equal(t, "preparation", out["kind"])
	require.Equal(t, "waiting", out["lifecycle"])
	clarification, ok := out["clarification"].(map[string]any)
	require.True(t, ok, "a waiting preparation exposes its open question set")
	require.Equal(t, "qs_p1", clarification["questionSetId"])
	require.Equal(t, "1", clarification["questionSetRevision"])
	require.NotEmpty(t, clarification["expiresAt"])
	subject := out["subject"].(map[string]any)
	require.Equal(t, "preparation-v1", subject["profileId"], "the reviewed preparation profile of this deployment")
	operationID := out["operationId"].(string)

	status, again, _ := do(t, ts, "POST", "/api/v1/preparations", tokenA, `{"commandId":"p1","prompt":{"transferId":"xfer_prompt","digest":"`+digest+`"},"brandReferences":[{"sourceId":"brand_1","revision":"3"}]}`)
	require.Equal(t, http.StatusAccepted, status)
	require.Equal(t, operationID, again["operationId"], "the same command returns the original operation")

	status, out, _ = do(t, ts, "POST", "/api/v1/preparations", tokenA, `{"commandId":"p2","prompt":{"transferId":"xfer_prompt","digest":"not-a-digest"}}`)
	require.Equal(t, http.StatusBadRequest, status)
	status, out, _ = do(t, ts, "POST", "/api/v1/preparations", tokenA, `{"commandId":"p3","prompt":{"transferId":"xfer_prompt","digest":"`+digest+`"},"tenantId":"tenant_b"}`)
	require.Equal(t, http.StatusBadRequest, status, "an unknown member is refused")

	// DEC-063: a preparation of a change names its base, which reaches
	// Control in the subject and is part of the intake's digest.
	prepare := func(commandID, base string) (int, map[string]any) {
		status, out, _ := do(t, ts, "POST", "/api/v1/preparations", tokenA, `{"commandId":"`+commandID+`","prompt":{"transferId":"xfer_prompt","digest":"`+digest+`"}`+base+`}`)
		return status, out
	}
	status, plain := prepare("p4", "")
	require.Equal(t, http.StatusAccepted, status, plain)
	status, based := prepare("p5", `,"refinementBase":{"sourceOperationId":"op_base","sourceRevision":"4"}`)
	require.Equal(t, http.StatusAccepted, status, based)
	control.mu.Lock()
	sent := append([]application.OperationSubject(nil), control.subjects[len(control.subjects)-2:]...)
	control.mu.Unlock()
	require.Empty(t, sent[0].SourceOperationID, "a preparation of a new component names no base")
	require.Empty(t, sent[0].SourceRevision)
	require.Equal(t, "op_base", sent[1].SourceOperationID)
	require.Equal(t, "4", sent[1].SourceRevision)
	require.NotEqual(t, sent[0].SubjectDigest, sent[1].SubjectDigest, "the base is part of the intake")
	for name, base := range map[string]string{
		"no revision":       `,"refinementBase":{"sourceOperationId":"op_base"}`,
		"no operation":      `,"refinementBase":{"sourceRevision":"4"}`,
		"a chosen artifact": `,"refinementBase":{"sourceOperationId":"op_base","sourceRevision":"4","sourceHandle":"hdl_1"}`,
		"not a revision":    `,"refinementBase":{"sourceOperationId":"op_base","sourceRevision":"latest"}`,
	} {
		status, _ := prepare("p6", base)
		require.Equal(t, http.StatusBadRequest, status, name)
	}

	status, out, _ = do(t, ts, "POST", "/api/v1/preparations/"+operationID+"/answers", tokenA, `{"commandId":"a1","questionSetId":"qs_p1","questionSetRevision":"2","answer":{"transferId":"xfer_ans","digest":"`+digest+`"}}`)
	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "REVISION_CONFLICT", out["error"].(map[string]any)["code"])
	status, out, _ = do(t, ts, "POST", "/api/v1/preparations/"+operationID+"/answers", tokenA, `{"commandId":"a1","questionSetId":"qs_p1","questionSetRevision":"1","answer":{"transferId":"xfer_ans","digest":"`+digest+`"}}`)
	require.Equal(t, http.StatusAccepted, status, out)
	require.Equal(t, "ans_a1", out["answerId"])
	require.Equal(t, "ans_a1", out["updateId"], "one tracked Update follows under the answer identity")
	require.Equal(t, operationID, out["operationId"])
	status, out, _ = do(t, ts, "POST", "/api/v1/preparations/op_missing/answers", tokenA, `{"commandId":"a2","questionSetId":"qs_p1","questionSetRevision":"1","answer":{"transferId":"xfer_ans","digest":"`+digest+`"}}`)
	require.Equal(t, http.StatusNotFound, status)
}
