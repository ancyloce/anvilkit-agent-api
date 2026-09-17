package http_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

// API-01 and API-06 over the strict transport: the prompt binding and the
// references reach Control as the intake, the waiting projection carries
// the open question set, an answer is bound to the question set revision
// and a stale revision is a public REVISION_CONFLICT.
func TestPreparationEndpoints(t *testing.T) {
	ts, _ := newServer(t)
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
