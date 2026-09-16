package http_test

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// API-12 over the transport: the authenticated caller begins a transfer
// and receives the scoped capability (URL, method, signed headers) once;
// the strict validator refuses classes outside the eight, numeric sizes
// and unknown members; the same command returns the original transfer; a
// finalize names the handle and the uploaded object version, another
// tenant's handle is not found, a verified mismatch is a rejection and a
// repeated finalize returns the original outcome. Business verification
// itself is proven in Control.
func TestArtifactTransfers(t *testing.T) {
	ts, _ := newServer(t)
	tokenA, tokenB := "token-tenant-a-0123456789", "token-tenant-b-0123456789"
	begin := `{"commandId":"x1","class":"prompt","expectedDigest":"` + digest + `","expectedSize":"4096","mediaType":"text/plain"}`

	status, out, _ := do(t, ts, "POST", "/api/v1/artifacts/transfers", "", begin)
	require.Equal(t, 401, status)

	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/transfers", tokenA, `{"commandId":"x1","class":"npm","expectedDigest":"`+digest+`","expectedSize":"4096","mediaType":"text/plain"}`)
	require.Equal(t, 400, status, "a class outside the eight is refused: %v", out)
	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/transfers", tokenA, `{"commandId":"x1","class":"prompt","expectedDigest":"`+digest+`","expectedSize":4096,"mediaType":"text/plain"}`)
	require.Equal(t, 400, status, "a numeric size is refused: %v", out)
	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/transfers", tokenA, `{"commandId":"x1","class":"prompt","expectedDigest":"`+digest+`","expectedSize":"4096","mediaType":"text/plain","objectKey":"leak"}`)
	require.Equal(t, 400, status, "an unknown member is refused: %v", out)

	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/transfers", tokenA, begin)
	require.Equal(t, 201, status, "%v", out)
	require.Equal(t, "begun", out["state"])
	require.Equal(t, "prompt", out["class"])
	handle, _ := out["handle"].(string)
	require.NotEmpty(t, handle)
	require.NotEmpty(t, out["uploadCapability"], "the authenticated caller is a trusted flow and receives the capability")
	require.Equal(t, "PUT", out["uploadMethod"])
	headers, _ := out["uploadHeaders"].(map[string]any)
	require.Equal(t, "text/plain", headers["Content-Type"])
	require.Equal(t, "4096", headers["Content-Length"])
	require.NotEmpty(t, out["uploadExpiresAt"])
	assertFrameSchema(t, "Transfer", mustJSON(t, out))

	status, again, _ := do(t, ts, "POST", "/api/v1/artifacts/transfers", tokenA, begin)
	require.Equal(t, 201, status)
	require.Equal(t, out["transferId"], again["transferId"], "the same command returns the original transfer")
	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/transfers", tokenA, `{"commandId":"x1","class":"prompt","expectedDigest":"`+digest+`","expectedSize":"4097","mediaType":"text/plain"}`)
	require.Equal(t, 409, status)
	require.Equal(t, "IDEMPOTENCY_CONFLICT", errorCode(out))

	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/"+handle+"/finalizations", tokenB, `{"commandId":"x1:fin","objectVersion":"v1"}`)
	require.Equal(t, 404, status, "another tenant's handle is not found: %v", out)
	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/"+handle+"/finalizations", tokenA, `{"commandId":"x1:fin"}`)
	require.Equal(t, 400, status, "the object version is required: %v", out)
	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/"+handle+"/finalizations", tokenA, `{"commandId":"x1:fin","objectVersion":"v1"}`)
	require.Equal(t, 200, status, "%v", out)
	require.Equal(t, "finalized", out["state"])
	require.Equal(t, "v1", out["objectVersion"])
	require.Nil(t, out["uploadCapability"], "a finalized transfer carries no capability")
	assertFrameSchema(t, "Transfer", mustJSON(t, out))
	status, same, _ := do(t, ts, "POST", "/api/v1/artifacts/"+handle+"/finalizations", tokenA, `{"commandId":"x1:fin","objectVersion":"v1"}`)
	require.Equal(t, 200, status)
	require.Equal(t, out["transferId"], same["transferId"], "a repeated finalize returns the original outcome")
	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/"+handle+"/finalizations", tokenA, `{"commandId":"x1:fin2","objectVersion":"v2"}`)
	require.Equal(t, 409, status, "another version for a finalized transfer conflicts: %v", out)

	// A verified mismatch (the fake plays Control's size check) is a
	// rejection, not a finalized transfer.
	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/transfers", tokenA, `{"commandId":"x2","class":"source","expectedDigest":"`+digest+`","expectedSize":"4096","mediaType":"application/zip"}`)
	require.Equal(t, 201, status)
	partial, _ := out["handle"].(string)
	status, out, _ = do(t, ts, "POST", "/api/v1/artifacts/"+partial+"/finalizations", tokenA, `{"commandId":"x2:fin","objectVersion":"partial"}`)
	require.Equal(t, 400, status, "%v", out)
	require.Equal(t, "INVALID_ARGUMENT", errorCode(out))
}
