package http_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// RB-S14, RB-A23: what the strict body adapter and the router refuse before
// any handler runs, each with the public envelope and a fixed message.
func TestStrictRequestRefusals(t *testing.T) {
	ts, _ := newServer(t)
	token := "token-tenant-a-0123456789"
	command := func(revision string) string {
		return `{"commandId":"cmd_1","kind":"cancel","expectedRevision":"` + revision + `"}`
	}
	operation := func(commandID string) string {
		return `{"commandId":"` + commandID + `","kind":"local_check","subject":{"profileId":"local-check-v1","subjectDigest":"` + digest + `"}}`
	}
	send := func(t *testing.T, method, path, body string, header map[string]string) (int, map[string]any, http.Header) {
		t.Helper()
		req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
		require.NoError(t, err)
		req.Header.Set("Authorization", "Bearer "+token)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range header {
			req.Header.Set(k, v)
		}
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		var out map[string]any
		require.NoError(t, json.NewDecoder(resp.Body).Decode(&out), "every answer here is JSON")
		return resp.StatusCode, out, resp.Header
	}

	for _, tc := range []struct {
		name, method, path, body string
		header                   map[string]string
		status                   int
		code, message, field     string
	}{
		{"a gzip body (RB-S14)", "POST", "/api/v1/operations", operation("c1"), map[string]string{"Content-Encoding": "gzip"},
			415, "INVALID_ARGUMENT", "compressed request bodies are not accepted", ""},
		{"a coding listed after identity", "POST", "/api/v1/operations", operation("c1"), map[string]string{"Content-Encoding": "identity, br"},
			415, "INVALID_ARGUMENT", "compressed request bodies are not accepted", ""},
		{"invalid UTF-8", "POST", "/api/v1/operations", operation("c\xff1"), nil,
			400, "INVALID_ARGUMENT", "invalid JSON: the body is not valid Unicode", ""},
		{"a lone high surrogate", "POST", "/api/v1/operations", operation(`c\ud8001`), nil,
			400, "INVALID_ARGUMENT", "invalid JSON: the body is not valid Unicode", ""},
		{"a lone low surrogate", "POST", "/api/v1/operations", operation(`c\udc001`), nil,
			400, "INVALID_ARGUMENT", "invalid JSON: the body is not valid Unicode", ""},
		{"a high surrogate followed by another escape", "POST", "/api/v1/operations", operation(`c\ud800A1`), nil,
			400, "INVALID_ARGUMENT", "invalid JSON: the body is not valid Unicode", ""},
		{"a high surrogate at the end of a string", "POST", "/api/v1/operations", operation(`c\ud800`), nil,
			400, "INVALID_ARGUMENT", "invalid JSON: the body is not valid Unicode", ""},
		{"a revision above the uint64 bound", "POST", "/api/v1/operations/op_1/commands", command("18446744073709551616"), nil,
			400, "INVALID_ARGUMENT", "request body: a value violates format", "/expectedRevision"},
		{"a 20-digit revision", "POST", "/api/v1/operations/op_1/commands", command("99999999999999999999"), nil,
			400, "INVALID_ARGUMENT", "request body: a value violates format", "/expectedRevision"},
		{"an SSE cursor above the uint64 bound", "GET", "/api/v1/operations/op_1/events?after=18446744073709551616", "", nil,
			400, "INVALID_ARGUMENT", "after must be a canonical sequence", "after"},
		{"a path no route serves", "GET", "/api/v1/nothing-here", "", nil,
			404, "NOT_FOUND", "no route serves this path", ""},
		{"a path outside the API", "GET", "/nothing-here", "", nil,
			404, "NOT_FOUND", "no route serves this path", ""},
		{"a method the route does not serve", "DELETE", "/api/v1/operations", "", nil,
			405, "INVALID_ARGUMENT", "this route does not serve the method", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, out, hdr := send(t, tc.method, tc.path, tc.body, tc.header)
			require.Equal(t, tc.status, status, "%v", out)
			require.Equal(t, tc.code, errorCode(out), "%v", out)
			require.Equal(t, tc.message, errorMessage(out))
			require.Equal(t, tc.field, errorField(out))
			e, _ := out["error"].(map[string]any)
			require.Equal(t, false, e["retryable"])
			require.Empty(t, hdr.Get("Retry-After"))
			require.NotEmpty(t, e["requestId"])
			if tc.status == 405 {
				require.Contains(t, hdr.Get("Allow"), "POST", "the methods the route serves")
			}
		})
	}

	// What the rules admit: the uint64 maximum, an identity coding, a valid
	// surrogate pair and an escaped BMP character reach the handler or the
	// schema, never the Unicode or coding refusal.
	// (The fake Control knows no op_1: the handler answers NOT_FOUND.)
	status, out, _ := send(t, "POST", "/api/v1/operations/op_1/commands", command("18446744073709551615"), nil)
	require.Equal(t, 404, status, "the uint64 maximum is a revision, so the handler is reached: %v", out)
	require.Equal(t, "NOT_FOUND", errorCode(out))
	status, out, _ = send(t, "POST", "/api/v1/operations/op_1/commands", command("3"), map[string]string{"Content-Encoding": "identity"})
	require.Equal(t, 404, status, "identity is no compression: %v", out)
	require.Equal(t, "NOT_FOUND", errorCode(out))
	for _, id := range []string{`c😀1`, `cé1`} {
		status, out, _ = send(t, "POST", "/api/v1/operations", operation(id), nil)
		require.Equal(t, 400, status, "%v", out)
		require.Equal(t, "request body: a value violates pattern", errorMessage(out), "valid Unicode reaches the schema (the id pattern is ASCII)")
	}
}
