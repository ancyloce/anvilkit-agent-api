package http_test

import (
	"bufio"
	"context"
	"encoding/json"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-contrib/sse"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	httptransport "github.com/ancyloce/anvilkit-agent-api/internal/transport/http"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

// testOptions are the injected transport bounds of the in-process tests;
// individual SSE tests lower them explicitly.
func testOptions() httptransport.Options {
	return httptransport.Options{
		Listen: "127.0.0.1:0", ReadHeaderTimeout: 10 * time.Second, BodyLimitBytes: 256 << 10,
		Stream: httptransport.StreamBounds{HeartbeatInterval: 15 * time.Second, FrameBuffer: 64, SlowConsumerGrace: 5 * time.Second, WriteTimeout: 10 * time.Second},
	}
}

// assertFrameSchema is the provider-side contract check of API-04: the SSE
// `data` documents the API emits validate against the public schemas.
func assertFrameSchema(t *testing.T, schema, data string) {
	t.Helper()
	spec, err := agentapi.GetSwagger()
	require.NoError(t, err)
	ref, ok := spec.Components.Schemas[schema]
	require.True(t, ok, "schema %s is embedded", schema)
	var inst any
	require.NoError(t, json.Unmarshal([]byte(data), &inst))
	require.NoError(t, ref.Value.VisitJSON(inst), "%s: %s", schema, data)
}

type verifier struct{}

func (verifier) Verify(_ context.Context, bearer string) (application.Principal, error) {
	switch bearer {
	case "token-tenant-a-0123456789":
		return application.Principal{TenantID: "tenant_a", ActorID: "user_a"}, nil
	case "token-tenant-b-0123456789":
		return application.Principal{TenantID: "tenant_b", ActorID: "user_b"}, nil
	}
	return application.Principal{}, application.ErrUnauthenticated
}

// fakeControl records what the API sends and answers like Control would for
// the transport concerns under test; business rules are proven in Control.
type fakeControl struct {
	mu        sync.Mutex
	commands  map[string]application.CommandIdentity
	ops       map[string]application.OperationView
	events    map[string][]application.EventFrame
	transfers map[string]application.TransferView
	handles   map[string]string
	panicOn   string // operation id whose GetOperation panics (recovery test)
}

func newFake() *fakeControl {
	return &fakeControl{commands: map[string]application.CommandIdentity{}, ops: map[string]application.OperationView{}, events: map[string][]application.EventFrame{}, transfers: map[string]application.TransferView{}, handles: map[string]string{}}
}

func (f *fakeControl) CreateOperation(_ context.Context, cmd application.CommandIdentity, p application.Principal, kind, profileID, subjectDigest, briefID, sourceRevision string) (application.OperationView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := cmd.TenantID + "/" + cmd.CommandID
	if prev, ok := f.commands[key]; ok {
		if prev.RequestDigest != cmd.RequestDigest {
			return application.OperationView{}, &application.ControlError{Code: "IDEMPOTENCY_CONFLICT", Message: "changed digest"}
		}
		return f.ops[key], nil
	}
	f.commands[key] = cmd
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	view := application.OperationView{
		OperationID: "op_" + cmd.CommandID, TenantID: p.TenantID, ActorID: p.ActorID, Kind: kind, ProfileID: profileID, SubjectDigest: subjectDigest,
		Lifecycle: "accepted", Phase: "accepted", Control: "none", Cleanup: "not_required", Finance: "not_funded", Revision: "2", CoveredEventSeq: "2",
		ExecutionEpoch: "1", CreatedAt: now, UpdatedAt: now, Deadline: now.Add(15 * time.Minute),
	}
	f.ops[key] = view
	f.events[view.OperationID] = []application.EventFrame{
		{OperationID: view.OperationID, EventSeq: "1", TransitionID: "accepted", EventType: "operation.changed", Revision: "1", OccurredAt: now, Lifecycle: "accepted", Phase: "intake", Control: "none", Cleanup: "not_required", Finance: "not_funded"},
		{OperationID: view.OperationID, EventSeq: "2", TransitionID: "intake_confirmed", EventType: "operation.changed", Revision: "2", OccurredAt: now, Lifecycle: "accepted", Phase: "accepted", Control: "none", Cleanup: "not_required", Finance: "not_funded"},
	}
	return view, nil
}

// CreatePreparation records the intake as a preparation operation whose
// projection carries an open question set, so the public clarification
// projection and API-06 are exercised over the fake.
func (f *fakeControl) CreatePreparation(ctx context.Context, cmd application.CommandIdentity, p application.Principal, profileID, subjectDigest string, intake application.PreparationIntake) (application.OperationView, error) {
	view, err := f.CreateOperation(ctx, cmd, p, "preparation", profileID, subjectDigest, "", "")
	if err != nil {
		return view, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if intake.PromptTransferID == "" {
		return application.OperationView{}, &application.ControlError{Code: "INVALID_ARGUMENT", Message: "prompt required"}
	}
	key := cmd.TenantID + "/" + cmd.CommandID
	view.Lifecycle, view.Phase = "waiting", "awaiting_input"
	view.Clarification = &application.Clarification{QuestionSetID: "qs_" + cmd.CommandID, QuestionSetRevision: "1", Round: "1", AskedAt: view.CreatedAt, ExpiresAt: view.CreatedAt.Add(7 * 24 * time.Hour), Questions: []application.Question{{QuestionID: "q1", Text: "Which call to action?"}}}
	f.ops[key] = view
	return view, nil
}

func (f *fakeControl) SubmitAnswer(_ context.Context, cmd application.CommandIdentity, p application.Principal, operationID, questionSetID, questionSetRevision, transferID, digest string) (application.AnswerReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, v := range f.ops {
		if v.OperationID == operationID && v.TenantID == p.TenantID {
			if v.Clarification == nil || v.Clarification.QuestionSetID != questionSetID {
				return application.AnswerReceipt{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
			}
			if v.Clarification.QuestionSetRevision != questionSetRevision {
				return application.AnswerReceipt{}, &application.ControlError{Code: "REVISION_CONFLICT", Message: "stale question set revision"}
			}
			return application.AnswerReceipt{AnswerID: "ans_" + cmd.CommandID, OperationID: operationID, QuestionSetID: questionSetID, QuestionSetRevision: questionSetRevision, UpdateID: "ans_" + cmd.CommandID, AcceptedAt: v.CreatedAt}, nil
		}
	}
	return application.AnswerReceipt{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
}

func (f *fakeControl) GetOperation(_ context.Context, p application.Principal, id string) (application.OperationView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.panicOn != "" && id == f.panicOn {
		panic("control adapter panicked")
	}
	for _, v := range f.ops {
		if v.OperationID == id && v.TenantID == p.TenantID {
			return v, nil
		}
	}
	return application.OperationView{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
}

func (f *fakeControl) SubmitCommand(_ context.Context, cmd application.CommandIdentity, p application.Principal, id, kind, expected, _ string) (application.CommandReceipt, error) {
	view, err := f.GetOperation(context.Background(), p, id)
	if err != nil {
		return application.CommandReceipt{}, err
	}
	if expected != view.Revision {
		return application.CommandReceipt{}, &application.ControlError{Code: "REVISION_CONFLICT", Message: "expected " + expected + ", current " + view.Revision}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Date(2026, 9, 14, 12, 0, 1, 0, time.UTC)
	f.events[id] = append(f.events[id], application.EventFrame{OperationID: id, EventSeq: "3", TransitionID: "cmd:" + cmd.CommandID, EventType: "operation.changed", Revision: "3", OccurredAt: now, Lifecycle: "canceled", Phase: "canceled", Control: "cancel_applied", Cleanup: "not_required", Finance: "not_funded"})
	return application.CommandReceipt{CommandID: cmd.CommandID, OperationID: id, Kind: kind, Outcome: "applied", OperationRevision: "3", AcceptedAt: now, SettledAt: &now}, nil
}

func (f *fakeControl) GetCommand(context.Context, application.Principal, string, string) (application.CommandReceipt, error) {
	return application.CommandReceipt{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
}

// BeginTransfer answers like Control: the same command returns the original
// transfer, a changed digest conflicts, and the capability is issued for a
// begun transfer only.
func (f *fakeControl) BeginTransfer(_ context.Context, cmd application.CommandIdentity, p application.Principal, intent application.TransferIntent) (application.TransferView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := cmd.TenantID + "/xfer/" + cmd.CommandID
	if prev, ok := f.commands[key]; ok {
		if prev.RequestDigest != cmd.RequestDigest {
			return application.TransferView{}, &application.ControlError{Code: "IDEMPOTENCY_CONFLICT", Message: "changed digest"}
		}
		return f.transfers[key], nil
	}
	if intent.Deadline.Before(time.Now().Add(time.Minute)) {
		return application.TransferView{}, &application.ControlError{Code: "INVALID_ARGUMENT", Message: "deadline too short"}
	}
	f.commands[key] = cmd
	view := application.TransferView{
		TransferID: "xfer_" + cmd.CommandID, Handle: "hdl_" + cmd.CommandID, Class: intent.Class, ExpectedDigest: intent.ExpectedDigest, ExpectedSize: intent.ExpectedSize,
		State: "begun", Deadline: intent.Deadline.UTC().Truncate(time.Second),
		Upload: &application.UploadCapability{URL: "https://artifacts.example.invalid/" + p.TenantID + "/xfer_" + cmd.CommandID + "?X-Amz-Signature=abc", Method: "PUT", Headers: map[string]string{"Content-Type": intent.MediaType, "Content-Length": intent.ExpectedSize}, ExpiresAt: intent.Deadline.UTC().Truncate(time.Second)},
	}
	if f.transfers == nil {
		f.transfers = map[string]application.TransferView{}
	}
	f.transfers[key] = view
	f.handles[view.Handle] = key
	return view, nil
}

func (f *fakeControl) FinalizeTransfer(_ context.Context, cmd application.CommandIdentity, p application.Principal, handle, objectVersion string) (application.TransferView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key, ok := f.handles[handle]
	if !ok || !strings.HasPrefix(key, p.TenantID+"/") {
		return application.TransferView{}, &application.ControlError{Code: "NOT_FOUND", Message: "not found"}
	}
	view := f.transfers[key]
	if view.State == "finalized" {
		if view.ObjectVersion != objectVersion {
			return application.TransferView{}, &application.ControlError{Code: "IDEMPOTENCY_CONFLICT", Message: "finalized at another version"}
		}
		return view, nil
	}
	if objectVersion == "partial" {
		view.State, view.ReasonCode, view.Upload = "rejected", "SIZE_MISMATCH", nil
		f.transfers[key] = view
		return application.TransferView{}, &application.ControlError{Code: "INVALID_ARGUMENT", Message: "object holds 12 bytes, the transfer declared 4096"}
	}
	view.State, view.ObjectVersion, view.Upload = "finalized", objectVersion, nil
	f.transfers[key] = view
	return view, nil
}

// StreamEvents honors the cursor: only events after it are emitted, and a
// cursor beyond the covered sequence is a reset.
func (f *fakeControl) StreamEvents(ctx context.Context, p application.Principal, id, after string, emit func(application.EventFrame) error) error {
	if _, err := f.GetOperation(ctx, p, id); err != nil {
		return err
	}
	cursor := 0
	if after != "" {
		cursor, _ = strconv.Atoi(after)
	}
	f.mu.Lock()
	covered := len(f.events[id])
	f.mu.Unlock()
	if cursor > covered {
		return &application.ResetRequired{CoveredEventSeq: strconv.Itoa(covered)}
	}
	sent := cursor
	for {
		f.mu.Lock()
		evs := append([]application.EventFrame(nil), f.events[id]...)
		f.mu.Unlock()
		for i := sent; i < len(evs); i++ {
			if err := emit(evs[i]); err != nil {
				return err
			}
			sent = i + 1
			if evs[i].Lifecycle == "canceled" {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func newServer(t *testing.T) (*httptest.Server, *fakeControl) {
	t.Helper()
	fake := newFake()
	srv, err := httptransport.NewServer(testOptions(), verifier{}, fake, func(context.Context) error { return nil })
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, fake
}

func do(t *testing.T, ts *httptest.Server, method, path, token, body string) (int, map[string]any, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	require.NoError(t, err)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out, resp.Header
}

const digest = "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3"

func createBody(commandID string) string {
	return `{"commandId":"` + commandID + `","kind":"local_check","subject":{"profileId":"local-check-v1","subjectDigest":"` + digest + `"}}`
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	require.NoError(t, err)
	return string(raw)
}

func errorCode(out map[string]any) string {
	e, _ := out["error"].(map[string]any)
	c, _ := e["code"].(string)
	return c
}

func errorMessage(out map[string]any) string {
	e, _ := out["error"].(map[string]any)
	m, _ := e["message"].(string)
	return m
}

// readFrames reads SSE frames from a live stream with the field rule of the
// protocol (and of sse.Decode): the value after the first colon, minus one
// optional leading space. Comment lines are counted separately.
type liveFrame struct {
	ID, Event, Data string
}

func readFrames(reader *bufio.Reader, stop func(liveFrame) bool) (frames []liveFrame, comments int, err error) {
	cur := liveFrame{}
	for {
		line, rerr := reader.ReadString('\n')
		if rerr != nil {
			return frames, comments, rerr
		}
		line = strings.TrimRight(line, "\n")
		if line == "" {
			if cur.Event != "" || cur.Data != "" {
				frames = append(frames, cur)
				if stop(cur) {
					return frames, comments, nil
				}
				cur = liveFrame{}
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			comments++
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			cur.ID = value
		case "event":
			cur.Event = value
		case "data":
			cur.Data = value
		}
	}
}

func sseRequest(t *testing.T, ts *httptest.Server, path, token string, headers map[string]string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("GET", ts.URL+path, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+token)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	return resp
}

func TestAPI(t *testing.T) {
	ts, fake := newServer(t)
	tokenA, tokenB := "token-tenant-a-0123456789", "token-tenant-b-0123456789"

	status, out, hdr := do(t, ts, "POST", "/api/v1/operations", "", createBody("c1"))
	require.Equal(t, 401, status)
	require.Equal(t, "UNAUTHENTICATED", errorCode(out))
	require.Equal(t, hdr.Get("X-Request-Id"), out["error"].(map[string]any)["requestId"], "the envelope carries the request id")

	status, out, _ = do(t, ts, "POST", "/api/v1/operations", tokenA, `{"commandId":"c1","kind":"local_check","subject":{"profileId":"local-check-v1","subjectDigest":"`+digest+`"},"tenantId":"tenant_b"}`)
	require.Equal(t, 400, status, "unknown member rejected by the OpenAPI validator: %v", out)
	require.Equal(t, "INVALID_ARGUMENT", errorCode(out))
	require.Contains(t, errorMessage(out), "tenantId", "the validator names the unknown member: %v", out)

	status, out, _ = do(t, ts, "POST", "/api/v1/operations", tokenA, `{"commandId":"c1","commandId":"c2","kind":"local_check","subject":{"profileId":"local-check-v1","subjectDigest":"`+digest+`"}}`)
	require.Equal(t, 400, status, "duplicate member rejected by the strict parser")
	require.Contains(t, errorMessage(out), "duplicate member")

	status, out, _ = do(t, ts, "POST", "/api/v1/operations", tokenA, createBody("c1")+" {}")
	require.Equal(t, 400, status, "trailing data rejected")
	require.Contains(t, errorMessage(out), "trailing data")

	status, out, _ = do(t, ts, "POST", "/api/v1/operations", tokenA, `{"commandId":"c1","kind":"local_check","subject":{"profileId":"local-check-v1","subjectDigest":"`+digest+`"},"n":1e999}`)
	require.Equal(t, 400, status, "non-finite number rejected: %v", out)

	status, out, _ = do(t, ts, "POST", "/api/v1/operations", tokenA, `{"commandId":"c1","kind":"preparation","subject":{"profileId":"local-check-v1","subjectDigest":"`+digest+`"}}`)
	require.Equal(t, 400, status, "enum outside the endpoint's kinds rejected")
	require.Contains(t, errorMessage(out), "kind", "%v", out)

	status, out, _ = do(t, ts, "POST", "/api/v1/operations", tokenA, `{"commandId":"c1","kind":"local_check","subject":{"profileId":"local-check-v1","subjectDigest":"`+digest+`","briefId":null}}`)
	require.Equal(t, 400, status, "null is not absence")
	require.Contains(t, errorMessage(out), "briefId", "%v", out)

	status, out, hdr = do(t, ts, "POST", "/api/v1/operations", tokenA, createBody("c1"))
	require.Equal(t, 202, status, "%v", out)
	require.Equal(t, "op_c1", out["operationId"])
	require.Equal(t, "tenant_a", out["tenantId"], "scope comes from the principal")
	require.NotEmpty(t, hdr.Get("X-Request-Id"))
	require.NotContains(t, out, "projectId", "absent optional members are omitted")
	sent := fake.commands["tenant_a/c1"]
	require.Regexp(t, `^sha256:[0-9a-f]{64}$`, sent.RequestDigest)

	status, out, _ = do(t, ts, "POST", "/api/v1/operations", tokenA, "{ \"kind\":\"local_check\", \"commandId\":\"c1\",\n \"subject\":{\"subjectDigest\":\""+digest+"\",\"profileId\":\"local-check-v1\"}}")
	require.Equal(t, 202, status, "reformatted retry has the same canonical digest")
	require.Equal(t, "op_c1", out["operationId"])

	status, out, _ = do(t, ts, "POST", "/api/v1/operations", tokenA, `{"commandId":"c1","kind":"local_check","subject":{"profileId":"local-check-v1","subjectDigest":"sha256:1111111111111111111111111111111111111111111111111111111111111111"}}`)
	require.Equal(t, 409, status)
	require.Equal(t, "IDEMPOTENCY_CONFLICT", errorCode(out))

	status, out, _ = do(t, ts, "GET", "/api/v1/operations/op_c1", tokenB, "")
	require.Equal(t, 404, status, "cross-tenant read is not found")
	require.Equal(t, "NOT_FOUND", errorCode(out))

	status, out, _ = do(t, ts, "POST", "/api/v1/operations/op_c1/commands", tokenA, `{"commandId":"k1","kind":"cancel","expectedRevision":"1"}`)
	require.Equal(t, 409, status)
	require.Equal(t, "REVISION_CONFLICT", errorCode(out))

	status, out, _ = do(t, ts, "POST", "/api/v1/operations/op_c1/commands", tokenA, `{"commandId":"k1","kind":"cancel","expectedRevision":"02"}`)
	require.Equal(t, 400, status, "leading-zero revision rejected by the schema pattern")
	require.Contains(t, errorMessage(out), "expectedRevision", "%v", out)

	status, out, _ = do(t, ts, "GET", "/api/v1/knowledge/sources/src_1", tokenA, "")
	require.Equal(t, 503, status)
	require.Equal(t, "DEPENDENCY_UNAVAILABLE", errorCode(out))

	// SSE: durable frames, then a cancel produces the terminal frame and the stream ends.
	resp := sseRequest(t, ts, "/api/v1/operations/op_c1/events", tokenA, nil)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	require.NoError(t, err)
	require.Equal(t, "text/event-stream", mediaType)
	require.Equal(t, "utf-8", params["charset"], "content type comes from gin-contrib/sse")
	require.Equal(t, "no-store", resp.Header.Get("Cache-Control"))
	reader := bufio.NewReader(resp.Body)
	frames := make(chan liveFrame, 8)
	go func() {
		defer close(frames)
		_, _, _ = readFrames(reader, func(f liveFrame) bool { frames <- f; return false })
	}()
	first := <-frames
	require.Equal(t, "1", first.ID)
	require.Equal(t, "operation.changed", first.Event)
	assertFrameSchema(t, "OperationEventFrame", first.Data)
	var data map[string]any
	require.NoError(t, json.Unmarshal([]byte(first.Data), &data))
	require.Equal(t, "intake", data["payload"].(map[string]any)["phase"])
	second := <-frames
	require.Equal(t, "2", second.ID)
	assertFrameSchema(t, "OperationEventFrame", second.Data)

	status, out, _ = do(t, ts, "POST", "/api/v1/operations/op_c1/commands", tokenA, `{"commandId":"k2","kind":"cancel","expectedRevision":"2"}`)
	require.Equal(t, 202, status, "%v", out)
	require.Equal(t, "applied", out["outcome"])
	third := <-frames
	require.Equal(t, "3", third.ID)
	assertFrameSchema(t, "OperationEventFrame", third.Data)
	require.NoError(t, json.Unmarshal([]byte(third.Data), &data))
	require.Equal(t, "canceled", data["payload"].(map[string]any)["lifecycle"])
	_, more := <-frames
	require.False(t, more, "the stream ends after the terminal frame")

	// Reconnection: Last-Event-ID replays only the events after the cursor;
	// the finite stream is decoded by the library's own decoder.
	resp = sseRequest(t, ts, "/api/v1/operations/op_c1/events", tokenA, map[string]string{"Last-Event-ID": "2"})
	events, err := sse.Decode(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "3", events[0].Id)
	require.Equal(t, "operation.changed", events[0].Event)
	assertFrameSchema(t, "OperationEventFrame", events[0].Data.(string))

	// `after` wins over the header.
	resp = sseRequest(t, ts, "/api/v1/operations/op_c1/events?after=1", tokenA, map[string]string{"Last-Event-ID": "2"})
	events, err = sse.Decode(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Len(t, events, 2)
	require.Equal(t, "2", events[0].Id)

	// A cursor beyond retention yields a reset frame.
	resp = sseRequest(t, ts, "/api/v1/operations/op_c1/events", tokenA, map[string]string{"Last-Event-ID": "999"})
	events, err = sse.Decode(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "reset_required", events[0].Event)
	require.Empty(t, events[0].Id, "a reset frame carries no cursor")
	require.Contains(t, events[0].Data.(string), `"coveredEventSeq":"3"`)
	assertFrameSchema(t, "ResetFrame", events[0].Data.(string))

	// Cursor binding errors are ordinary envelopes.
	status, out, _ = do(t, ts, "GET", "/api/v1/operations/op_c1/events?after=x", tokenA, "")
	require.Equal(t, 400, status)
	require.Equal(t, "INVALID_ARGUMENT", errorCode(out))
	require.Contains(t, errorMessage(out), "after")
	req, _ := http.NewRequest("GET", ts.URL+"/api/v1/operations/op_c1/events", nil)
	req.Header.Set("Authorization", "Bearer "+tokenA)
	req.Header.Set("Last-Event-ID", "007")
	r, err := ts.Client().Do(req)
	require.NoError(t, err)
	var env map[string]any
	_ = json.NewDecoder(r.Body).Decode(&env)
	r.Body.Close()
	require.Equal(t, 400, r.StatusCode)
	require.Contains(t, errorMessage(env), "Last-Event-ID")
	status, out, _ = do(t, ts, "GET", "/api/v1/operations/op_c1/events", tokenB, "")
	require.Equal(t, 404, status, "the stream is authorized before the first frame")
}

// A panic inside a handler or adapter becomes the public envelope through
// Gin's recovery and the single error renderer.
func TestPanicRendersTheEnvelope(t *testing.T) {
	ts, fake := newServer(t)
	_, _, _ = do(t, ts, "POST", "/api/v1/operations", "token-tenant-a-0123456789", createBody("p1"))
	fake.panicOn = "op_p1"
	status, out, hdr := do(t, ts, "GET", "/api/v1/operations/op_p1", "token-tenant-a-0123456789", "")
	require.Equal(t, 503, status)
	require.Equal(t, "DEPENDENCY_UNAVAILABLE", errorCode(out))
	require.Equal(t, true, out["error"].(map[string]any)["retryable"])
	require.NotEmpty(t, hdr.Get("X-Request-Id"))
}

// Start binds the listener before it reports success: an occupied port or an
// invalid address fails startup (nothing is reported as serving), a free
// address serves at once, and Stop releases the port and ends the listener
// without an error.
func TestStartBindsTheListenerBeforeReportingStarted(t *testing.T) {
	newAt := func(listen string) *httptransport.Server {
		opts := testOptions()
		opts.Listen = listen
		srv, err := httptransport.NewServer(opts, verifier{}, newFake(), func(context.Context) error { return nil })
		require.NoError(t, err)
		return srv
	}

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer occupied.Close()
	_, err = newAt(occupied.Addr().String()).Start()
	require.Error(t, err, "an occupied port is a startup failure")
	require.Contains(t, err.Error(), "listen "+occupied.Addr().String())

	_, err = newAt("127.0.0.1:notaport").Start()
	require.Error(t, err, "an invalid listen address is a startup failure")

	srv := newAt("127.0.0.1:0")
	addr, err := srv.Start()
	require.NoError(t, err)
	resp, err := http.Get("http://" + addr.String() + "/healthz")
	require.NoError(t, err, "the bound listener serves as soon as Start returned")
	resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	select {
	case err := <-srv.Served():
		t.Fatalf("the listener stopped while serving: %v", err)
	default:
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, srv.Stop(ctx))
	select {
	case err := <-srv.Served():
		require.NoError(t, err, "a listener closed by Stop is not a failure")
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return after Stop")
	}
	released, err := net.Listen("tcp", addr.String())
	require.NoError(t, err, "the port is released after shutdown")
	released.Close()
}
