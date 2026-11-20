package http_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	httptransport "github.com/ancyloce/anvilkit-agent-api/internal/transport/http"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

// The principal's personal conversations (DEC-067). The fake stands in for
// Control's ConversationService for the transport concerns under test: the
// strict request shapes, the scope and command the API derives from the
// verified principal, NOT_FOUND for another owner, and the replay of a
// command; Control's own tests prove the storage rules.

const (
	tokenAlice = "token-alice-a-0123456789"
	tokenBob   = "token-bob-a-0123456789xx"
	tokenCarol = "token-carol-b-0123456789"
)

type conversationVerifier struct{}

func (conversationVerifier) Verify(ctx context.Context, bearer string) (application.Principal, error) {
	switch bearer {
	case tokenAlice:
		return application.Principal{TenantID: "tenant_a", ProjectID: "project_a", ActorID: "user_alice"}, nil
	case tokenBob:
		return application.Principal{TenantID: "tenant_a", ProjectID: "project_a", ActorID: "user_bob"}, nil
	case tokenCarol:
		return application.Principal{TenantID: "tenant_b", ProjectID: "project_a", ActorID: "user_alice"}, nil
	}
	return verifier{}.Verify(ctx, bearer)
}

type conversationRecord struct {
	conv     agentapi.Conversation
	messages []agentapi.Message
}

type conversationCommand struct {
	cmd     application.CommandIdentity
	convID  string
	receipt agentapi.MessageReceipt
}

type fakeConversations struct {
	convs    map[string]*conversationRecord
	commands map[string]conversationCommand // tenant/commandId
	calls    int
	scopes   []application.Principal
	// replies opens a turn per message (DEC-068); open is each
	// conversation's unfinished turn, stops the stop commands recorded.
	replies bool
	turns   map[string]*agentapi.Turn
	open    map[string]string
	stops   []application.CommandIdentity
}

func (f *fakeControl) conversations() *fakeConversations {
	if f.conv == nil {
		f.conv = &fakeConversations{convs: map[string]*conversationRecord{}, commands: map[string]conversationCommand{}, turns: map[string]*agentapi.Turn{}, open: map[string]string{}}
	}
	return f.conv
}

var conversationNotFound = &application.ControlError{Code: "NOT_FOUND", Message: "not found"}

func (fc *fakeConversations) owned(p application.Principal, id string) (*conversationRecord, error) {
	fc.calls++
	fc.scopes = append(fc.scopes, p)
	r, ok := fc.convs[id]
	if !ok || r.conv.TenantId != p.TenantID || r.conv.OwnerActorId != p.ActorID || deref(r.conv.ProjectId) != p.ProjectID {
		return nil, conversationNotFound
	}
	return r, nil
}

// replay answers a recorded command: the same actor and digest replay it,
// anything else reusing its id conflicts.
func (fc *fakeConversations) replay(cmd application.CommandIdentity) (conversationCommand, bool, error) {
	prior, ok := fc.commands[cmd.TenantID+"/"+cmd.CommandID]
	if !ok {
		return conversationCommand{}, false, nil
	}
	if prior.cmd.ActorID != cmd.ActorID || prior.cmd.RequestDigest != cmd.RequestDigest {
		return conversationCommand{}, false, &application.ControlError{Code: "IDEMPOTENCY_CONFLICT", Message: "another request"}
	}
	return prior, true, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func (f *fakeControl) CreateConversation(_ context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateConversationRequest) (agentapi.Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fc := f.conversations()
	fc.calls++
	fc.scopes = append(fc.scopes, p)
	if prior, ok, err := fc.replay(cmd); err != nil || ok {
		if err != nil {
			return agentapi.Conversation{}, err
		}
		return fc.convs[prior.convID].conv, nil
	}
	now := time.Date(2026, 11, 19, 2, 30, len(fc.convs), 0, time.UTC)
	c := agentapi.Conversation{ConversationId: fmt.Sprintf("cnv_%d", len(fc.convs)+1), TenantId: p.TenantID, OwnerActorId: p.ActorID,
		Status: agentapi.ConversationStatusActive, Title: req.Title, Revision: "1", LastMessageSeq: "0", CreatedAt: now, UpdatedAt: now}
	if p.ProjectID != "" {
		c.ProjectId = &p.ProjectID
	}
	fc.convs[c.ConversationId] = &conversationRecord{conv: c}
	fc.commands[cmd.TenantID+"/"+cmd.CommandID] = conversationCommand{cmd: cmd, convID: c.ConversationId}
	return c, nil
}

func (f *fakeControl) GetConversation(_ context.Context, p application.Principal, id string) (agentapi.Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, err := f.conversations().owned(p, id)
	if err != nil {
		return agentapi.Conversation{}, err
	}
	return r.conv, nil
}

func (f *fakeControl) ListConversations(_ context.Context, p application.Principal, params agentapi.ListConversationsParams) (agentapi.ConversationPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fc := f.conversations()
	fc.calls++
	page := agentapi.ConversationPage{Conversations: []agentapi.Conversation{}}
	for _, r := range fc.convs {
		if r.conv.TenantId == p.TenantID && r.conv.OwnerActorId == p.ActorID && (params.Status == nil || r.conv.Status == *params.Status) {
			page.Conversations = append(page.Conversations, r.conv)
		}
	}
	return page, nil
}

func (f *fakeControl) UpdateConversation(_ context.Context, cmd application.CommandIdentity, p application.Principal, id string, req agentapi.UpdateConversationRequest) (agentapi.Conversation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fc := f.conversations()
	r, err := fc.owned(p, id)
	if err != nil {
		return agentapi.Conversation{}, err
	}
	if _, ok, err := fc.replay(cmd); err != nil || ok {
		return r.conv, err
	}
	if req.ExpectedRevision != r.conv.Revision {
		return agentapi.Conversation{}, &application.ControlError{Code: "REVISION_CONFLICT", Message: "conversation at revision " + r.conv.Revision}
	}
	if req.Status != nil {
		r.conv.Status = *req.Status
	}
	if req.Title != nil {
		r.conv.Title = req.Title
	}
	r.conv.Revision = bump(r.conv.Revision)
	fc.commands[cmd.TenantID+"/"+cmd.CommandID] = conversationCommand{cmd: cmd, convID: id}
	return r.conv, nil
}

func (f *fakeControl) AppendMessage(_ context.Context, cmd application.CommandIdentity, p application.Principal, id string, req agentapi.AppendMessageRequest) (agentapi.MessageReceipt, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fc := f.conversations()
	r, err := fc.owned(p, id)
	if err != nil {
		return agentapi.MessageReceipt{}, err
	}
	if prior, ok, err := fc.replay(cmd); err != nil || ok {
		return prior.receipt, err
	}
	if _, busy := fc.open[id]; busy {
		// Control's refusal, as MapError carries it (maperror_test.go).
		return agentapi.MessageReceipt{}, &application.ControlError{Code: "CAPACITY_EXHAUSTED", Message: "TURN_BUSY: conversation " + id + " has an unfinished turn", Retryable: true}
	}
	switch {
	case req.ExpectedRevision != r.conv.Revision:
		return agentapi.MessageReceipt{}, &application.ControlError{Code: "REVISION_CONFLICT", Message: "conversation at revision " + r.conv.Revision}
	case r.conv.Status != agentapi.ConversationStatusActive:
		return agentapi.MessageReceipt{}, &application.ControlError{Code: "INVALID_ARGUMENT", Message: "conversation archived"}
	case len(req.Content.Text) > 32768:
		return agentapi.MessageReceipt{}, &application.ControlError{Code: "INVALID_ARGUMENT", Message: "a message is 1 to 32768 bytes"}
	}
	r.conv.LastMessageSeq, r.conv.Revision = bump(r.conv.LastMessageSeq), bump(r.conv.Revision)
	m := agentapi.Message{MessageId: fmt.Sprintf("msg_%s_%s", id, r.conv.LastMessageSeq), ConversationId: id, MessageSeq: r.conv.LastMessageSeq,
		Role: "user", State: "completed", Content: req.Content, ContentDigest: digest, CreatedAt: r.conv.CreatedAt}
	r.messages = append(r.messages, m)
	receipt := agentapi.MessageReceipt{ConversationId: id, MessageId: m.MessageId, MessageSeq: m.MessageSeq, ConversationRevision: r.conv.Revision, CreatedAt: m.CreatedAt}
	if fc.replies {
		turnID, opID := "trn_"+m.MessageId, "op_"+m.MessageId
		fc.turns[turnID] = &agentapi.Turn{TurnId: turnID, ConversationId: id, InputMessageId: m.MessageId, OperationId: opID, State: agentapi.TurnStateQueued,
			ContentStatus: agentapi.ContentStatusUnknown, SenderStatus: agentapi.SenderStatusNotStarted, Finance: "funded", CreatedAt: m.CreatedAt, UpdatedAt: m.CreatedAt}
		fc.open[id] = turnID
		receipt.TurnId, receipt.OperationId = &turnID, &opID
	}
	fc.commands[cmd.TenantID+"/"+cmd.CommandID] = conversationCommand{cmd: cmd, convID: id, receipt: receipt}
	return receipt, nil
}

func (f *fakeControl) ListMessages(_ context.Context, p application.Principal, id string, _ agentapi.ListMessagesParams) (agentapi.MessagePage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, err := f.conversations().owned(p, id)
	if err != nil {
		return agentapi.MessagePage{}, err
	}
	return agentapi.MessagePage{Messages: append([]agentapi.Message{}, r.messages...)}, nil
}

func (f *fakeControl) GetTurn(_ context.Context, p application.Principal, convID, turnID string) (agentapi.Turn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fc := f.conversations()
	if _, err := fc.owned(p, convID); err != nil {
		return agentapi.Turn{}, err
	}
	t, ok := fc.turns[turnID]
	if !ok || t.ConversationId != convID {
		return agentapi.Turn{}, conversationNotFound
	}
	return *t, nil
}

func (f *fakeControl) StopTurn(_ context.Context, cmd application.CommandIdentity, p application.Principal, convID, turnID string) (agentapi.Turn, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fc := f.conversations()
	if _, err := fc.owned(p, convID); err != nil {
		return agentapi.Turn{}, err
	}
	t, ok := fc.turns[turnID]
	if !ok || t.ConversationId != convID {
		return agentapi.Turn{}, conversationNotFound
	}
	fc.stops = append(fc.stops, cmd)
	if t.State == agentapi.TurnStateQueued || t.State == agentapi.TurnStateRunning {
		t.State = agentapi.TurnStateCancelPending
	}
	return *t, nil
}

func bump(s string) string {
	v, _ := strconv.ParseUint(s, 10, 64)
	return strconv.FormatUint(v+1, 10)
}

func conversationServer(t *testing.T) (*httptest.Server, *fakeControl) {
	t.Helper()
	fake := newFake()
	srv, err := httptransport.NewServer(testOptions(), conversationVerifier{}, fake, httptransport.Gateways{}, func(context.Context) error { return nil })
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return ts, fake
}

func TestConversationRoutes(t *testing.T) {
	ts, fake := conversationServer(t)

	code, out, _ := do(t, ts, http.MethodPost, "/api/v1/conversations", tokenAlice, `{"commandId":"conv-1","title":"Navigation bar"}`)
	require.Equal(t, http.StatusCreated, code)
	assertFrameSchema(t, "Conversation", mustJSON(t, out))
	require.Equal(t, "tenant_a", out["tenantId"], "the tenant, project and owner come from the principal")
	require.Equal(t, "project_a", out["projectId"])
	require.Equal(t, "user_alice", out["ownerActorId"])
	convID := out["conversationId"].(string)
	created := fake.conversations().commands["tenant_a/conv-1"].cmd
	require.Equal(t, application.CommandIdentity{TenantID: "tenant_a", CommandID: "conv-1", ActorID: "user_alice", RequestDigest: created.RequestDigest}, created)
	require.True(t, strings.HasPrefix(created.RequestDigest, "sha256:"))

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/conversations", tokenAlice, ` { "title" : "Navigation bar", "commandId" : "conv-1" } `)
	require.Equal(t, http.StatusCreated, code, "a retry with other formatting is the same command")
	require.Equal(t, convID, out["conversationId"])
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/conversations", tokenAlice, `{"commandId":"conv-1","title":"Other"}`)
	require.Equal(t, http.StatusConflict, code)
	require.Equal(t, "IDEMPOTENCY_CONFLICT", errorCode(out))

	t.Run("strict requests reach no owner", func(t *testing.T) {
		before := fake.conversations().calls
		for name, c := range map[string]struct{ method, path, body string }{
			"a role":                 {http.MethodPost, "/messages", `{"commandId":"m","expectedRevision":"1","role":"system","content":{"type":"text","text":"x"}}`},
			"a role in the content":  {http.MethodPost, "/messages", `{"commandId":"m","expectedRevision":"1","content":{"type":"text","text":"x","role":"assistant"}}`},
			"another content type":   {http.MethodPost, "/messages", `{"commandId":"m","expectedRevision":"1","content":{"type":"html","text":"<b>x</b>"}}`},
			"empty text":             {http.MethodPost, "/messages", `{"commandId":"m","expectedRevision":"1","content":{"type":"text","text":""}}`},
			"32769 characters":       {http.MethodPost, "/messages", `{"commandId":"m","expectedRevision":"1","content":{"type":"text","text":"` + strings.Repeat("a", 32769) + `"}}`},
			"a duplicate member":     {http.MethodPost, "/messages", `{"commandId":"m","expectedRevision":"1","content":{"type":"text","text":"a","text":"b"}}`},
			"a numeric revision":     {http.MethodPost, "/messages", `{"commandId":"m","expectedRevision":1,"content":{"type":"text","text":"x"}}`},
			"a 21-digit revision":    {http.MethodPatch, "", `{"commandId":"u","expectedRevision":"123456789012345678901","title":"x"}`},
			"a deleted status":       {http.MethodPatch, "", `{"commandId":"u","expectedRevision":"1","status":"deleted"}`},
			"a null title":           {http.MethodPatch, "", `{"commandId":"u","expectedRevision":"1","title":null}`},
			"a limit above 100":      {http.MethodGet, "/messages?limit=101", ""},
			"a sequence with a sign": {http.MethodGet, "/messages?beforeMessageSeq=-1", ""},
		} {
			code, out, _ := do(t, ts, c.method, "/api/v1/conversations/"+convID+c.path, tokenAlice, c.body)
			require.Equal(t, http.StatusBadRequest, code, name)
			require.Equal(t, "INVALID_ARGUMENT", errorCode(out), name)
		}
		code, _, _ := do(t, ts, http.MethodPost, "/api/v1/conversations", tokenAlice, `{"commandId":"conv-x","projectId":"project_b"}`)
		require.Equal(t, http.StatusBadRequest, code, "a request names no scope")
		require.Equal(t, before, fake.conversations().calls, "Control is never asked")
	})

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/conversations/"+convID+"/messages", tokenAlice, `{"commandId":"msg-1","expectedRevision":"1","content":{"type":"text","text":"Make it black and add a search box 🔍"}}`)
	require.Equal(t, http.StatusCreated, code)
	assertFrameSchema(t, "MessageReceipt", mustJSON(t, out))
	require.Equal(t, "1", out["messageSeq"])
	require.Equal(t, "2", out["conversationRevision"])
	require.NotContains(t, out, "turnId", "no reply is produced in this slice")
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/conversations/"+convID+"/messages", tokenAlice, `{"commandId":"msg-1","expectedRevision":"1","content":{"type":"text","text":"Make it black and add a search box 🔍"}}`)
	require.Equal(t, http.StatusCreated, code, "a retried message answers the original receipt")
	require.Equal(t, "2", out["conversationRevision"])
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/conversations/"+convID+"/messages", tokenAlice, `{"commandId":"msg-2","expectedRevision":"1","content":{"type":"text","text":"stale"}}`)
	require.Equal(t, http.StatusConflict, code)
	require.Equal(t, "REVISION_CONFLICT", errorCode(out))
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/conversations/"+convID+"/messages", tokenAlice, `{"commandId":"msg-3","expectedRevision":"2","content":{"type":"text","text":"`+strings.Repeat("é", 20000)+`"}}`)
	require.Equal(t, http.StatusBadRequest, code, "Control's byte bound: 20000 characters are 40000 bytes")
	require.Equal(t, "INVALID_ARGUMENT", errorCode(out))

	code, out, _ = do(t, ts, http.MethodGet, "/api/v1/conversations/"+convID+"/messages", tokenAlice, "")
	require.Equal(t, http.StatusOK, code)
	assertFrameSchema(t, "MessagePage", mustJSON(t, out))
	require.Len(t, out["messages"], 1)
	first := out["messages"].([]any)[0].(map[string]any)
	require.Equal(t, "Make it black and add a search box 🔍", first["content"].(map[string]any)["text"])
	require.Equal(t, "user", first["role"])

	t.Run("another owner or tenant gets not found", func(t *testing.T) {
		for name, token := range map[string]string{"another user of the project": tokenBob, "the same actor id in another tenant": tokenCarol} {
			for _, c := range []struct{ method, path, body string }{
				{http.MethodGet, "", ""},
				{http.MethodGet, "/messages", ""},
				{http.MethodPost, "/messages", `{"commandId":"intrude-1","expectedRevision":"2","content":{"type":"text","text":"x"}}`},
				{http.MethodPatch, "", `{"commandId":"intrude-2","expectedRevision":"2","status":"archived"}`},
			} {
				code, out, _ := do(t, ts, c.method, "/api/v1/conversations/"+convID+c.path, token, c.body)
				require.Equal(t, http.StatusNotFound, code, "%s %s %s", name, c.method, c.path)
				require.Equal(t, "NOT_FOUND", errorCode(out))
				require.NotContains(t, mustJSON(t, out), "search box", "no body is disclosed")
			}
			code, out, _ := do(t, ts, http.MethodGet, "/api/v1/conversations", token, "")
			require.Equal(t, http.StatusOK, code)
			require.NotContains(t, mustJSON(t, out), convID, name)
		}
		code, out, _ := do(t, ts, http.MethodPost, "/api/v1/conversations", tokenBob, `{"commandId":"conv-1","title":"Navigation bar"}`)
		require.Equal(t, http.StatusConflict, code, "another user reusing a command id is refused, not answered with the owner's conversation")
		require.Equal(t, "IDEMPOTENCY_CONFLICT", errorCode(out))
	})

	code, out, _ = do(t, ts, http.MethodPatch, "/api/v1/conversations/"+convID, tokenAlice, `{"commandId":"archive-1","expectedRevision":"2","status":"archived"}`)
	require.Equal(t, http.StatusOK, code)
	assertFrameSchema(t, "Conversation", mustJSON(t, out))
	require.Equal(t, "archived", out["status"])
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/conversations/"+convID+"/messages", tokenAlice, `{"commandId":"msg-4","expectedRevision":"3","content":{"type":"text","text":"late"}}`)
	require.Equal(t, http.StatusBadRequest, code, "an archived conversation takes no message")
	require.Equal(t, "INVALID_ARGUMENT", errorCode(out))
	code, out, _ = do(t, ts, http.MethodGet, "/api/v1/conversations?status=archived&limit=10", tokenAlice, "")
	require.Equal(t, http.StatusOK, code)
	assertFrameSchema(t, "ConversationPage", mustJSON(t, out))
	require.Len(t, out["conversations"], 1)
	for _, p := range fake.conversations().scopes {
		require.NotEmpty(t, p.ActorID, "every call carries the verified principal")
	}
}

// TestConversationTurnRoutes (DEC-068): where replies are enabled a message's
// receipt names its turn and operation; a message while that turn is
// unfinished is a retryable 429 CAPACITY_EXHAUSTED whose message begins
// TURN_BUSY: and that names when to come back; the turn is the owner's to
// read and to stop, a stop is accepted with 202 under the command the API
// derives from the verified principal and the canonical body, and its body is
// strict.
func TestConversationTurnRoutes(t *testing.T) {
	ts, fake := conversationServer(t)
	fake.conversations().replies = true
	code, out, _ := do(t, ts, http.MethodPost, "/api/v1/conversations", tokenAlice, `{"commandId":"conv-t"}`)
	require.Equal(t, http.StatusCreated, code)
	convID := out["conversationId"].(string)

	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/conversations/"+convID+"/messages", tokenAlice, `{"commandId":"msg-t1","expectedRevision":"1","content":{"type":"text","text":"Make it black"}}`)
	require.Equal(t, http.StatusCreated, code)
	assertFrameSchema(t, "MessageReceipt", mustJSON(t, out))
	turnID, opID := out["turnId"].(string), out["operationId"].(string)
	require.NotEmpty(t, turnID)
	require.NotEmpty(t, opID)

	code, out, header := do(t, ts, http.MethodPost, "/api/v1/conversations/"+convID+"/messages", tokenAlice, `{"commandId":"msg-t2","expectedRevision":"2","content":{"type":"text","text":"And a search box"}}`)
	require.Equal(t, http.StatusTooManyRequests, code, "the previous turn is unfinished")
	require.Equal(t, "CAPACITY_EXHAUSTED", errorCode(out))
	envelope := out["error"].(map[string]any)
	require.True(t, strings.HasPrefix(envelope["message"].(string), "TURN_BUSY:"), envelope["message"])
	require.Equal(t, true, envelope["retryable"])
	require.Equal(t, "5", header.Get("Retry-After"))

	path := "/api/v1/conversations/" + convID + "/turns/" + turnID
	code, out, _ = do(t, ts, http.MethodGet, path, tokenAlice, "")
	require.Equal(t, http.StatusOK, code)
	assertFrameSchema(t, "Turn", mustJSON(t, out))
	require.Equal(t, "queued", out["state"])
	require.Equal(t, opID, out["operationId"])
	require.NotContains(t, out, "text", "a turn carries no text")

	code, out, _ = do(t, ts, http.MethodPost, path+"/cancellations", tokenAlice, `{"commandId":"stop-1"}`)
	require.Equal(t, http.StatusAccepted, code)
	assertFrameSchema(t, "Turn", mustJSON(t, out))
	require.Equal(t, "cancel_pending", out["state"])
	stop := fake.conversations().stops[0]
	require.Equal(t, application.CommandIdentity{TenantID: "tenant_a", CommandID: "stop-1", ActorID: "user_alice", RequestDigest: stop.RequestDigest}, stop)
	require.True(t, strings.HasPrefix(stop.RequestDigest, "sha256:"))

	t.Run("strict requests reach no owner", func(t *testing.T) {
		before := len(fake.conversations().stops)
		for name, body := range map[string]string{
			"no command":       `{}`,
			"another member":   `{"commandId":"stop-2","reason":"too slow"}`,
			"a numeric id":     `{"commandId":7}`,
			"a duplicate":      `{"commandId":"stop-2","commandId":"stop-3"}`,
			"a revision named": `{"commandId":"stop-2","expectedRevision":"3"}`,
		} {
			code, out, _ := do(t, ts, http.MethodPost, path+"/cancellations", tokenAlice, body)
			require.Equal(t, http.StatusBadRequest, code, name)
			require.Equal(t, "INVALID_ARGUMENT", errorCode(out), name)
		}
		require.Equal(t, before, len(fake.conversations().stops), "Control is never asked")
	})

	t.Run("another owner or tenant gets not found", func(t *testing.T) {
		for name, token := range map[string]string{"another user of the project": tokenBob, "the same actor id in another tenant": tokenCarol} {
			code, out, _ := do(t, ts, http.MethodGet, path, token, "")
			require.Equal(t, http.StatusNotFound, code, name)
			require.Equal(t, "NOT_FOUND", errorCode(out), name)
			code, _, _ = do(t, ts, http.MethodPost, path+"/cancellations", token, `{"commandId":"intrude-stop"}`)
			require.Equal(t, http.StatusNotFound, code, name)
		}
		code, _, _ := do(t, ts, http.MethodGet, "/api/v1/conversations/"+convID+"/turns/trn_unknown", tokenAlice, "")
		require.Equal(t, http.StatusNotFound, code, "a turn of no message")
	})
}
