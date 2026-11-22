package http_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-contrib/sse"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

// D4a (DEC-070, design 0001 §8): one message of the caller's conversation,
// and the conversation's event stream on the operation stream's serving and
// bounds. The fake stands in for Control's ConversationService.

func (f *fakeControl) GetMessage(_ context.Context, p application.Principal, convID, messageID string) (agentapi.Message, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, err := f.conversations().owned(p, convID)
	if err != nil {
		return agentapi.Message{}, err
	}
	for _, m := range r.messages {
		if m.MessageId == messageID {
			return m, nil
		}
	}
	return agentapi.Message{}, conversationNotFound
}

func (f *fakeControl) StreamConversationEvents(ctx context.Context, p application.Principal, convID, after string, admitted func(), emit func(agentapi.ConversationEventFrame) error) error {
	f.mu.Lock()
	_, err := f.conversations().owned(p, convID)
	script := f.convStream
	f.mu.Unlock()
	if err != nil {
		return err
	}
	if script != nil {
		return script(ctx, convID, after, admitted, emit)
	}
	admitted()
	<-ctx.Done()
	return ctx.Err()
}

// aliceConversation opens one conversation of Alice's with one message and
// answers the server, the fake and both ids.
func aliceConversation(t *testing.T) (*httptest.Server, *fakeControl, string, string) {
	t.Helper()
	ts, fake := conversationServer(t)
	code, out, _ := do(t, ts, http.MethodPost, "/api/v1/conversations", tokenAlice, `{"commandId":"conv-stream-1","title":"Streaming"}`)
	require.Equal(t, http.StatusCreated, code, out)
	convID := out["conversationId"].(string)
	code, out, _ = do(t, ts, http.MethodPost, "/api/v1/conversations/"+convID+"/messages", tokenAlice,
		`{"commandId":"msg-stream-1","expectedRevision":"1","content":{"type":"text","text":"Hello"}}`)
	require.Equal(t, http.StatusCreated, code, out)
	return ts, fake, convID, out["messageId"].(string)
}

func TestGetMessage(t *testing.T) {
	ts, _, convID, messageID := aliceConversation(t)
	code, out, _ := do(t, ts, http.MethodGet, "/api/v1/conversations/"+convID+"/messages/"+messageID, tokenAlice, "")
	require.Equal(t, http.StatusOK, code, out)
	assertFrameSchema(t, "Message", mustJSON(t, out))
	require.Equal(t, messageID, out["messageId"])
	require.Equal(t, "Hello", out["content"].(map[string]any)["text"])
	code, _, _ = do(t, ts, http.MethodGet, "/api/v1/conversations/"+convID+"/messages/"+messageID, tokenBob, "")
	require.Equal(t, http.StatusNotFound, code, "another owner's message is not found")
	code, _, _ = do(t, ts, http.MethodGet, "/api/v1/conversations/"+convID+"/messages/msg_missing", tokenAlice, "")
	require.Equal(t, http.StatusNotFound, code)
}

// TestConversationEventStream: the frames are the library's SSE encoding of
// ConversationEventFrame documents (id the event sequence, event its type,
// delta text included); a stream Control ends on its own ends with an end
// frame (a conversation has no terminal state); a cursor that cannot be
// honored yields one reset_required frame with the covered sequence.
func TestConversationEventStream(t *testing.T) {
	ts, fake, convID, messageID := aliceConversation(t)
	occurred := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	seq := func(s string) *string { return &s }
	var gotAfter string
	fake.mu.Lock()
	fake.convStream = func(ctx context.Context, id, after string, admitted func(), emit func(agentapi.ConversationEventFrame) error) error {
		gotAfter = after
		admitted()
		text := "Hel"
		for _, f := range []agentapi.ConversationEventFrame{
			{ConversationId: id, EventSeq: "3", EventType: agentapi.MessageStarted, OccurredAt: occurred, MessageId: &messageID, TurnId: seq("trn_1")},
			{ConversationId: id, EventSeq: "4", EventType: agentapi.MessageDelta, OccurredAt: occurred, MessageId: &messageID, ChunkSeq: seq("1"), ContentRevision: seq("1"), Text: &text},
		} {
			if err := emit(f); err != nil {
				return err
			}
		}
		return nil
	}
	fake.mu.Unlock()
	resp := sseRequest(t, ts, "/api/v1/conversations/"+convID+"/events?after=2", tokenAlice, nil)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.True(t, strings.HasPrefix(resp.Header.Get("Content-Type"), "text/event-stream"))
	events, err := sse.Decode(resp.Body)
	require.NoError(t, err)
	require.Equal(t, "2", gotAfter)
	require.Len(t, events, 3)
	require.Equal(t, "3", events[0].Id)
	require.Equal(t, "message.started", events[0].Event)
	require.Equal(t, "4", events[1].Id)
	require.Equal(t, "message.delta", events[1].Event)
	for _, ev := range events[:2] {
		assertFrameSchema(t, "ConversationEventFrame", ev.Data.(string))
	}
	assertFrameSchema(t, "ConversationEndFrame", events[2].Data.(string))
	var delta agentapi.ConversationEventFrame
	require.NoError(t, json.Unmarshal([]byte(events[1].Data.(string)), &delta))
	require.Equal(t, "Hel", *delta.Text)
	require.Equal(t, "1", *delta.ChunkSeq)
	require.Equal(t, "end", events[2].Event, "Control ended the stream: the client resumes")
	require.Empty(t, events[2].Id)
	require.JSONEq(t, `{"conversationId":"`+convID+`","reason":"server_shutdown"}`, events[2].Data.(string))

	fake.mu.Lock()
	fake.convStream = func(context.Context, string, string, func(), func(agentapi.ConversationEventFrame) error) error {
		return &application.ResetRequired{CoveredEventSeq: "7"}
	}
	fake.mu.Unlock()
	resp = sseRequest(t, ts, "/api/v1/conversations/"+convID+"/events", tokenAlice, map[string]string{"Last-Event-ID": "99"})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	events, err = sse.Decode(resp.Body)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, "reset_required", events[0].Event)
	assertFrameSchema(t, "ConversationResetFrame", events[0].Data.(string))
	require.JSONEq(t, `{"conversationId":"`+convID+`","coveredEventSeq":"7"}`, events[0].Data.(string))
}

// TestConversationEventCursor (design 0001 §8.1): a cursor named twice with
// different values is refused INVALID_ARGUMENT naming the parameter; the
// same value twice is one cursor; a malformed cursor and another owner's
// conversation are refused before any stream.
func TestConversationEventCursor(t *testing.T) {
	ts, fake, convID, _ := aliceConversation(t)
	var gotAfter string
	fake.mu.Lock()
	fake.convStream = func(_ context.Context, _, after string, admitted func(), _ func(agentapi.ConversationEventFrame) error) error {
		gotAfter = after
		admitted()
		return nil
	}
	fake.mu.Unlock()
	path := "/api/v1/conversations/" + convID + "/events"

	resp := sseRequest(t, ts, path+"?after=1", tokenAlice, map[string]string{"Last-Event-ID": "2"})
	body := readBody(t, resp)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode, body)
	require.Contains(t, body, `"code":"INVALID_ARGUMENT"`)
	require.Contains(t, body, `"field":"after"`)

	resp = sseRequest(t, ts, path+"?after=2", tokenAlice, map[string]string{"Last-Event-ID": "2"})
	_ = readBody(t, resp)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.Equal(t, "2", gotAfter)

	resp = sseRequest(t, ts, path, tokenAlice, map[string]string{"Last-Event-ID": "5"})
	_ = readBody(t, resp)
	require.Equal(t, "5", gotAfter, "the header alone is the cursor")

	resp = sseRequest(t, ts, path+"?after=01", tokenAlice, nil)
	_ = readBody(t, resp)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)

	resp = sseRequest(t, ts, path, tokenBob, nil)
	_ = readBody(t, resp)
	require.Equal(t, http.StatusNotFound, resp.StatusCode, "another owner's conversation")
}

// TestConversationStreamStaysOpen: a conversation stream opens with its
// retry interval and stays open until the client goes.
func TestConversationStreamStaysOpen(t *testing.T) {
	ts, _, convID, _ := aliceConversation(t)
	resp := sseRequest(t, ts, "/api/v1/conversations/"+convID+"/events", tokenAlice, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(line, "retry: "), "the stream opens with its retry interval")
	require.NoError(t, resp.Body.Close(), "the client goes; the stream ends with it")
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	var b strings.Builder
	_, err := bufioCopy(&b, resp.Body)
	require.NoError(t, err)
	return b.String()
}
