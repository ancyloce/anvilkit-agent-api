package control

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// DEC-067: the principal's personal conversations over Control's
// ConversationService. Every call carries the principal's scope, which is
// the only owner a conversation can have; no request field names a tenant,
// project, actor or role.

var conversationStatuses = map[agentapi.ConversationStatus]controlv1.ConversationStatus{
	agentapi.ConversationStatusActive:   controlv1.ConversationStatus_CONVERSATION_STATUS_ACTIVE,
	agentapi.ConversationStatusArchived: controlv1.ConversationStatus_CONVERSATION_STATUS_ARCHIVED,
}

func toConversation(c *controlv1.Conversation) agentapi.Conversation {
	return agentapi.Conversation{
		ConversationId: c.GetConversationId(), TenantId: c.GetTenantId(), ProjectId: optional(c.GetProjectId()), OwnerActorId: c.GetOwnerActorId(),
		Status: agentapi.ConversationStatus(enumWord(c.GetStatus().String(), "CONVERSATION_STATUS_")), Title: c.Title,
		Revision: c.GetRevision(), LastMessageSeq: c.GetLastMessageSeq(),
		CreatedAt: c.GetCreatedAt().AsTime().UTC(), UpdatedAt: c.GetUpdatedAt().AsTime().UTC(),
	}
}

func toMessage(m *controlv1.Message) agentapi.Message {
	return agentapi.Message{
		MessageId: m.GetMessageId(), ConversationId: m.GetConversationId(), MessageSeq: m.GetMessageSeq(),
		Role:    agentapi.MessageRole(enumWord(m.GetRole().String(), "MESSAGE_ROLE_")),
		State:   agentapi.MessageState(enumWord(m.GetState().String(), "MESSAGE_STATE_")),
		Content: agentapi.MessageContent{Type: agentapi.Text, Text: m.GetText()}, ContentDigest: m.GetContentDigest(),
		CreatedAt: m.GetCreatedAt().AsTime().UTC(),
		// D4a: the text's revision and the chunks it covers; absent from a
		// Control before D4a.
		ContentRevision: optional(m.GetContentRevision()), ThroughChunkSeq: optional(m.GetThroughChunkSeq()),
	}
}

// toTurn is the public turn view (DEC-068): its state and the reply's content
// and sender status, the operation's finance, never text.
func toTurn(t *controlv1.Turn) agentapi.Turn {
	return agentapi.Turn{
		TurnId: t.GetTurnId(), ConversationId: t.GetConversationId(), InputMessageId: t.GetInputMessageId(), AssistantMessageId: t.AssistantMessageId,
		OperationId:   t.GetOperationId(),
		State:         agentapi.TurnState(enumWord(t.GetState().String(), "TURN_STATE_")),
		ContentStatus: agentapi.ContentStatus(enumWord(t.GetContentStatus().String(), "CONTENT_STATUS_")),
		SenderStatus:  agentapi.SenderStatus(enumWord(t.GetSenderStatus().String(), "SENDER_STATUS_")),
		Finance:       agentapi.FinanceState(enumWord(t.GetFinance().String(), "FINANCE_STATE_")),
		FailureCode:   t.FailureCode, CreatedAt: t.GetCreatedAt().AsTime().UTC(), UpdatedAt: t.GetUpdatedAt().AsTime().UTC(),
	}
}

func limitOf(l *agentapi.HistoryLimit) int32 {
	if l == nil {
		return 0
	}
	return int32(*l)
}

func (c *Client) CreateConversation(ctx context.Context, cmd application.CommandIdentity, p application.Principal, req agentapi.CreateConversationRequest) (agentapi.Conversation, error) {
	resp, err := controlv1.NewConversationServiceClient(c.conn).CreateConversation(ctx, &controlv1.CreateConversationRequest{
		Command: command(cmd), Scope: scope(p), Title: req.Title,
	})
	if err != nil {
		return agentapi.Conversation{}, mapErr(err)
	}
	return toConversation(resp.GetConversation()), nil
}

func (c *Client) GetConversation(ctx context.Context, p application.Principal, conversationID string) (agentapi.Conversation, error) {
	resp, err := controlv1.NewConversationServiceClient(c.conn).GetConversation(ctx, &controlv1.GetConversationRequest{Scope: scope(p), ConversationId: conversationID})
	if err != nil {
		return agentapi.Conversation{}, mapErr(err)
	}
	out := toConversation(resp.GetConversation())
	// D4a: the snapshot's covered event and open turn; a client subscribes
	// to the events after the covered sequence.
	out.CoveredEventSeq = optional(resp.GetCoveredEventSeq())
	if t := resp.GetActiveTurn(); t != nil {
		turn := toTurn(t)
		out.ActiveTurn = &turn
	}
	return out, nil
}

func (c *Client) ListConversations(ctx context.Context, p application.Principal, params agentapi.ListConversationsParams) (agentapi.ConversationPage, error) {
	req := &controlv1.ListConversationsRequest{Scope: scope(p), Cursor: deref(params.Cursor), Limit: limitOf(params.Limit)}
	if params.Status != nil {
		req.Status = conversationStatuses[*params.Status]
	}
	resp, err := controlv1.NewConversationServiceClient(c.conn).ListConversations(ctx, req)
	if err != nil {
		return agentapi.ConversationPage{}, mapErr(err)
	}
	page := agentapi.ConversationPage{Conversations: make([]agentapi.Conversation, 0, len(resp.GetConversations())), NextCursor: resp.NextCursor}
	for _, conv := range resp.GetConversations() {
		page.Conversations = append(page.Conversations, toConversation(conv))
	}
	return page, nil
}

func (c *Client) UpdateConversation(ctx context.Context, cmd application.CommandIdentity, p application.Principal, conversationID string, req agentapi.UpdateConversationRequest) (agentapi.Conversation, error) {
	in := &controlv1.UpdateConversationRequest{Command: command(cmd), Scope: scope(p), ConversationId: conversationID, ExpectedRevision: req.ExpectedRevision, Title: req.Title}
	if req.Status != nil {
		in.Status = conversationStatuses[*req.Status]
	}
	resp, err := controlv1.NewConversationServiceClient(c.conn).UpdateConversation(ctx, in)
	if err != nil {
		return agentapi.Conversation{}, mapErr(err)
	}
	return toConversation(resp.GetConversation()), nil
}

func (c *Client) AppendMessage(ctx context.Context, cmd application.CommandIdentity, p application.Principal, conversationID string, req agentapi.AppendMessageRequest) (agentapi.MessageReceipt, error) {
	resp, err := controlv1.NewConversationServiceClient(c.conn).AppendMessage(ctx, &controlv1.AppendMessageRequest{
		Command: command(cmd), Scope: scope(p), ConversationId: conversationID, ExpectedRevision: req.ExpectedRevision, Text: req.Content.Text,
	})
	if err != nil {
		return agentapi.MessageReceipt{}, mapErr(err)
	}
	r := resp.GetReceipt()
	return agentapi.MessageReceipt{
		ConversationId: r.GetConversationId(), MessageId: r.GetMessageId(), MessageSeq: r.GetMessageSeq(),
		ConversationRevision: r.GetConversationRevision(), CreatedAt: r.GetCreatedAt().AsTime().UTC(),
		TurnId: r.TurnId, AssistantMessageId: r.AssistantMessageId, OperationId: r.OperationId,
	}, nil
}

func (c *Client) ListMessages(ctx context.Context, p application.Principal, conversationID string, params agentapi.ListMessagesParams) (agentapi.MessagePage, error) {
	resp, err := controlv1.NewConversationServiceClient(c.conn).ListMessages(ctx, &controlv1.ListMessagesRequest{
		Scope: scope(p), ConversationId: conversationID, BeforeMessageSeq: params.BeforeMessageSeq, Limit: limitOf(params.Limit),
	})
	if err != nil {
		return agentapi.MessagePage{}, mapErr(err)
	}
	page := agentapi.MessagePage{Messages: make([]agentapi.Message, 0, len(resp.GetMessages())), NextBeforeMessageSeq: resp.NextBeforeMessageSeq}
	for _, m := range resp.GetMessages() {
		page.Messages = append(page.Messages, toMessage(m))
	}
	return page, nil
}

func (c *Client) GetTurn(ctx context.Context, p application.Principal, conversationID, turnID string) (agentapi.Turn, error) {
	resp, err := controlv1.NewConversationServiceClient(c.conn).GetTurn(ctx, &controlv1.GetTurnRequest{Scope: scope(p), ConversationId: conversationID, TurnId: turnID})
	if err != nil {
		return agentapi.Turn{}, mapErr(err)
	}
	return toTurn(resp.GetTurn()), nil
}

func (c *Client) StopTurn(ctx context.Context, cmd application.CommandIdentity, p application.Principal, conversationID, turnID string) (agentapi.Turn, error) {
	resp, err := controlv1.NewConversationServiceClient(c.conn).StopTurn(ctx, &controlv1.StopTurnRequest{Command: command(cmd), Scope: scope(p), ConversationId: conversationID, TurnId: turnID})
	if err != nil {
		return agentapi.Turn{}, mapErr(err)
	}
	return toTurn(resp.GetTurn()), nil
}

func (c *Client) GetMessage(ctx context.Context, p application.Principal, conversationID, messageID string) (agentapi.Message, error) {
	resp, err := controlv1.NewConversationServiceClient(c.conn).GetMessage(ctx, &controlv1.GetMessageRequest{Scope: scope(p), ConversationId: conversationID, MessageId: messageID})
	if err != nil {
		return agentapi.Message{}, mapErr(err)
	}
	return toMessage(resp.GetMessage()), nil
}

// StreamConversationEvents follows Control's conversation stream (D4a,
// DEC-070). The cursor is checked against the committed log first, so an
// overrun cursor becomes a reset frame; Control ends a stream whose cursor
// it can no longer honor INVALID_ARGUMENT, which is read back as a reset at
// the covered sequence.
func (c *Client) StreamConversationEvents(ctx context.Context, p application.Principal, conversationID, afterSeq string, admitted func(), emit func(agentapi.ConversationEventFrame) error) error {
	svc := controlv1.NewConversationServiceClient(c.conn)
	covered := func(after string) error {
		page, err := svc.ListConversationEvents(ctx, &controlv1.ListConversationEventsRequest{Scope: scope(p), ConversationId: conversationID, AfterEventSeq: after, Limit: 1})
		if err != nil {
			return mapErr(err)
		}
		if page.GetResetRequired() {
			return &application.ResetRequired{CoveredEventSeq: page.GetCoveredEventSeq()}
		}
		return nil
	}
	reset := func(err error, after string) error {
		if status.Code(err) != codes.InvalidArgument {
			return mapErr(err)
		}
		if again := covered(after); again != nil {
			return again
		}
		return mapErr(err)
	}
	if err := covered(afterSeq); err != nil {
		return err
	}
	stream, err := svc.StreamConversationEvents(ctx, &controlv1.StreamConversationEventsRequest{Scope: scope(p), ConversationId: conversationID, AfterEventSeq: afterSeq})
	if err != nil {
		return mapErr(err)
	}
	// As for operations: the headers mean Control admitted the stream; a
	// refusal arrives as the status of a response without headers.
	md, err := stream.Header()
	if err != nil {
		return mapErr(err)
	}
	if md == nil {
		if _, err := stream.Recv(); err != nil && !errors.Is(err, io.EOF) {
			return reset(err, afterSeq)
		}
		return nil
	}
	admitted()
	last := afterSeq
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return reset(err, last)
		}
		frame := toConversationFrame(msg.GetEvent())
		if err := emit(frame); err != nil {
			return err
		}
		last = frame.EventSeq
	}
}

// toConversationFrame flattens one conversation event into the public
// ConversationEventFrame: the type's members only, absent ones omitted.
func toConversationFrame(ev *controlv1.ConversationEvent) agentapi.ConversationEventFrame {
	f := agentapi.ConversationEventFrame{
		ConversationId: ev.GetConversationId(), EventSeq: ev.GetEventSeq(),
		EventType: agentapi.ConversationEventFrameEventType(ev.GetEventType()), OccurredAt: ev.GetOccurredAt().AsTime().UTC(),
	}
	role := func(r controlv1.MessageRole) *agentapi.MessageRole {
		v := agentapi.MessageRole(enumWord(r.String(), "MESSAGE_ROLE_"))
		return &v
	}
	switch p := ev.GetPayload().(type) {
	case *controlv1.ConversationEvent_MessageAccepted:
		m := p.MessageAccepted
		f.MessageId, f.MessageSeq, f.Role, f.TurnId = optional(m.GetMessageId()), optional(m.GetMessageSeq()), role(m.GetRole()), optional(m.GetTurnId())
	case *controlv1.ConversationEvent_MessageStarted:
		m := p.MessageStarted
		f.MessageId, f.TurnId = optional(m.GetMessageId()), optional(m.GetTurnId())
	case *controlv1.ConversationEvent_MessageDelta:
		m := p.MessageDelta
		text := m.GetText()
		f.MessageId, f.ChunkSeq, f.ContentRevision, f.Text = optional(m.GetMessageId()), optional(m.GetChunkSeq()), optional(m.GetContentRevision()), &text
	case *controlv1.ConversationEvent_MessageSealed:
		m := p.MessageSealed
		state := agentapi.MessageState(enumWord(m.GetState().String(), "MESSAGE_STATE_"))
		turnState := agentapi.TurnState(enumWord(m.GetTurnState().String(), "TURN_STATE_"))
		f.MessageId, f.State, f.ContentRevision, f.ThroughChunkSeq = optional(m.GetMessageId()), &state, optional(m.GetContentRevision()), optional(m.GetThroughChunkSeq())
		f.ContentDigest, f.TurnState, f.FailureCode = optional(m.GetContentDigest()), &turnState, m.FailureCode
	case *controlv1.ConversationEvent_TurnChanged:
		turn := toTurn(p.TurnChanged.GetTurn())
		f.Turn = &turn
	case *controlv1.ConversationEvent_ConversationChanged:
		m := p.ConversationChanged
		st := agentapi.ConversationStatus(enumWord(m.GetStatus().String(), "CONVERSATION_STATUS_"))
		f.ConversationRevision, f.Status = optional(m.GetConversationRevision()), &st
	}
	return f
}
