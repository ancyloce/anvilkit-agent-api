package control

import (
	"context"

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
	return toConversation(resp.GetConversation()), nil
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
