package control

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// streamStub answers D4a's ConversationService reads: the snapshot, a
// streaming message, the event list (a reset past "10") and a stream that
// sends its events and then ends INVALID_ARGUMENT (the cursor overran).
type streamStub struct {
	controlv1.UnimplementedConversationServiceServer
	events []*controlv1.ConversationEvent
	lists  []string
}

func (s *streamStub) GetConversation(context.Context, *controlv1.GetConversationRequest) (*controlv1.GetConversationResponse, error) {
	return &controlv1.GetConversationResponse{
		Conversation:    &controlv1.Conversation{ConversationId: "cnv_1", Revision: "3", LastMessageSeq: "2", Status: controlv1.ConversationStatus_CONVERSATION_STATUS_ACTIVE},
		CoveredEventSeq: "9",
		ActiveTurn:      &controlv1.Turn{TurnId: "trn_1", State: controlv1.TurnState_TURN_STATE_RUNNING},
	}, nil
}

func (s *streamStub) GetMessage(_ context.Context, in *controlv1.GetMessageRequest) (*controlv1.GetMessageResponse, error) {
	return &controlv1.GetMessageResponse{Message: &controlv1.Message{MessageId: in.GetMessageId(), State: controlv1.MessageState_MESSAGE_STATE_STREAMING,
		Role: controlv1.MessageRole_MESSAGE_ROLE_ASSISTANT, Text: "Hel", ContentRevision: "1", ThroughChunkSeq: "1"}}, nil
}

func (s *streamStub) ListConversationEvents(_ context.Context, in *controlv1.ListConversationEventsRequest) (*controlv1.ListConversationEventsResponse, error) {
	s.lists = append(s.lists, in.GetAfterEventSeq())
	if in.GetAfterEventSeq() == "10" || in.GetAfterEventSeq() == "12" {
		return &controlv1.ListConversationEventsResponse{ResetRequired: true, CoveredEventSeq: "9"}, nil
	}
	return &controlv1.ListConversationEventsResponse{CoveredEventSeq: "9"}, nil
}

func (s *streamStub) StreamConversationEvents(_ *controlv1.StreamConversationEventsRequest, stream controlv1.ConversationService_StreamConversationEventsServer) error {
	if err := stream.SendHeader(metadata.Pairs("anvilkit-stream", "admitted")); err != nil {
		return err
	}
	for _, ev := range s.events {
		if err := stream.Send(&controlv1.StreamConversationEventsResponse{Event: ev}); err != nil {
			return err
		}
	}
	return status.Error(codes.InvalidArgument, "INVALID_ARGUMENT: the cursor cannot be honored")
}

// TestConversationStreamAdapter (D4a, DEC-070): the snapshot carries its
// covered event and active turn; a streaming message its revision; each
// event becomes the flat public frame with its type's members only; Control
// ending the stream INVALID_ARGUMENT is read back as a reset at the covered
// sequence; a cursor past the log is a reset before any stream.
func TestConversationStreamAdapter(t *testing.T) {
	at := timestamppb.New(time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC))
	reason := "CANCELED"
	stub := &streamStub{events: []*controlv1.ConversationEvent{
		{ConversationId: "cnv_1", EventSeq: "11", EventType: "message.delta", OccurredAt: at, Payload: &controlv1.ConversationEvent_MessageDelta{
			MessageDelta: &controlv1.MessageDeltaPayload{MessageId: "msg_2", ChunkSeq: "1", ContentRevision: "1", Text: "Hel"}}},
		{ConversationId: "cnv_1", EventSeq: "12", EventType: "message.canceled", OccurredAt: at, Payload: &controlv1.ConversationEvent_MessageSealed{
			MessageSealed: &controlv1.MessageSealedPayload{MessageId: "msg_2", State: controlv1.MessageState_MESSAGE_STATE_CANCELED, ContentRevision: "1",
				ThroughChunkSeq: "1", ContentDigest: "sha256:0dc7fa9db7237a2b5c96f70f59bb00f73bb86a0ca5554e91c312f9ada26e18b3",
				TurnState: controlv1.TurnState_TURN_STATE_CANCELED, FailureCode: &reason}}},
	}}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	controlv1.RegisterConversationServiceServer(server, stub)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///control", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := &Client{conn: conn}
	p := application.Principal{TenantID: "tenant_a", ProjectID: "project_a", ActorID: "user_alice"}

	conv, err := c.GetConversation(t.Context(), p, "cnv_1")
	require.NoError(t, err)
	require.Equal(t, "9", *conv.CoveredEventSeq)
	require.Equal(t, "trn_1", conv.ActiveTurn.TurnId)
	require.Equal(t, agentapi.TurnStateRunning, conv.ActiveTurn.State)

	m, err := c.GetMessage(t.Context(), p, "cnv_1", "msg_2")
	require.NoError(t, err)
	require.Equal(t, agentapi.MessageStateStreaming, m.State)
	require.Equal(t, "1", *m.ContentRevision)
	require.Equal(t, "1", *m.ThroughChunkSeq)

	var frames []agentapi.ConversationEventFrame
	admitted := false
	err = c.StreamConversationEvents(t.Context(), p, "cnv_1", "9", func() { admitted = true }, func(f agentapi.ConversationEventFrame) error {
		frames = append(frames, f)
		return nil
	})
	require.True(t, admitted)
	var reset *application.ResetRequired
	require.True(t, errors.As(err, &reset), "%v", err)
	require.Equal(t, "9", reset.CoveredEventSeq)
	require.Equal(t, []string{"9", "12"}, stub.lists, "the reset is read at the last delivered event")
	require.Len(t, frames, 2)
	delta := frames[0]
	require.Equal(t, agentapi.MessageDelta, delta.EventType)
	require.Equal(t, "Hel", *delta.Text)
	require.Equal(t, "1", *delta.ChunkSeq)
	require.Nil(t, delta.State, "a delta carries no seal members")
	sealed := frames[1]
	require.Equal(t, agentapi.MessageCanceled, sealed.EventType)
	require.Equal(t, agentapi.MessageStateCanceled, *sealed.State)
	require.Equal(t, agentapi.TurnStateCanceled, *sealed.TurnState)
	require.Equal(t, "CANCELED", *sealed.FailureCode)
	require.Nil(t, sealed.Text)

	err = c.StreamConversationEvents(t.Context(), p, "cnv_1", "10", func() { t.Fatal("no stream past the log") }, nil)
	require.True(t, errors.As(err, &reset))
	require.Equal(t, "9", reset.CoveredEventSeq)
}
