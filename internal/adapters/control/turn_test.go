package control

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// turnStub answers GetTurn and StopTurn of ConversationService and keeps the
// requests.
type turnStub struct {
	controlv1.UnimplementedConversationServiceServer
	get  *controlv1.GetTurnRequest
	stop *controlv1.StopTurnRequest
	turn *controlv1.Turn
}

func (s *turnStub) GetTurn(_ context.Context, in *controlv1.GetTurnRequest) (*controlv1.GetTurnResponse, error) {
	s.get = in
	if in.GetTurnId() == "trn_other" {
		return nil, status.Error(codes.NotFound, "turn trn_other")
	}
	return &controlv1.GetTurnResponse{Turn: s.turn}, nil
}

func (s *turnStub) StopTurn(_ context.Context, in *controlv1.StopTurnRequest) (*controlv1.StopTurnResponse, error) {
	s.stop = in
	return &controlv1.StopTurnResponse{Turn: s.turn, Existing: true}, nil
}

// TestTurnViews (DEC-068): the turn's enums become the public words, its
// optional assistant message and failure code stay optional, its times are
// UTC; the stop carries the derived command and the principal's scope, and
// Control's NOT_FOUND stays not found.
func TestTurnViews(t *testing.T) {
	code := "CONTENT_UNRECOVERABLE"
	reply := "msg_2"
	stub := &turnStub{turn: &controlv1.Turn{
		TurnId: "trn_1", ConversationId: "cnv_1", InputMessageId: "msg_1", AssistantMessageId: &reply, OperationId: "op_1",
		State: controlv1.TurnState_TURN_STATE_FAILED, ContentStatus: controlv1.ContentStatus_CONTENT_STATUS_PARTIAL,
		SenderStatus: controlv1.SenderStatus_SENDER_STATUS_STOPPED, Finance: controlv1.FinanceState_FINANCE_STATE_EXPOSURE_UNKNOWN, FailureCode: &code,
		CreatedAt: timestamppb.New(time.Date(2026, 11, 20, 1, 0, 0, 0, time.FixedZone("x", 3600))), UpdatedAt: timestamppb.New(time.Date(2026, 11, 20, 1, 2, 0, 0, time.UTC)),
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

	got, err := c.GetTurn(t.Context(), p, "cnv_1", "trn_1")
	require.NoError(t, err)
	require.Equal(t, agentapi.Turn{
		TurnId: "trn_1", ConversationId: "cnv_1", InputMessageId: "msg_1", AssistantMessageId: &reply, OperationId: "op_1",
		State: agentapi.TurnStateFailed, ContentStatus: agentapi.ContentStatusPartial, SenderStatus: agentapi.SenderStatusStopped,
		Finance: agentapi.FinanceState("exposure_unknown"), FailureCode: &code,
		CreatedAt: time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC), UpdatedAt: time.Date(2026, 11, 20, 1, 2, 0, 0, time.UTC),
	}, got)
	require.Equal(t, "cnv_1", stub.get.GetConversationId())
	require.Equal(t, &controlv1.Scope{TenantId: "tenant_a", ProjectId: "project_a", ActorId: "user_alice"}, stub.get.GetScope())

	cmd := application.CommandIdentity{TenantID: "tenant_a", CommandID: "stop-1", ActorID: "user_alice", RequestDigest: "sha256:aa"}
	stopped, err := c.StopTurn(t.Context(), cmd, p, "cnv_1", "trn_1")
	require.NoError(t, err)
	require.Equal(t, got, stopped, "a replayed stop answers the turn as it stands")
	require.Equal(t, &controlv1.CommandIdentity{TenantId: "tenant_a", CommandId: "stop-1", ActorId: "user_alice", RequestDigest: "sha256:aa"}, stub.stop.GetCommand())
	require.Equal(t, "trn_1", stub.stop.GetTurnId())

	stub.turn.AssistantMessageId, stub.turn.FailureCode = nil, nil
	got, err = c.GetTurn(t.Context(), p, "cnv_1", "trn_1")
	require.NoError(t, err)
	require.Nil(t, got.AssistantMessageId, "a turn that ended before any text names no message")
	require.Nil(t, got.FailureCode)

	_, err = c.GetTurn(t.Context(), p, "cnv_1", "trn_other")
	var ce *application.ControlError
	require.ErrorAs(t, err, &ce)
	require.Equal(t, "NOT_FOUND", ce.Code)
}
