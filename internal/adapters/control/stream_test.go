package control_test

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// streamingControl answers StreamOperationEvents the way Control does: it
// refuses before any header (its stream caps), or sends the response
// headers on admission and then the events.
type streamingControl struct {
	controlv1.UnimplementedOperationServiceServer
	refuse bool
}

func (s *streamingControl) ListOperationEvents(context.Context, *controlv1.ListOperationEventsRequest) (*controlv1.ListOperationEventsResponse, error) {
	return &controlv1.ListOperationEventsResponse{}, nil
}

func (s *streamingControl) StreamOperationEvents(req *controlv1.StreamOperationEventsRequest, stream controlv1.OperationService_StreamOperationEventsServer) error {
	if s.refuse {
		return status.Error(codes.ResourceExhausted, "streams of the tenant")
	}
	if err := stream.SendHeader(metadata.Pairs("anvilkit-stream", "admitted")); err != nil {
		return err
	}
	return stream.Send(&controlv1.StreamOperationEventsResponse{Event: &controlv1.OperationEvent{OperationId: req.GetOperationId(), EventSeq: "1", EventType: "operation.changed", Revision: "1"}})
}

func dialStreaming(t *testing.T, fake *streamingControl) *control.Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	controlv1.RegisterOperationServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := control.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// D-7: a stream Control refuses is the capacity error, and the caller is
// never told it was admitted.
func TestAStreamControlRefusesIsNeverAdmitted(t *testing.T) {
	c := dialStreaming(t, &streamingControl{refuse: true})
	admitted := false
	err := c.StreamEvents(context.Background(), application.Principal{TenantID: "tenant_a", ProjectID: "proj_a", ActorID: "user_a"}, "op_1", "",
		func() { admitted = true }, func(application.EventFrame) error { return nil })
	requireCode(t, err, "CAPACITY_EXHAUSTED")
	require.False(t, admitted)
}

// D-7: admission is reported once Control sent its headers, before the
// first frame.
func TestAdmissionIsReportedBeforeTheFirstFrame(t *testing.T) {
	c := dialStreaming(t, &streamingControl{})
	var mu sync.Mutex
	var order []string
	err := c.StreamEvents(context.Background(), application.Principal{TenantID: "tenant_a", ProjectID: "proj_a", ActorID: "user_a"}, "op_1", "",
		func() { mu.Lock(); order = append(order, "admitted"); mu.Unlock() },
		func(f application.EventFrame) error {
			mu.Lock()
			order = append(order, "frame "+f.EventSeq)
			mu.Unlock()
			return nil
		})
	require.NoError(t, err)
	require.Equal(t, []string{"admitted", "frame 1"}, order)
}
