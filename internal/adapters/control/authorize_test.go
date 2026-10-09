package control_test

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// scopedControl answers GetOperation by the project rule of Control (P0.3)
// and counts the tenant-only reads that reach it.
type scopedControl struct {
	controlv1.UnimplementedOperationServiceServer
	controlv1.UnimplementedPreviewServiceServer
	controlv1.UnimplementedReleaseServiceServer
	reads atomic.Int32
	roles []string
}

func (s *scopedControl) GetOperation(_ context.Context, req *controlv1.GetOperationRequest) (*controlv1.GetOperationResponse, error) {
	s.roles = req.GetScope().GetRoles()
	if req.GetScope().GetProjectId() != "proj_a" {
		return nil, status.Error(codes.NotFound, "NOT_FOUND")
	}
	return &controlv1.GetOperationResponse{Operation: &controlv1.OperationView{OperationId: req.GetOperationId()}}, nil
}

func (s *scopedControl) GetSource(context.Context, *controlv1.GetSourceRequest) (*controlv1.GetSourceResponse, error) {
	s.reads.Add(1)
	return nil, status.Error(codes.NotFound, "NOT_FOUND")
}

func (s *scopedControl) GetPreview(context.Context, *controlv1.GetPreviewRequest) (*controlv1.GetPreviewResponse, error) {
	s.reads.Add(1)
	return nil, status.Error(codes.NotFound, "NOT_FOUND")
}

func (s *scopedControl) GetRelease(context.Context, *controlv1.GetReleaseRequest) (*controlv1.GetReleaseResponse, error) {
	s.reads.Add(1)
	return nil, status.Error(codes.NotFound, "NOT_FOUND")
}

// P0.3 AC3: the source, preview and release reads name a tenant only; the
// adapter first reads the operation under the user's scope, so a user of
// another project of the tenant is refused before any of them is asked.
// The user's verified roles travel in that scope.
func TestTenantOnlyReadsAreAuthorizedByTheScopedRead(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	fake := &scopedControl{}
	controlv1.RegisterOperationServiceServer(srv, fake)
	controlv1.RegisterPreviewServiceServer(srv, fake)
	controlv1.RegisterReleaseServiceServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	c, err := control.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	ctx := context.Background()

	other := application.Principal{TenantID: "tenant_a", ProjectID: "proj_c", ActorID: "user_c", Roles: []string{"author"}}
	_, err = c.ReadSource(ctx, other, "op_1")
	requireCode(t, err, "NOT_FOUND")
	_, err = c.GetPreview(ctx, other, "op_1")
	requireCode(t, err, "NOT_FOUND")
	_, err = c.ReadPreviewArtifact(ctx, other, "op_1", "sha256:"+strings.Repeat("0", 64))
	requireCode(t, err, "NOT_FOUND")
	_, err = c.GetRelease(ctx, other, "op_1")
	requireCode(t, err, "NOT_FOUND")
	require.Zero(t, fake.reads.Load(), "no tenant-only read reached Control for another project's user")
	require.Equal(t, []string{"author"}, fake.roles, "the verified roles travel in the scope")

	own := application.Principal{TenantID: "tenant_a", ProjectID: "proj_a", ActorID: "user_a"}
	_, _ = c.ReadSource(ctx, own, "op_1")
	require.Equal(t, int32(1), fake.reads.Load(), "the project's own user reaches the read")
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	var ce *application.ControlError
	require.ErrorAs(t, err, &ce)
	require.Equal(t, code, ce.Code)
}
