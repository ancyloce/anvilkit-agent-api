package control

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

// createStub answers CreateOperation and keeps the requests.
type createStub struct {
	controlv1.OperationServiceClient
	got []*controlv1.CreateOperationRequest
}

func (s *createStub) CreateOperation(_ context.Context, in *controlv1.CreateOperationRequest, _ ...grpc.CallOption) (*controlv1.CreateOperationResponse, error) {
	s.got = append(s.got, in)
	return &controlv1.CreateOperationResponse{Operation: &controlv1.OperationView{OperationId: "op_1"}}, nil
}

// DEC-063: a preparation's base travels in the operation subject, as a
// refinement's and a release's source do; without one the subject names
// only the profile and the intake's digest.
func TestCreatePreparationNamesItsBaseInTheSubject(t *testing.T) {
	stub := &createStub{}
	c := &Client{ops: stub}
	intake := application.PreparationIntake{PromptTransferID: "xfer_prompt", PromptDigest: "sha256:aa"}
	_, err := c.CreatePreparation(context.Background(), application.CommandIdentity{}, application.Principal{}, "preparation-v1", "sha256:intake", intake)
	require.NoError(t, err)
	intake.BaseOperationID, intake.BaseRevision = "op_base", "4"
	_, err = c.CreatePreparation(context.Background(), application.CommandIdentity{}, application.Principal{}, "preparation-v1", "sha256:based", intake)
	require.NoError(t, err)
	require.Len(t, stub.got, 2)
	require.Nil(t, stub.got[0].GetSubject().SourceOperationId)
	require.Nil(t, stub.got[0].GetSubject().SourceRevision)
	require.Equal(t, "op_base", stub.got[1].GetSubject().GetSourceOperationId())
	require.Equal(t, "4", stub.got[1].GetSubject().GetSourceRevision())
	require.Equal(t, controlv1.OperationKind_OPERATION_KIND_PREPARATION, stub.got[1].GetKind())
	require.Equal(t, "xfer_prompt", stub.got[1].GetPreparation().GetPrompt().GetTransferId())
}
