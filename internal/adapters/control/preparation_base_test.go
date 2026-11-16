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

// DEC-063, DEC-064: a preparation's base and a refinement's travel as the
// subject's refinement_base, apart from a release's source operation;
// without one the subject names only the profile and the intake's digest.
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
	require.Nil(t, stub.got[0].GetSubject().RefinementBase)
	require.Equal(t, "op_base", stub.got[1].GetSubject().GetRefinementBase().GetSourceOperationId())
	require.Equal(t, "4", stub.got[1].GetSubject().GetRefinementBase().GetSourceRevision())
	require.Nil(t, stub.got[1].GetSubject().SourceOperationId, "a base is not a release's source operation")
	require.Nil(t, stub.got[1].GetSubject().SourceRevision)
	require.Equal(t, controlv1.OperationKind_OPERATION_KIND_PREPARATION, stub.got[1].GetKind())
	require.Equal(t, "xfer_prompt", stub.got[1].GetPreparation().GetPrompt().GetTransferId())

	// A refinement states its base the same way; a release keeps its own fields.
	_, err = c.CreateOperation(context.Background(), application.CommandIdentity{}, application.Principal{}, "refinement",
		application.OperationSubject{ProfileID: "refinement-v1", SubjectDigest: "sha256:lineage", BriefID: "brf_1", BaseOperationID: "op_base", BaseRevision: "4"})
	require.NoError(t, err)
	_, err = c.CreateOperation(context.Background(), application.CommandIdentity{}, application.Principal{}, "release",
		application.OperationSubject{ProfileID: "release-v1", SubjectDigest: "sha256:lineage", SourceOperationID: "op_src", SourceRevision: "4", PackageVersion: "1.0.0"})
	require.NoError(t, err)
	require.Len(t, stub.got, 4)
	require.Equal(t, "op_base", stub.got[2].GetSubject().GetRefinementBase().GetSourceOperationId())
	require.Nil(t, stub.got[2].GetSubject().SourceOperationId)
	require.Nil(t, stub.got[3].GetSubject().RefinementBase)
	require.Equal(t, "op_src", stub.got[3].GetSubject().GetSourceOperationId())

	// The view states the base back.
	view := toView(&controlv1.OperationView{Subject: stub.got[2].GetSubject()})
	require.Equal(t, "op_base", view.BaseOperationID)
	require.Equal(t, "4", view.BaseRevision)
	require.Empty(t, view.SourceOperationID)
}
