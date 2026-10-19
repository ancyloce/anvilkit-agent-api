package knowledge

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
)

// A search without maxContextItems (optional in the public contract) must
// not reach Knowledge as 0, which its request rule refuses.
func TestSearchRequestCapsContextItems(t *testing.T) {
	p := application.Principal{TenantID: "tenant_a", ActorID: "user_a"}
	req := agentapi.SearchRequest{SnapshotId: "snap_1", Query: "hero", RetrievalProfileId: "hybrid-v1"}
	in := searchRequest(p, req, time.Now().Add(time.Minute))
	require.Equal(t, uint32(defaultMaxContextItems), in.GetMaxContextItems())
	require.True(t, in.GetMaxContextItems() >= 1 && in.GetMaxContextItems() <= 64, "the default satisfies Knowledge's request rule (1..64)")

	eight := 8
	req.MaxContextItems = &eight
	require.Equal(t, uint32(8), searchRequest(p, req, time.Now().Add(time.Minute)).GetMaxContextItems())
}
