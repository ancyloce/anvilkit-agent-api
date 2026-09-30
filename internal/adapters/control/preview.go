package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-contracts/go/agentapi"
	controlv1 "github.com/ancyloce/anvilkit-agent-contracts/go/anvilkit/control/v1"
)

// Preview reads (P20). The preview is read under the principal's tenant;
// its artifact bytes come through Control's one-GET read capability of the
// exact object version, fetched without redirects and verified against
// the digest and size Control recorded before a byte is returned. The API
// holds no object-store credential and logs no capability.

// maxPreviewArtifact bounds one preview artifact (the contract's module
// bound).
const maxPreviewArtifact = 2 << 20

var previewFetch = &http.Client{Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

func mediaTypeOf(class string) agentapi.PreviewArtifactMediaType {
	if class == "css" {
		return agentapi.PreviewArtifactMediaType("text/css")
	}
	return agentapi.PreviewArtifactMediaType("text/javascript")
}

func toPreview(p *controlv1.Preview) agentapi.Preview {
	out := agentapi.Preview{
		OperationId: p.GetOperationId(), SubjectDigest: p.GetSubjectDigest(), BaseRevision: p.GetBaseRevision(),
		State: agentapi.PreviewState(enumWord(p.GetState().String(), "PREVIEW_STATE_")), SourceDigest: p.GetSourceDigest(),
		BuildProfileId: p.GetBuildProfileId(), HostProfileId: p.GetHostProfileId(), Revision: p.GetRevision(), UpdatedAt: p.GetUpdatedAt().AsTime().UTC(),
		Styles: []agentapi.PreviewArtifact{},
	}
	if p.SourceRevision != nil {
		v := p.GetSourceRevision()
		out.SourceRevision = &v
	}
	if p.CurrentRevision != nil {
		v := p.GetCurrentRevision()
		out.CurrentRevision = &v
	}
	if p.FailureCode != nil {
		v := p.GetFailureCode()
		out.FailureCode = &v
	}
	if m := p.GetModule(); m != nil {
		out.Module = &agentapi.PreviewArtifact{Digest: m.GetDigest(), SizeBytes: m.GetSizeBytes(), MediaType: mediaTypeOf(m.GetClass())}
	}
	for _, s := range p.GetStyles() {
		out.Styles = append(out.Styles, agentapi.PreviewArtifact{Digest: s.GetDigest(), SizeBytes: s.GetSizeBytes(), MediaType: mediaTypeOf(s.GetClass())})
	}
	return out
}

func (c *Client) preview(ctx context.Context, p application.Principal, operationID string) (*controlv1.Preview, error) {
	resp, err := controlv1.NewPreviewServiceClient(c.conn).GetPreview(ctx, &controlv1.GetPreviewRequest{TenantId: p.TenantID, OperationId: operationID})
	if err != nil {
		return nil, mapErr(err)
	}
	return resp.GetPreview(), nil
}

func (c *Client) GetPreview(ctx context.Context, p application.Principal, operationID string) (application.PreviewView, error) {
	pv, err := c.preview(ctx, p, operationID)
	if err != nil {
		return application.PreviewView{}, err
	}
	return toPreview(pv), nil
}

func (c *Client) ReadPreviewArtifact(ctx context.Context, p application.Principal, operationID, digest string) (application.PreviewBytes, error) {
	pv, err := c.preview(ctx, p, operationID)
	if err != nil {
		return application.PreviewBytes{}, err
	}
	var ref *controlv1.ArtifactReference
	for _, a := range append([]*controlv1.ArtifactReference{pv.GetModule()}, pv.GetStyles()...) {
		if a != nil && a.GetDigest() == digest {
			ref = a
		}
	}
	if ref == nil {
		return application.PreviewBytes{}, &application.ControlError{Code: "NOT_FOUND", Message: "no artifact of this preview has that digest"}
	}
	size, err := strconv.ParseInt(ref.GetSizeBytes(), 10, 64)
	if err != nil || size < 0 || size > maxPreviewArtifact {
		return application.PreviewBytes{}, &application.ControlError{Code: "INVALID_ARGUMENT", Message: "preview artifact exceeds the bound"}
	}
	body, err := c.readVerified(ctx, ref.GetHandle(), operationID, digest, size)
	if err != nil {
		return application.PreviewBytes{}, err
	}
	return application.PreviewBytes{MediaType: string(mediaTypeOf(ref.GetClass())), Body: body}, nil
}

// maxSourceArchive bounds one source archive the API serves.
const maxSourceArchive = 16 << 20

func (c *Client) ReadSource(ctx context.Context, p application.Principal, operationID string) (application.SourceBytes, error) {
	resp, err := controlv1.NewPreviewServiceClient(c.conn).GetSource(ctx, &controlv1.GetSourceRequest{TenantId: p.TenantID, OperationId: operationID})
	if err != nil {
		return application.SourceBytes{}, mapErr(err)
	}
	src := resp.GetSource()
	size, err := strconv.ParseInt(src.GetSizeBytes(), 10, 64)
	if err != nil || size < 0 || size > maxSourceArchive {
		return application.SourceBytes{}, &application.ControlError{Code: "INVALID_ARGUMENT", Message: "source archive exceeds the bound"}
	}
	body, err := c.readVerified(ctx, src.GetHandle(), operationID, src.GetDigest(), size)
	if err != nil {
		return application.SourceBytes{}, err
	}
	return application.SourceBytes{Lineage: resp.GetLineage(), Revision: resp.GetRevision(), Digest: src.GetDigest(), Body: body}, nil
}

// readVerified fetches one artifact through Control's one-GET read
// capability (no redirects) and returns its bytes only when they have the
// recorded size and digest.
func (c *Client) readVerified(ctx context.Context, handle, operationID, digest string, size int64) ([]byte, error) {
	read, err := c.artifacts.ReadArtifact(ctx, &controlv1.ReadArtifactRequest{Handle: handle, OperationId: operationID})
	if err != nil {
		return nil, mapErr(err)
	}
	dl := read.GetDownload()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dl.GetUrl(), nil)
	if err != nil {
		return nil, &application.ControlError{Code: "DEPENDENCY_UNAVAILABLE", Message: "artifact capability unusable", Retryable: true}
	}
	for k, v := range dl.GetHeaders() {
		req.Header.Set(k, v)
	}
	resp, err := previewFetch.Do(req)
	if err != nil {
		return nil, &application.ControlError{Code: "DEPENDENCY_UNAVAILABLE", Message: "artifact store unreachable", Retryable: true}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, size+1))
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil, &application.ControlError{Code: "DEPENDENCY_UNAVAILABLE", Message: fmt.Sprintf("artifact read answered %d", resp.StatusCode), Retryable: true}
	}
	sum := sha256.Sum256(body)
	if int64(len(body)) != size || "sha256:"+hex.EncodeToString(sum[:]) != digest {
		return nil, &application.ControlError{Code: "EFFECT_UNCERTAIN", Message: "artifact bytes differ from the recorded digest or size", Retryable: false}
	}
	return body, nil
}
