package httpapi

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"

	controlv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1"
	"github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1/controlv1connect"
)

// disclosureProcedure is the fully qualified private method this service calls
// for authorization. It is recorded on the authenticated context as the
// destination the caller was authorized for; it is not a permission the API
// grants itself.
const disclosureProcedure = controlv1connect.ControlServiceGetDisclosureAuthorizationProcedure

// DisclosureAuthorizer is the narrow slice of Control this service calls for
// authorization. The generated Connect client satisfies it directly.
//
// Control owns the whole business-authorization decision and returns four facts
// and a deadline; the API receives no membership list, role string or upstream
// body, and it keeps no cache of its own. A protected read therefore obtains
// current evidence, and a stream renews it, rather than reusing a decision
// beyond the instant Control bound it to.
type DisclosureAuthorizer interface {
	GetDisclosureAuthorization(
		context.Context,
		*connect.Request[controlv1.GetDisclosureAuthorizationRequest],
	) (*connect.Response[controlv1.GetDisclosureAuthorizationResponse], error)
}

// disclosureGrant is one unexpired permission to disclose one operation.
type disclosureGrant struct {
	boundResourceScope string
	freshUntil         time.Time
	evidenceRevision   uint64
}

// usableAt reports whether the grant still authorizes disclosure at instant.
//
// The comparison subtracts the qualified maximum inter-service clock error, so
// a grant inside that margin of its expiry is treated as already expired. The
// margin is spent on the safe side: the API can be early, never late.
func (g disclosureGrant) usableAt(instant time.Time) bool {
	return instant.Add(config.ClockMaxInterServiceError).Before(g.freshUntil)
}

// renewAt reports when a stream should ask Control for new evidence. Renewal is
// bounded both by the grant's own expiry and by the contract's renewal cadence,
// so a long-lived grant is still revalidated on schedule.
func (g disclosureGrant) renewAt(issuedAt time.Time) time.Time {
	deadline := g.freshUntil.Add(-config.ClockMaxInterServiceError)
	cadence := issuedAt.Add(config.AuthorizationFreshness)
	if cadence.Before(deadline) {
		return cadence
	}
	return deadline
}

// errDisclosureExpired reports a grant that arrived already inside the clock
// margin. It is neither a denial nor a transport failure, so callers map it to
// the closed-failure outcome their surface defines.
var errDisclosureExpired = errors.New("httpapi: the disclosure grant arrived expired")

// authorizeDisclosure obtains current evidence that actor may read operationID.
//
// Every protected surface passes through here before it touches a projection,
// an event or a snapshot page. A negative decision denies; evidence that cannot
// be established fails closed as an unavailable dependency. Neither outcome
// tells the caller whether the operation exists.
func (s *Server) authorizeDisclosure(
	ctx context.Context,
	actor identity.Actor,
	operationID string,
) (disclosureGrant, *clientFault) {
	if s.disclosure == nil {
		return disclosureGrant{}, newFault(codeDependencyUnavailable, "the authorization dependency is not configured")
	}

	call, cancel := context.WithTimeout(ctx, s.controlCallTimeout)
	defer cancel()

	tenantID, actorID := actor.TenantID, actor.ActorID
	serviceIdentity, destinationMethod := logging.ServiceName, disclosureProcedure
	requestID := requestIDFrom(ctx)

	// The role is deliberately absent: the controlled identity profile
	// establishes an actor and its granted API actions, not a business role,
	// and a role this service invented would be exactly the "membership
	// implies action" inference the design forbids.
	request := connect.NewRequest(&controlv1.GetDisclosureAuthorizationRequest{
		Context: &controlv1.AuthenticatedContext{
			TenantId:          &tenantID,
			ActorId:           &actorID,
			ServiceIdentity:   &serviceIdentity,
			DestinationMethod: &destinationMethod,
			RequestId:         &requestID,
		},
		OperationId: &operationID,
	})
	if credential := actor.ControlCredential(); credential != "" {
		request.Header().Set("Authorization", "Bearer "+credential)
	}

	started := time.Now()
	response, err := s.disclosure.GetDisclosureAuthorization(call, request)
	elapsed := time.Since(started)
	if err != nil {
		code := connect.CodeOf(err)
		fault := disclosureFault(code)
		s.logControlCall(ctx, disclosureProcedure, elapsed, code.String(), outcomeForCode(code), fault)
		return disclosureGrant{}, fault
	}

	grant, evidenceErr := grantFrom(response.Msg, operationID)
	switch {
	case evidenceErr == nil:
		s.logControlCall(ctx, disclosureProcedure, elapsed, "ok", "ok", nil)
		return grant, nil
	case errors.Is(evidenceErr, errDisclosureDenied):
		fault := newFault(codePermissionDenied, "the authenticated actor may not read this operation")
		s.logControlCall(ctx, disclosureProcedure, elapsed, "ok", "denied", fault)
		return disclosureGrant{}, fault
	default:
		// Unusable evidence is not a denial and not the caller's problem, so it
		// fails closed as an unavailable dependency rather than as a decision
		// this service was never given.
		fault := newFault(codeDependencyUnavailable, "current authorization evidence could not be established")
		s.logControlCall(ctx, disclosureProcedure, elapsed, "ok", "error", fault)
		return disclosureGrant{}, fault
	}
}

// errDisclosureDenied reports evidence that was established and is negative.
var errDisclosureDenied = errors.New("httpapi: disclosure is denied for this actor and operation")

// grantFrom holds Control's answer to the disclosure contract before any byte
// is disclosed. A response that omits a fact, decides nothing, binds another
// resource or is already expired authorizes nothing.
func grantFrom(message *controlv1.GetDisclosureAuthorizationResponse, operationID string) (disclosureGrant, error) {
	if message == nil || message.Decision == nil {
		return disclosureGrant{}, errors.New("the evidence omits a decision")
	}
	switch *message.Decision {
	case controlv1.DisclosureDecision_DISCLOSURE_DECISION_ALLOW:
	case controlv1.DisclosureDecision_DISCLOSURE_DECISION_DENY:
		return disclosureGrant{}, errDisclosureDenied
	default:
		return disclosureGrant{}, errors.New("the evidence carries no decided decision")
	}

	// A decision for one resource is never a decision for another resource of
	// the same tenant, so the returned scope must name the operation asked for.
	if message.BoundResourceScope == nil || *message.BoundResourceScope != operationID {
		return disclosureGrant{}, errors.New("the evidence is bound to another resource scope")
	}
	if message.FreshUntil == nil || !message.FreshUntil.IsValid() {
		return disclosureGrant{}, errors.New("the evidence omits its expiry")
	}

	grant := disclosureGrant{
		boundResourceScope: *message.BoundResourceScope,
		freshUntil:         message.FreshUntil.AsTime(),
	}
	if message.EvidenceRevision == nil {
		return disclosureGrant{}, errors.New("the evidence omits its revision")
	}
	grant.evidenceRevision = *message.EvidenceRevision
	if !grant.usableAt(time.Now()) {
		return disclosureGrant{}, errDisclosureExpired
	}
	return grant, nil
}

// disclosureFault maps a transport failure. Control's own denial of this
// service's call is a dependency problem, not the browser caller's: the API is
// the client there, and reporting it as the caller's permission problem would
// misattribute a deployment fault.
func disclosureFault(code connect.Code) *clientFault {
	switch code {
	case connect.CodeResourceExhausted:
		return newFault(codeDependencyUnavailable, "the authorization dependency is shedding load")
	default:
		return newFault(codeDependencyUnavailable, "current authorization evidence could not be established")
	}
}
