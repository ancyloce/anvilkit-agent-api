package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"connectrpc.com/connect"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"

	controlv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1"
	"github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1/controlv1connect"
)

// The command routes and the actions they require, from the Agent OpenAPI's
// x-anvilkit-required-action.
const (
	routeLocalChecks       = "/v1/local-checks"
	routeOperationCancel   = "/v1/operations/{operationId}/cancel"
	routeOperationHold     = "/v1/operations/{operationId}/hold"
	routeOperationResume   = "/v1/operations/{operationId}/resume"
	routeDefinitionChanges = "/v1/operations/definition-changes"
	actionLocalCheckCreate = "local-check.create"
	actionOperationCancel  = "operation.cancel"
)

// The private methods these routes forward to.
const (
	admitOperationProcedure = controlv1connect.ControlServiceAdmitOperationProcedure
	cancelProcedure         = controlv1connect.ControlServiceCancelProcedure
)

// controlCommandBodyMaxBytes is the x-anvilkit-max-body-bytes ceiling the Agent
// OpenAPI declares for the reserved control-lane commands. It is a per-method
// override of ingress.commandBodyMaxBytes, not a separate pilot limit.
const controlCommandBodyMaxBytes = 4096

// OperationCommander is the narrow slice of Control the public command routes
// call. The generated ControlService client satisfies it directly.
//
// Only two methods appear here, and each one backs exactly one declared public
// operation. The API decides nothing either method decides: Control computes
// the request digest, owns idempotency, admits or refuses capacity, and is the
// sole authority for a fence. This service converts values and reports what it
// was told.
type OperationCommander interface {
	AdmitOperation(
		context.Context,
		*connect.Request[controlv1.AdmitOperationRequest],
	) (*connect.Response[controlv1.AdmitOperationResponse], error)
	Cancel(
		context.Context,
		*connect.Request[controlv1.ControlCommandRequest],
	) (*connect.Response[controlv1.ControlCommandResponse], error)
}

var (
	localCheckCommandMembers = []string{"commandId", "fixtureId"}
	controlCommandMembers    = []string{"commandId", "expectedOperationRevision"}
	// localCheckFixtureIds is urn:anvilkit:agent-enums:v1#/$defs/localCheckFixtureId.
	// The two retained fixtures are the only accepted input: no text, code, path
	// or URL reaches Control through this route.
	localCheckFixtureIds = map[string]struct{}{"plain-v1": {}, "newline-v1": {}}
	// reasonCodePattern is urn:anvilkit:agent-enums:v1#/$defs/reasonCode.
	reasonCodePattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{1,63}$`)
)

// localCheckCommand is the LocalCheckCommand request body: exactly these two
// members, and nothing that could name an actor, a tenant or an operation.
type localCheckCommand struct {
	CommandID string `json:"commandId"`
	FixtureID string `json:"fixtureId"`
}

// operationAccepted is the OperationAccepted response body. Optional members
// are omitted rather than emptied, and requestDigest is the server-computed
// semantic digest the idempotency key resolves against.
type operationAccepted struct {
	OperationID       string `json:"operationId"`
	OperationRevision string `json:"operationRevision"`
	AcceptedAt        string `json:"acceptedAt"`
	RequestDigest     string `json:"requestDigest,omitempty"`
	QueueExpiresAt    string `json:"queueExpiresAt,omitempty"`
	Existing          bool   `json:"existing"`
	// Server-bound at intake for a preparation (test billing); absent for a
	// local-check, which carries no authority and no quote.
	FundingAuthority     string `json:"fundingAuthority,omitempty"`
	AuthorizedFundingRef string `json:"authorizedFundingRef,omitempty"`
}

// controlCommand is the ControlCommand request body of the reserved lane.
type controlCommand struct {
	CommandID                 string `json:"commandId"`
	ExpectedOperationRevision string `json:"expectedOperationRevision"`
	ReasonCode                string `json:"reasonCode,omitempty"`
}

// controlCommandResult is the ControlCommandResult response body.
type controlCommandResult struct {
	OperationID         string `json:"operationId"`
	OperationRevision   string `json:"operationRevision"`
	ControlState        string `json:"controlState"`
	Status              string `json:"status"`
	ReasonCode          string `json:"reasonCode,omitempty"`
	FenceAcknowledgedAt string `json:"fenceAcknowledgedAt,omitempty"`
	Coalesced           bool   `json:"coalesced"`
	TrackedCommandID    string `json:"trackedCommandId"`
}

// handleSubmitLocalCheck serves POST /v1/local-checks.
//
// The route exists only in the controlled local profile; where it is not
// registered it is absent rather than denied, so no body, header or credential
// can reach it. Acceptance requires Control's durable confirmation: this
// handler never acknowledges an operation it was not told is durable, and it
// derives no outcome from anything other than Control's answer.
func (s *Server) handleSubmitLocalCheck(w http.ResponseWriter, r *http.Request) {
	actor, fault := s.authorizeAction(r, actionLocalCheckCreate)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	command, fault := decodeLocalCheckCommand(r)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	accepted, fault := s.forwardLocalCheck(r.Context(), actor, command)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}
	// A replay of an already accepted command is not a second acceptance, so it
	// answers 200 with the original identity and revision rather than 202.
	status := http.StatusAccepted
	if accepted.Existing {
		status = http.StatusOK
	} else {
		w.Header().Set("Location", "/v1/operations/"+accepted.OperationID)
	}
	recordOutcome(r.Context(), status, "", "")
	writeJSON(w, status, accepted)
}

// handleUnsupportedLocalControlCommand rejects commands excluded by the fixed
// local profile without reading or changing any operation.
func (s *Server) handleUnsupportedLocalControlCommand(w http.ResponseWriter, r *http.Request) {
	if operationID := r.PathValue("operationId"); isIdentifier(operationID) {
		recordOperation(r.Context(), operationID)
	}
	s.writeError(w, r, newFault(codeChangeBlocked, "the controlled local profile does not support hold, resume or definition changes"))
}

// handleCancelOperation serves POST /v1/operations/{operationId}/cancel.
//
// Cancellation travels the reserved control-command lane: it stays available
// while business capacity is occupied, because nothing in this handler consults
// an intake bound. It fences new dispatch and recalls nothing already issued.
func (s *Server) handleCancelOperation(w http.ResponseWriter, r *http.Request) {
	actor, fault := s.authorizeAction(r, actionOperationCancel)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	operationID := r.PathValue("operationId")
	if !isIdentifier(operationID) {
		s.writeError(w, r, newFault(codeInvalidArgument, "the operation identifier is not a values-v1 identifier"))
		return
	}
	recordOperation(r.Context(), operationID)

	command, fault := decodeControlCommand(r)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	result, fault := s.forwardCancel(r.Context(), actor, operationID, command)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	recordOutcome(r.Context(), http.StatusAccepted, "", "")
	writeJSON(w, http.StatusAccepted, result)
}

// authorizeAction resolves the trusted caller and confirms it holds action.
// The two identity outcomes stay distinct: an unresolved credential is never
// reported as a permission problem, and a resolved actor missing the action is
// never reported as an authentication problem.
func (s *Server) authorizeAction(r *http.Request, action string) (identity.Actor, *clientFault) {
	actor, err := s.identities.Authorize(r.Header.Get("Authorization"), action)
	if err != nil {
		if errors.Is(err, identity.ErrPermissionDenied) {
			return identity.Actor{}, newFault(codePermissionDenied, "the authenticated actor does not hold "+action)
		}
		return identity.Actor{}, newFault(codeUnauthenticated, "the request carries no accepted platform identity")
	}
	recordActor(r.Context(), actor)
	return actor, nil
}

func decodeLocalCheckCommand(r *http.Request) (*localCheckCommand, *clientFault) {
	body, fault := readBoundedBody(r, config.LocalCheckBodyMaxBytes)
	if fault != nil {
		return nil, fault
	}
	members, fault := scanStrictJSON(body)
	if fault != nil {
		return nil, fault
	}
	if fault := requireExactMembers(members, localCheckCommandMembers); fault != nil {
		return nil, fault
	}

	var command localCheckCommand
	if err := json.Unmarshal(body, &command); err != nil {
		return nil, memberTypeFault(err)
	}
	if !isIdentifier(command.CommandID) {
		return nil, newFault(codeInvalidArgument, "commandId must be a values-v1 identifier")
	}
	if _, retained := localCheckFixtureIds[command.FixtureID]; !retained {
		return nil, newFault(codeInvalidArgument, "fixtureId must name a retained local-check fixture")
	}
	return &command, nil
}

func decodeControlCommand(r *http.Request) (*controlCommand, *clientFault) {
	body, fault := readBoundedBody(r, controlCommandBodyMaxBytes)
	if fault != nil {
		return nil, fault
	}
	members, fault := scanStrictJSON(body)
	if fault != nil {
		return nil, fault
	}
	if fault := requireMembers(members, controlCommandMembers, []string{"reasonCode"}); fault != nil {
		return nil, fault
	}

	var command controlCommand
	if err := json.Unmarshal(body, &command); err != nil {
		return nil, memberTypeFault(err)
	}
	if !isIdentifier(command.CommandID) {
		return nil, newFault(codeInvalidArgument, "commandId must be a values-v1 identifier")
	}
	if _, err := parseEventSeq(command.ExpectedOperationRevision); err != nil {
		return nil, newFault(codeInvalidArgument, "expectedOperationRevision must be a bounded unsigned decimal counter")
	}
	if command.ReasonCode != "" && !reasonCodePattern.MatchString(command.ReasonCode) {
		return nil, newFault(codeInvalidArgument, "reasonCode must be a registered reason-code token")
	}
	return &command, nil
}

// forwardLocalCheck performs the private admission call and converts its
// acceptance. The kind, intake source and fixture are the only values this
// service supplies; the caller's identity comes from the authenticated context,
// never from the body.
func (s *Server) forwardLocalCheck(
	ctx context.Context,
	actor identity.Actor,
	command *localCheckCommand,
) (*operationAccepted, *clientFault) {
	kind := controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK
	intakeSource := controlv1.IntakeSource_INTAKE_SOURCE_API
	message := &controlv1.AdmitOperationRequest{
		Context:             s.authenticatedContext(ctx, actor, admitOperationProcedure),
		CommandId:           &command.CommandID,
		Kind:                &kind,
		IntakeSource:        &intakeSource,
		LocalCheckFixtureId: &command.FixtureID,
	}

	call, cancel := context.WithTimeout(ctx, s.controlCallTimeout)
	defer cancel()

	started := time.Now()
	request := connect.NewRequest(message)
	if credential := actor.ControlCredential(); credential != "" {
		request.Header().Set("Authorization", "Bearer "+credential)
	}
	response, err := s.commands.AdmitOperation(call, request)
	elapsed := time.Since(started)

	if err != nil {
		code := connect.CodeOf(err)
		fault := localCheckFault(code)
		s.logControlCall(ctx, admitOperationProcedure, elapsed, code.String(), outcomeForCode(code), fault)
		return nil, fault
	}

	accepted, contractErr := acceptedFrom(response.Msg)
	if contractErr != nil {
		// An acceptance this service cannot hold to the contract is not an
		// acknowledgement. Reporting it as a dependency failure keeps the
		// caller's retry honest: the operation may or may not exist, and only
		// the same command can resolve that.
		fault := newFault(codeDependencyUnavailable, "the intake dependency returned an acceptance outside its contract")
		s.logControlCall(ctx, admitOperationProcedure, elapsed, "ok", "error", fault)
		return nil, fault
	}

	recordOperation(ctx, accepted.OperationID)
	s.logControlCall(ctx, admitOperationProcedure, elapsed, "ok", "ok", nil)
	return accepted, nil
}

// forwardCancel performs the private cancellation call and converts its result.
func (s *Server) forwardCancel(
	ctx context.Context,
	actor identity.Actor,
	operationID string,
	command *controlCommand,
) (*controlCommandResult, *clientFault) {
	expectedRevision, err := parseEventSeq(command.ExpectedOperationRevision)
	if err != nil {
		return nil, newFault(codeInvalidArgument, "expectedOperationRevision must be a bounded unsigned decimal counter")
	}
	message := &controlv1.ControlCommandRequest{
		Context:                   s.authenticatedContext(ctx, actor, cancelProcedure),
		CommandId:                 &command.CommandID,
		OperationId:               &operationID,
		ExpectedOperationRevision: &expectedRevision,
	}
	if command.ReasonCode != "" {
		message.ReasonCode = &command.ReasonCode
	}

	call, cancel := context.WithTimeout(ctx, s.controlCallTimeout)
	defer cancel()

	started := time.Now()
	request := connect.NewRequest(message)
	if credential := actor.ControlCredential(); credential != "" {
		request.Header().Set("Authorization", "Bearer "+credential)
	}
	response, callErr := s.commands.Cancel(call, request)
	elapsed := time.Since(started)

	if callErr != nil {
		code := connect.CodeOf(callErr)
		fault := controlCommandFault(code)
		s.logControlCall(ctx, cancelProcedure, elapsed, code.String(), outcomeForCode(code), fault)
		return nil, fault
	}

	result, contractErr := commandResultFrom(response.Msg, operationID)
	if contractErr != nil {
		fault := newFault(codeDependencyUnavailable, "the command dependency returned a result outside its contract")
		s.logControlCall(ctx, cancelProcedure, elapsed, "ok", "error", fault)
		return nil, fault
	}

	s.logControlCall(ctx, cancelProcedure, elapsed, "ok", "ok", nil)
	return result, nil
}

// authenticatedContext builds the destination-level identity a private call
// carries. The role is deliberately absent for the same reason it is absent
// from the disclosure call: the controlled profile establishes an actor and its
// granted API actions, not a business role.
func (s *Server) authenticatedContext(ctx context.Context, actor identity.Actor, procedure string) *controlv1.AuthenticatedContext {
	tenantID, actorID := actor.TenantID, actor.ActorID
	serviceIdentity, destinationMethod := logging.ServiceName, procedure
	requestID := requestIDFrom(ctx)
	return &controlv1.AuthenticatedContext{
		TenantId:          &tenantID,
		ActorId:           &actorID,
		ServiceIdentity:   &serviceIdentity,
		DestinationMethod: &destinationMethod,
		RequestId:         &requestID,
	}
}

// acceptedFrom converts Control's acceptance and holds it to the response
// contract, so this service never emits a body the public schema would reject
// and never invents an identity, revision or digest it was not given.
func acceptedFrom(message *controlv1.AdmitOperationResponse) (*operationAccepted, error) {
	if message == nil {
		return nil, errors.New("the acceptance is empty")
	}
	if message.OperationId == nil || !isIdentifier(*message.OperationId) {
		return nil, errors.New("the acceptance omits or malforms operationId")
	}
	if message.OperationRevision == nil {
		return nil, errors.New("the acceptance omits operationRevision")
	}
	if message.AcceptedAt == nil || !message.AcceptedAt.IsValid() {
		return nil, errors.New("the acceptance omits or malforms acceptedAt")
	}
	if message.QueueExpiresAt == nil || !message.QueueExpiresAt.IsValid() || !message.QueueExpiresAt.AsTime().After(message.AcceptedAt.AsTime()) {
		return nil, errors.New("the local acceptance omits or malforms its original queue deadline")
	}
	// The local-check body carries the digest the idempotency key resolves
	// against; without it a caller cannot tell a replay from a fresh identity.
	if message.RequestDigest == nil || !digestPattern.MatchString(*message.RequestDigest) {
		return nil, errors.New("the acceptance omits or malforms requestDigest")
	}
	if message.Existing == nil {
		return nil, errors.New("the acceptance omits existing")
	}

	accepted := &operationAccepted{
		OperationID:       *message.OperationId,
		OperationRevision: strconv.FormatUint(*message.OperationRevision, 10),
		AcceptedAt:        instant(message.AcceptedAt.AsTime()),
		RequestDigest:     *message.RequestDigest,
		Existing:          *message.Existing,
	}
	if message.QueueExpiresAt != nil {
		if !message.QueueExpiresAt.IsValid() {
			return nil, errors.New("the acceptance malforms queueExpiresAt")
		}
		accepted.QueueExpiresAt = instant(message.QueueExpiresAt.AsTime())
	}
	return accepted, nil
}

// commandResultFrom converts Control's tracked command state. A result bound to
// another operation is refused rather than relabelled.
func commandResultFrom(message *controlv1.ControlCommandResponse, operationID string) (*controlCommandResult, error) {
	if message == nil {
		return nil, errors.New("the result is empty")
	}
	if message.OperationId == nil || *message.OperationId != operationID {
		return nil, errors.New("the result is bound to another operation")
	}
	if message.OperationRevision == nil {
		return nil, errors.New("the result omits operationRevision")
	}
	controlState, known := controlStateNames[message.GetControlState()]
	if !known {
		return nil, errors.New("the result carries no decided control state")
	}
	status, known := publicStatusNames[message.GetStatus()]
	if !known {
		return nil, errors.New("the result carries no decided public status")
	}
	if message.Coalesced == nil {
		return nil, errors.New("the result omits coalesced")
	}
	if message.TrackedCommandId == nil || !isIdentifier(*message.TrackedCommandId) {
		return nil, errors.New("the result omits or malforms trackedCommandId")
	}

	result := &controlCommandResult{
		OperationID:       operationID,
		OperationRevision: strconv.FormatUint(*message.OperationRevision, 10),
		ControlState:      controlState,
		Status:            status,
		Coalesced:         *message.Coalesced,
		TrackedCommandID:  *message.TrackedCommandId,
	}
	if message.ReasonCode != nil {
		if !reasonCodePattern.MatchString(*message.ReasonCode) {
			return nil, errors.New("the result malforms reasonCode")
		}
		result.ReasonCode = *message.ReasonCode
	}
	if message.FenceAcknowledgedAt != nil {
		if !message.FenceAcknowledgedAt.IsValid() {
			return nil, errors.New("the result malforms fenceAcknowledgedAt")
		}
		result.FenceAcknowledgedAt = instant(message.FenceAcknowledgedAt.AsTime())
	}
	return result, nil
}

// localCheckFault maps an admission failure onto the public envelope.
//
// Two families are recoverable from the RPC status on this route alone. ABORTED
// has exactly one meaning here, because the route creates an operation and
// carries no expected revision: the same commandId was accepted for a different
// fixture. RESOURCE_EXHAUSTED likewise has one meaning, because the controlled
// local profile has a single capacity bound, so the overload reason the
// envelope requires is the occupied intake slot rather than a guess.
func localCheckFault(code connect.Code) *clientFault {
	switch code {
	case connect.CodePermissionDenied:
		return newFault(codePermissionDenied, "the current fixture scope does not permit local-check creation")
	case connect.CodeInvalidArgument:
		return newFault(codeInvalidArgument, "the local-check command was rejected by admission")
	case connect.CodeAborted:
		return newFault(codeIdempotencyConflict,
			"this command identifier was accepted for a different fixture")
	case connect.CodeResourceExhausted:
		return overloadFault("an unresolved local-check already occupies the single local slot", overloadReasonQueueFull)
	case connect.CodeFailedPrecondition:
		return newFault(codeProfileQualification, "the controlled local profile does not admit this command")
	default:
		// Everything else, a timeout included, leaves intake unknown. It is
		// never an acceptance, and it never proves that no operation exists.
		return newFault(codeDependencyUnavailable, "the intake dependency is unavailable")
	}
}

// controlCommandFault maps a reserved-lane failure onto the public envelope.
//
// The private contract signals ABORTED for REVISION_CONFLICT,
// IDEMPOTENCY_CONFLICT and OPERATION_TERMINAL alike, and carries no code on the
// error path, so the family name is reported as the envelope's open-string
// code. The envelope's code is routed by family, and an unrecognized code
// inside a known family is the family default, so a client behaves correctly
// while this service states only what Control actually told it.
func controlCommandFault(code connect.Code) *clientFault {
	switch code {
	case connect.CodePermissionDenied:
		return newFault(codePermissionDenied, "the current fixture scope does not permit this command")
	case connect.CodeInvalidArgument:
		return newFault(codeInvalidArgument, "the command was rejected by Control")
	case connect.CodeNotFound:
		return newFault(codeNotFound, "no such operation exists in this scope")
	case connect.CodeAborted:
		return newFault(codeAborted, "the command conflicts with the operation's current state")
	case connect.CodeFailedPrecondition:
		return newFault(codeChangeBlocked, "the operation's current state does not admit this command")
	default:
		return newFault(codeDependencyUnavailable, "the command dependency is unavailable")
	}
}

// instant renders a private timestamp as the RFC 3339 UTC-Z string the public
// value contract requires.
func instant(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

// controlStateNames and publicStatusNames translate the private enums into the
// public JSON members. The mapping is written out rather than derived from the
// generated names, so a renamed or added private member cannot silently produce
// a public value that no consumer recognizes.
var controlStateNames = map[controlv1.ControlState]string{
	controlv1.ControlState_CONTROL_STATE_RUNNING:        "running",
	controlv1.ControlState_CONTROL_STATE_HOLD_PENDING:   "hold_pending",
	controlv1.ControlState_CONTROL_STATE_HELD:           "held",
	controlv1.ControlState_CONTROL_STATE_BLOCKED:        "blocked",
	controlv1.ControlState_CONTROL_STATE_RESUME_PENDING: "resume_pending",
}

var publicStatusNames = map[controlv1.PublicStatus]string{
	controlv1.PublicStatus_PUBLIC_STATUS_PENDING:   "pending",
	controlv1.PublicStatus_PUBLIC_STATUS_RUNNING:   "running",
	controlv1.PublicStatus_PUBLIC_STATUS_BLOCKED:   "blocked",
	controlv1.PublicStatus_PUBLIC_STATUS_SUCCEEDED: "succeeded",
	controlv1.PublicStatus_PUBLIC_STATUS_FAILED:    "failed",
	controlv1.PublicStatus_PUBLIC_STATUS_CANCELED:  "canceled",
	controlv1.PublicStatus_PUBLIC_STATUS_EXPIRED:   "expired",
}
