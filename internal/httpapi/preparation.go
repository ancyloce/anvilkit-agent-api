package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"connectrpc.com/connect"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"

	controlv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1"
	"github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1/controlv1connect"
	values "github.com/ancyloce/anvilkit-agent-api/internal/contracts/valuesv1"
)

// The preparation routes (development plan S2, 2026-09-13), from the Agent
// OpenAPI: submission and answers require component.prepare, the read requires
// operation.read under the same disclosure handshake as the view. Like the
// local-check route they exist only in the controlled local profile.
const (
	routePreparations         = "/v1/operations/preparations"
	routePreparationAnswers   = "/v1/operations/{operationId}/preparation-answers"
	routePreparationDetail    = "/v1/operations/{operationId}/preparation"
	actionComponentPrepare    = "component.prepare"
	recordAnswersProcedure    = controlv1connect.ControlServiceRecordPreparationAnswersProcedure
	readPreparationProcedure  = controlv1connect.ControlServiceReadPreparationProcedure
	preparationBodyMaxBytes   = config.PreparationBodyMaxBytes
	preparationInputMaxBytes  = config.PreparationBodyMaxBytes
	preparationAnswerMaxBytes = config.PreparationBodyMaxBytes
)

// PreparationCommander is the slice of Control the two preparation commands
// and the read call; the generated ControlService client satisfies it.
type PreparationCommander interface {
	RecordPreparationAnswers(context.Context, *connect.Request[controlv1.RecordPreparationAnswersRequest]) (*connect.Response[controlv1.RecordPreparationAnswersResponse], error)
	ReadPreparation(context.Context, *connect.Request[controlv1.ReadPreparationRequest]) (*connect.Response[controlv1.ReadPreparationResponse], error)
}

var (
	preparationCommandMembers = []string{"schemaVersion", "commandId", "intakeSource", "input"}
	preparationAnswersMembers = []string{"commandId", "expectedOperationRevision", "questionSetRef", "answerSet"}
	artifactRefMembers        = []string{"kind", "refId", "subjectDigest", "contentDigest", "sizeBytes", "objectVersion"}
	intakeSources             = map[string]controlv1.IntakeSource{"ui": controlv1.IntakeSource_INTAKE_SOURCE_UI, "api": controlv1.IntakeSource_INTAKE_SOURCE_API}
	artifactKinds             = map[string]values.ArtifactKind{"evidence": values.ArtifactKind_EVIDENCE, "preparation-input": values.ArtifactKind_PREPARATION_INPUT}
	artifactKindNames         = map[values.ArtifactKind]string{values.ArtifactKind_EVIDENCE: "evidence", values.ArtifactKind_PREPARATION_INPUT: "preparation-input"}
)

// artifactRef is the public JSON artifactRef value.
type artifactRef struct {
	Kind          string `json:"kind"`
	RefID         string `json:"refId"`
	SubjectDigest string `json:"subjectDigest"`
	ContentDigest string `json:"contentDigest"`
	SizeBytes     string `json:"sizeBytes"`
	ObjectVersion string `json:"objectVersion"`
}

func (ref artifactRef) valid() bool {
	size, err := strconv.ParseUint(ref.SizeBytes, 10, 64)
	_, known := artifactKinds[ref.Kind]
	return known && isIdentifier(ref.RefID) && digestPattern.MatchString(ref.SubjectDigest) && digestPattern.MatchString(ref.ContentDigest) && err == nil && size > 0 && isIdentifier(ref.ObjectVersion)
}

func (ref artifactRef) proto() *values.ArtifactRef {
	size, _ := strconv.ParseUint(ref.SizeBytes, 10, 64)
	kind := artifactKinds[ref.Kind]
	return &values.ArtifactRef{Kind: &kind, RefId: &ref.RefID, SubjectDigest: &ref.SubjectDigest, ContentDigest: &ref.ContentDigest, SizeBytes: &size, ObjectVersion: &ref.ObjectVersion}
}

func artifactRefFrom(ref *values.ArtifactRef) (*artifactRef, error) {
	if ref == nil {
		return nil, nil
	}
	name, known := artifactKindNames[ref.GetKind()]
	converted := artifactRef{Kind: name, RefID: ref.GetRefId(), SubjectDigest: ref.GetSubjectDigest(), ContentDigest: ref.GetContentDigest(), SizeBytes: strconv.FormatUint(ref.GetSizeBytes(), 10), ObjectVersion: ref.GetObjectVersion()}
	if !known || !converted.valid() {
		return nil, errors.New("the reference is outside the value contract")
	}
	return &converted, nil
}

// preparationCommand is the PreparationCommand request body. The input record
// is forwarded as the bytes the caller sent; Control validates it against the
// retained preparation schema and stores it.
type preparationCommand struct {
	SchemaVersion json.RawMessage `json:"schemaVersion"`
	CommandID     string          `json:"commandId"`
	IntakeSource  string          `json:"intakeSource"`
	Input         json.RawMessage `json:"input"`
}

// preparationAnswersCommand is the PreparationAnswersCommand request body.
type preparationAnswersCommand struct {
	CommandID                 string          `json:"commandId"`
	ExpectedOperationRevision string          `json:"expectedOperationRevision"`
	QuestionSetRef            artifactRef     `json:"questionSetRef"`
	AnswerSet                 json.RawMessage `json:"answerSet"`
}

// preparationAnswersAccepted is the PreparationAnswersAccepted response body.
type preparationAnswersAccepted struct {
	OperationID          string      `json:"operationId"`
	OperationRevision    string      `json:"operationRevision"`
	QuestionSetRevision  string      `json:"questionSetRevision"`
	AcceptedAnswerSetRef artifactRef `json:"acceptedAnswerSetRef"`
	Existing             bool        `json:"existing"`
}

// preparationDetail is the PreparationDetail response body: the projection
// with the content of the artifacts it references.
type preparationDetail struct {
	OperationID       string                 `json:"operationId"`
	Status            string                 `json:"status"`
	BusinessStage     string                 `json:"businessStage"`
	OperationRevision string                 `json:"operationRevision"`
	Preparation       *preparationProjection `json:"preparation"`
	Input             json.RawMessage        `json:"input"`
	QuestionSet       json.RawMessage        `json:"questionSet,omitempty"`
	AcceptedAnswerSet json.RawMessage        `json:"acceptedAnswerSet,omitempty"`
	Brief             json.RawMessage        `json:"brief,omitempty"`
}

type preparationProjection struct {
	Round                string       `json:"round"`
	QuestionSetRef       *artifactRef `json:"questionSetRef,omitempty"`
	QuestionSetRevision  string       `json:"questionSetRevision,omitempty"`
	QuestionSetExpiresAt string       `json:"questionSetExpiresAt,omitempty"`
	AcceptedAnswerSetRef *artifactRef `json:"acceptedAnswerSetRef,omitempty"`
	BriefRef             *artifactRef `json:"briefRef,omitempty"`
	BriefRevision        string       `json:"briefRevision,omitempty"`
	ContentRejections    string       `json:"contentRejections,omitempty"`
}

var businessStageNames = map[controlv1.BusinessStage]string{
	controlv1.BusinessStage_BUSINESS_STAGE_ADMISSION_PENDING: "admission_pending", controlv1.BusinessStage_BUSINESS_STAGE_QUEUED: "queued",
	controlv1.BusinessStage_BUSINESS_STAGE_ANALYZING: "analyzing", controlv1.BusinessStage_BUSINESS_STAGE_AWAITING_INPUT: "awaiting_input",
	controlv1.BusinessStage_BUSINESS_STAGE_BRIEF_READY: "brief_ready", controlv1.BusinessStage_BUSINESS_STAGE_EXPIRED: "expired",
	controlv1.BusinessStage_BUSINESS_STAGE_FAILED: "failed", controlv1.BusinessStage_BUSINESS_STAGE_CANCELED: "canceled",
}

// handleSubmitPreparation serves POST /v1/operations/preparations.
func (s *Server) handleSubmitPreparation(w http.ResponseWriter, r *http.Request) {
	actor, fault := s.authorizeAction(r, actionComponentPrepare)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}
	command, fault := decodePreparationCommand(r)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}
	accepted, fault := s.forwardPreparation(r.Context(), actor, command)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}
	status := http.StatusAccepted
	if accepted.Existing {
		status = http.StatusOK
	} else {
		w.Header().Set("Location", "/v1/operations/"+accepted.OperationID)
	}
	recordOutcome(r.Context(), status, "", "")
	writeJSON(w, status, accepted)
}

// handleSubmitPreparationAnswers serves POST /v1/operations/{operationId}/preparation-answers.
func (s *Server) handleSubmitPreparationAnswers(w http.ResponseWriter, r *http.Request) {
	actor, fault := s.authorizeAction(r, actionComponentPrepare)
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
	command, fault := decodePreparationAnswersCommand(r)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}
	accepted, fault := s.forwardPreparationAnswers(r.Context(), actor, operationID, command)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}
	status := http.StatusAccepted
	if accepted.Existing {
		status = http.StatusOK
	}
	recordOutcome(r.Context(), status, "", "")
	writeJSON(w, status, accepted)
}

// handleReadPreparation serves GET /v1/operations/{operationId}/preparation.
func (s *Server) handleReadPreparation(w http.ResponseWriter, r *http.Request) {
	actor, operationID, grant, fault := s.beginProtectedRead(r)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}
	detail, fault := s.forwardReadPreparation(r.Context(), actor, operationID)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}
	if !grant.usableAt(time.Now()) {
		s.writeError(w, r, newFault(codeDependencyUnavailable, "authorization expired during the protected read"))
		return
	}
	recordOutcome(r.Context(), http.StatusOK, "", "")
	writeJSON(w, http.StatusOK, detail)
}

func decodePreparationCommand(r *http.Request) (*preparationCommand, *clientFault) {
	body, fault := readBoundedBody(r, preparationBodyMaxBytes)
	if fault != nil {
		return nil, fault
	}
	members, fault := scanStrictJSON(body)
	if fault != nil {
		return nil, fault
	}
	if fault := requireExactMembers(members, preparationCommandMembers); fault != nil {
		return nil, fault
	}
	var command preparationCommand
	if err := json.Unmarshal(body, &command); err != nil {
		return nil, memberTypeFault(err)
	}
	if string(command.SchemaVersion) != "1" {
		return nil, newFault(codeUnsupportedSchema, "schemaVersion must be 1")
	}
	if !isIdentifier(command.CommandID) {
		return nil, newFault(codeInvalidArgument, "commandId must be a values-v1 identifier")
	}
	if _, known := intakeSources[command.IntakeSource]; !known {
		return nil, newFault(codeInvalidArgument, "intakeSource must be ui or api")
	}
	// The input record's members are Control's to validate against the retained
	// schema; the boundary requires an object with a non-empty prompt text.
	var input struct {
		SchemaVersion json.RawMessage `json:"schemaVersion"`
		Prompt        struct {
			Text string `json:"text"`
		} `json:"prompt"`
	}
	if len(command.Input) == 0 || command.Input[0] != '{' || json.Unmarshal(command.Input, &input) != nil || string(input.SchemaVersion) != "1" || input.Prompt.Text == "" || len(command.Input) > preparationInputMaxBytes {
		return nil, newFault(codeInvalidArgument, "input must be a PreparationInputV1 record with a non-empty prompt")
	}
	return &command, nil
}

func decodePreparationAnswersCommand(r *http.Request) (*preparationAnswersCommand, *clientFault) {
	body, fault := readBoundedBody(r, preparationBodyMaxBytes)
	if fault != nil {
		return nil, fault
	}
	members, fault := scanStrictJSON(body)
	if fault != nil {
		return nil, fault
	}
	if fault := requireExactMembers(members, preparationAnswersMembers); fault != nil {
		return nil, fault
	}
	var command preparationAnswersCommand
	if err := json.Unmarshal(body, &command); err != nil {
		return nil, memberTypeFault(err)
	}
	if !isIdentifier(command.CommandID) {
		return nil, newFault(codeInvalidArgument, "commandId must be a values-v1 identifier")
	}
	if _, err := parseEventSeq(command.ExpectedOperationRevision); err != nil {
		return nil, newFault(codeInvalidArgument, "expectedOperationRevision must be a bounded unsigned decimal counter")
	}
	if !command.QuestionSetRef.valid() || command.QuestionSetRef.Kind != "evidence" {
		return nil, newFault(codeInvalidArgument, "questionSetRef must be an evidence artifact reference")
	}
	var answers struct {
		SchemaVersion json.RawMessage `json:"schemaVersion"`
		OperationID   string          `json:"operationId"`
		Answers       []struct {
			QuestionID string `json:"questionId"`
			Answer     string `json:"answer"`
		} `json:"answers"`
	}
	if len(command.AnswerSet) == 0 || command.AnswerSet[0] != '{' || json.Unmarshal(command.AnswerSet, &answers) != nil || string(answers.SchemaVersion) != "1" || answers.OperationID != r.PathValue("operationId") || len(answers.Answers) == 0 || len(command.AnswerSet) > preparationAnswerMaxBytes {
		return nil, newFault(codeInvalidArgument, "answerSet must be an AnswerSetV1 record for this operation with at least one answer")
	}
	return &command, nil
}

func (s *Server) forwardPreparation(ctx context.Context, actor identity.Actor, command *preparationCommand) (*operationAccepted, *clientFault) {
	kind := controlv1.OperationKind_OPERATION_KIND_PREPARATION
	intakeSource := intakeSources[command.IntakeSource]
	message := &controlv1.AdmitOperationRequest{Context: s.authenticatedContext(ctx, actor, admitOperationProcedure), CommandId: &command.CommandID, Kind: &kind, IntakeSource: &intakeSource, PreparationInput: []byte(command.Input)}
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
		fault := preparationFault(code)
		s.logControlCall(ctx, admitOperationProcedure, elapsed, code.String(), outcomeForCode(code), fault)
		return nil, fault
	}
	accepted, contractErr := preparationAcceptedFrom(response.Msg)
	if contractErr != nil {
		fault := newFault(codeDependencyUnavailable, "the intake dependency returned an acceptance outside its contract")
		s.logControlCall(ctx, admitOperationProcedure, elapsed, "ok", "error", fault)
		return nil, fault
	}
	recordOperation(ctx, accepted.OperationID)
	s.logControlCall(ctx, admitOperationProcedure, elapsed, "ok", "ok", nil)
	return accepted, nil
}

// preparationAcceptedFrom holds the acceptance to the contract: a preparation
// always carries the server-bound test authority and its Control-generated quote.
func preparationAcceptedFrom(message *controlv1.AdmitOperationResponse) (*operationAccepted, error) {
	accepted, err := acceptedFrom(message)
	if err != nil {
		return nil, err
	}
	if message.GetFundingAuthority() != controlv1.FundingAuthority_FUNDING_AUTHORITY_TEST || message.AuthorizedFundingRef == nil || !isIdentifier(*message.AuthorizedFundingRef) || message.FundingState != nil {
		return nil, errors.New("the preparation acceptance omits its bound test quote")
	}
	accepted.FundingAuthority = "test"
	accepted.AuthorizedFundingRef = *message.AuthorizedFundingRef
	return accepted, nil
}

func (s *Server) forwardPreparationAnswers(ctx context.Context, actor identity.Actor, operationID string, command *preparationAnswersCommand) (*preparationAnswersAccepted, *clientFault) {
	expectedRevision, err := parseEventSeq(command.ExpectedOperationRevision)
	if err != nil {
		return nil, newFault(codeInvalidArgument, "expectedOperationRevision must be a bounded unsigned decimal counter")
	}
	message := &controlv1.RecordPreparationAnswersRequest{Context: s.authenticatedContext(ctx, actor, recordAnswersProcedure), CommandId: &command.CommandID, OperationId: &operationID, ExpectedOperationRevision: &expectedRevision, QuestionSetRef: command.QuestionSetRef.proto(), AnswerSet: []byte(command.AnswerSet)}
	call, cancel := context.WithTimeout(ctx, s.controlCallTimeout)
	defer cancel()
	started := time.Now()
	request := connect.NewRequest(message)
	if credential := actor.ControlCredential(); credential != "" {
		request.Header().Set("Authorization", "Bearer "+credential)
	}
	response, callErr := s.preparations.RecordPreparationAnswers(call, request)
	elapsed := time.Since(started)
	if callErr != nil {
		code := connect.CodeOf(callErr)
		fault := preparationAnswersFault(code, callErr)
		s.logControlCall(ctx, recordAnswersProcedure, elapsed, code.String(), outcomeForCode(code), fault)
		return nil, fault
	}
	m := response.Msg
	if m.GetDecision() == controlv1.AcceptResultDecision_ACCEPT_RESULT_DECISION_STALE {
		fault := newFault(codeRevisionConflict, "an answer set is already accepted for this question set")
		s.logControlCall(ctx, recordAnswersProcedure, elapsed, "ok", "ok", nil)
		return nil, fault
	}
	ref, refErr := artifactRefFrom(m.GetAcceptedAnswerSetRef())
	if (m.GetDecision() != controlv1.AcceptResultDecision_ACCEPT_RESULT_DECISION_ACCEPTED && m.GetDecision() != controlv1.AcceptResultDecision_ACCEPT_RESULT_DECISION_DUPLICATE) || m.OperationRevision == nil || m.QuestionSetRevision == nil || refErr != nil || ref == nil {
		fault := newFault(codeDependencyUnavailable, "the command dependency returned a result outside its contract")
		s.logControlCall(ctx, recordAnswersProcedure, elapsed, "ok", "error", fault)
		return nil, fault
	}
	s.logControlCall(ctx, recordAnswersProcedure, elapsed, "ok", "ok", nil)
	return &preparationAnswersAccepted{OperationID: operationID, OperationRevision: strconv.FormatUint(m.GetOperationRevision(), 10), QuestionSetRevision: strconv.FormatUint(m.GetQuestionSetRevision(), 10), AcceptedAnswerSetRef: *ref, Existing: m.GetDecision() == controlv1.AcceptResultDecision_ACCEPT_RESULT_DECISION_DUPLICATE}, nil
}

func (s *Server) forwardReadPreparation(ctx context.Context, actor identity.Actor, operationID string) (*preparationDetail, *clientFault) {
	message := &controlv1.ReadPreparationRequest{Context: s.authenticatedContext(ctx, actor, readPreparationProcedure), OperationId: &operationID}
	call, cancel := context.WithTimeout(ctx, s.controlCallTimeout)
	defer cancel()
	started := time.Now()
	request := connect.NewRequest(message)
	if credential := actor.ControlCredential(); credential != "" {
		request.Header().Set("Authorization", "Bearer "+credential)
	}
	response, err := s.preparations.ReadPreparation(call, request)
	elapsed := time.Since(started)
	if err != nil {
		code := connect.CodeOf(err)
		fault := controlCommandFault(code)
		if code == connect.CodeNotFound || code == connect.CodePermissionDenied {
			fault = newFault(codeNotFound, "no such preparation exists in this scope")
		}
		s.logControlCall(ctx, readPreparationProcedure, elapsed, code.String(), outcomeForCode(code), fault)
		return nil, fault
	}
	detail, contractErr := preparationDetailFrom(response.Msg, operationID)
	if contractErr != nil {
		fault := newFault(codeDependencyUnavailable, "the read dependency returned a result outside its contract")
		s.logControlCall(ctx, readPreparationProcedure, elapsed, "ok", "error", fault)
		return nil, fault
	}
	s.logControlCall(ctx, readPreparationProcedure, elapsed, "ok", "ok", nil)
	return detail, nil
}

func preparationDetailFrom(m *controlv1.ReadPreparationResponse, operationID string) (*preparationDetail, error) {
	if m == nil || m.OperationRevision == nil || m.Preparation == nil || len(m.GetInput()) == 0 || !json.Valid(m.GetInput()) {
		return nil, errors.New("the preparation read is incomplete")
	}
	status, known := publicStatusNames[m.GetStatus()]
	stage, stageKnown := businessStageNames[m.GetBusinessStage()]
	if !known || !stageKnown {
		return nil, errors.New("the preparation read carries no decided state")
	}
	p := m.GetPreparation()
	projection := &preparationProjection{Round: strconv.FormatUint(p.GetRound(), 10)}
	var err error
	if projection.QuestionSetRef, err = artifactRefFrom(p.QuestionSetRef); err != nil {
		return nil, err
	}
	if projection.QuestionSetRef != nil {
		if p.QuestionSetRevision == nil || p.QuestionSetExpiresAt == nil || !p.QuestionSetExpiresAt.IsValid() {
			return nil, errors.New("a posed question set carries its revision and clock")
		}
		projection.QuestionSetRevision = strconv.FormatUint(p.GetQuestionSetRevision(), 10)
		projection.QuestionSetExpiresAt = instant(p.GetQuestionSetExpiresAt().AsTime())
	}
	if projection.AcceptedAnswerSetRef, err = artifactRefFrom(p.AcceptedAnswerSetRef); err != nil {
		return nil, err
	}
	if projection.BriefRef, err = artifactRefFrom(p.BriefRef); err != nil {
		return nil, err
	}
	if projection.BriefRef != nil {
		if p.BriefRevision == nil {
			return nil, errors.New("a frozen brief carries its revision")
		}
		projection.BriefRevision = strconv.FormatUint(p.GetBriefRevision(), 10)
	}
	if p.ContentRejections != nil {
		projection.ContentRejections = strconv.FormatUint(p.GetContentRejections(), 10)
	}
	detail := &preparationDetail{OperationID: operationID, Status: status, BusinessStage: stage, OperationRevision: strconv.FormatUint(m.GetOperationRevision(), 10), Preparation: projection, Input: json.RawMessage(m.GetInput())}
	for _, member := range []struct {
		raw  []byte
		into *json.RawMessage
	}{{m.GetQuestionSet(), &detail.QuestionSet}, {m.GetAcceptedAnswerSet(), &detail.AcceptedAnswerSet}, {m.GetBrief(), &detail.Brief}} {
		if len(member.raw) == 0 {
			continue
		}
		if !json.Valid(member.raw) {
			return nil, errors.New("the preparation read carries a malformed document")
		}
		*member.into = json.RawMessage(member.raw)
	}
	return detail, nil
}

// preparationFault maps an admission failure onto the public envelope.
func preparationFault(code connect.Code) *clientFault {
	switch code {
	case connect.CodePermissionDenied:
		return newFault(codePermissionDenied, "the current scope does not permit preparation")
	case connect.CodeInvalidArgument:
		return newFault(codeInvalidArgument, "the preparation command was rejected by admission")
	case connect.CodeAborted:
		return newFault(codeIdempotencyConflict, "this command identifier was accepted for a different input")
	case connect.CodeFailedPrecondition:
		return newFault(codeProfileQualification, "the controlled local profile does not admit preparations")
	default:
		return newFault(codeDependencyUnavailable, "the intake dependency is unavailable")
	}
}

// preparationAnswersFault maps an answer failure; Control names the ABORTED
// member in its message, which this service reports rather than guesses.
func preparationAnswersFault(code connect.Code, err error) *clientFault {
	if code == connect.CodeAborted {
		var connectErr *connect.Error
		if errors.As(err, &connectErr) {
			switch connectErr.Message() {
			case codeRevisionConflict:
				return newFault(codeRevisionConflict, "the answers name a superseded question set or operation revision")
			case codeIdempotencyConflict:
				return newFault(codeIdempotencyConflict, "this command identifier was accepted with different answers")
			case "OPERATION_TERMINAL":
				return newFault(codeAborted, "the preparation no longer accepts answers")
			}
		}
	}
	return controlCommandFault(code)
}
