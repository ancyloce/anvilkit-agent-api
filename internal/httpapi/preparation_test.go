package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1"
	values "github.com/ancyloce/anvilkit-agent-api/internal/contracts/valuesv1"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"
)

const (
	preparationOperation = "op-prep-fixture-1"
	preparationInput     = `{"schemaVersion":1,"prompt":{"text":"A launch hero with a heading, a description and a call to action; no image."}}`
	preparationBody      = `{"schemaVersion":1,"commandId":"cmd-prep-1","intakeSource":"ui","input":` + preparationInput + `}`
	questionSetRefJSON   = `{"kind":"evidence","refId":"question-set-1","subjectDigest":"sha256:3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855e","contentDigest":"sha256:7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c","sizeBytes":"612","objectVersion":"sha256:7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"}`
	answersBody          = `{"commandId":"cmd-prep-1-answers-1","expectedOperationRevision":"3","questionSetRef":` + questionSetRefJSON + `,"answerSet":{"schemaVersion":1,"operationId":"` + preparationOperation + `","questionSetRevision":"1","answers":[{"questionId":"q-1-content-image","answer":"Imageless result"}]}}`
)

// stubPreparations stands in for Control's preparation methods.
type stubPreparations struct {
	answered   *controlv1.RecordPreparationAnswersRequest
	answers    *controlv1.RecordPreparationAnswersResponse
	answersErr error
	read       *controlv1.ReadPreparationRequest
	detail     *controlv1.ReadPreparationResponse
	readErr    error
}

func (s *stubPreparations) RecordPreparationAnswers(_ context.Context, request *connect.Request[controlv1.RecordPreparationAnswersRequest]) (*connect.Response[controlv1.RecordPreparationAnswersResponse], error) {
	s.answered = request.Msg
	if s.answersErr != nil {
		return nil, s.answersErr
	}
	return connect.NewResponse(s.answers), nil
}

func (s *stubPreparations) ReadPreparation(_ context.Context, request *connect.Request[controlv1.ReadPreparationRequest]) (*connect.Response[controlv1.ReadPreparationResponse], error) {
	s.read = request.Msg
	if s.readErr != nil {
		return nil, s.readErr
	}
	return connect.NewResponse(s.detail), nil
}

func evidenceRef(refID string) *values.ArtifactRef {
	kind := values.ArtifactKind_EVIDENCE
	size := uint64(612)
	digest := "sha256:7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c7c"
	return &values.ArtifactRef{Kind: &kind, RefId: text(refID), SubjectDigest: text("sha256:3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855e"), ContentDigest: &digest, SizeBytes: &size, ObjectVersion: &digest}
}

func preparationAccepted(existing bool) *controlv1.AdmitOperationResponse {
	response := acceptedResponse(existing)
	response.OperationId = text(preparationOperation)
	response.FundingState = nil
	authority := controlv1.FundingAuthority_FUNDING_AUTHORITY_TEST
	response.FundingAuthority = &authority
	response.AuthorizedFundingRef = text("tq-" + preparationOperation)
	return response
}

func newPreparationHarness(t *testing.T, commander *stubCommander, preparations *stubPreparations) *commandHarness {
	t.Helper()
	h := newCommandHarness(t, commander, true)
	server := NewServer(Dependencies{Logger: testLogger(h.logs), Identities: testProfile(t), Commands: commander, Preparations: preparations, ServesLocalChecks: true, ControlCallTimeout: 5 * time.Second})
	h.handler = server.PublicHandler()
	return h
}

// newPreparationHarnessWithDisclosure adds the protected-read handshake the
// detail route shares with the view.
func newPreparationHarnessWithDisclosure(t *testing.T, preparations *stubPreparations) *readHarness {
	t.Helper()
	disclosure := &fakeDisclosure{decision: controlv1.DisclosureDecision_DISCLOSURE_DECISION_ALLOW, scope: preparationOperation, freshFor: time.Minute}
	h := newReadHarness(t, fixtureProjection(), disclosure)
	server := NewServer(Dependencies{Logger: testLogger(h.logs), Identities: testProfile(t), Disclosure: disclosure, Preparations: preparations, ReadModel: h.projection, ServesLocalChecks: true, ControlCallTimeout: 5 * time.Second})
	server.readRoleSeenAt.Store(time.Now().UnixNano())
	h.server, h.handler = server, server.PublicHandler()
	return h
}

func testLogger(logs *syncBuffer) *slog.Logger {
	return logging.New(logs, logging.Identity{ServiceVersion: "0.0.0-test", ServiceInstanceID: "test-instance", Environment: "test"}, slog.LevelDebug)
}

func TestPreparationSubmissionForwardsInlineInput(t *testing.T) {
	commander := &stubCommander{accepted: preparationAccepted(false)}
	h := newPreparationHarness(t, commander, &stubPreparations{})
	response := h.post(t, routePreparations, preparationBody, "Bearer "+localToken, "application/json")
	if response.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	if commander.admitted.GetKind() != controlv1.OperationKind_OPERATION_KIND_PREPARATION || commander.admitted.GetIntakeSource() != controlv1.IntakeSource_INTAKE_SOURCE_UI || string(commander.admitted.GetPreparationInput()) != preparationInput || commander.admitted.GetContext().GetActorId() != "developer-fixture-3" || commander.admitted.GetContext().GetTenantId() != "tenant-fixture-3" {
		t.Fatalf("forwarded %+v", commander.admitted)
	}
	var accepted operationAccepted
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil || accepted.OperationID != preparationOperation || accepted.FundingAuthority != "test" || accepted.AuthorizedFundingRef != "tq-"+preparationOperation || accepted.Existing {
		t.Fatalf("body %s", response.Body.String())
	}
	if response.Header().Get("Location") != "/v1/operations/"+preparationOperation {
		t.Fatal("a fresh acceptance names its operation")
	}
	commander.accepted = preparationAccepted(true)
	if replay := h.post(t, routePreparations, preparationBody, "Bearer "+localToken, "application/json"); replay.Code != http.StatusOK {
		t.Fatalf("replay status %d", replay.Code)
	}
}

func TestPreparationSubmissionRejectsShapeAndAuthority(t *testing.T) {
	commander := &stubCommander{accepted: preparationAccepted(false)}
	h := newPreparationHarness(t, commander, &stubPreparations{})
	for name, body := range map[string]string{
		"missing input":       `{"schemaVersion":1,"commandId":"cmd-prep-1","intakeSource":"ui"}`,
		"reference only":      `{"schemaVersion":1,"commandId":"cmd-prep-1","intakeSource":"ui","inputRef":` + questionSetRefJSON + `}`,
		"empty prompt":        `{"schemaVersion":1,"commandId":"cmd-prep-1","intakeSource":"ui","input":{"schemaVersion":1,"prompt":{"text":""}}}`,
		"platform source":     `{"schemaVersion":1,"commandId":"cmd-prep-1","intakeSource":"platform","input":` + preparationInput + `}`,
		"client funding ref":  `{"schemaVersion":1,"commandId":"cmd-prep-1","intakeSource":"ui","input":` + preparationInput + `,"authorizedFundingRef":"tq-1"}`,
		"non-canonical actor": `{"schemaVersion":1,"commandId":"cmd-prep-1","intakeSource":"ui","input":` + preparationInput + `,"actorId":"someone"}`,
	} {
		response := h.post(t, routePreparations, body, "Bearer "+localToken, "application/json")
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s: status %d: %s", name, response.Code, response.Body.String())
		}
	}
	if commander.admitCalls.Load() != 0 {
		t.Fatal("rejected bodies never reach Control")
	}
	if response := h.post(t, routePreparations, preparationBody, "Bearer "+readerToken, "application/json"); response.Code != http.StatusForbidden {
		t.Fatalf("an actor without component.prepare: %d", response.Code)
	}
	commander.accepted = acceptedResponse(false)
	if response := h.post(t, routePreparations, preparationBody, "Bearer "+localToken, "application/json"); response.Code != http.StatusServiceUnavailable {
		t.Fatalf("an acceptance without the bound test quote is not acknowledged: %d %s", response.Code, response.Body.String())
	}
	commander.accepted, commander.admitErr = nil, connect.NewError(connect.CodeAborted, errors.New("IDEMPOTENCY_CONFLICT"))
	if response := h.post(t, routePreparations, preparationBody, "Bearer "+localToken, "application/json"); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), codeIdempotencyConflict) {
		t.Fatalf("different input under the same command: %d %s", response.Code, response.Body.String())
	}
}

func TestPreparationAnswersForwardAndMapDecisions(t *testing.T) {
	revision, questionSetRevision := uint64(4), uint64(1)
	accepted := controlv1.AcceptResultDecision_ACCEPT_RESULT_DECISION_ACCEPTED
	preparations := &stubPreparations{answers: &controlv1.RecordPreparationAnswersResponse{Decision: &accepted, OperationRevision: &revision, QuestionSetRevision: &questionSetRevision, AcceptedAnswerSetRef: evidenceRef("answer-set-1")}}
	h := newPreparationHarness(t, &stubCommander{}, preparations)
	path := "/v1/operations/" + preparationOperation + "/preparation-answers"
	response := h.post(t, path, answersBody, "Bearer "+localToken, "application/json")
	if response.Code != http.StatusAccepted {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	if preparations.answered.GetOperationId() != preparationOperation || preparations.answered.GetExpectedOperationRevision() != 3 || preparations.answered.GetQuestionSetRef().GetRefId() != "question-set-1" || preparations.answered.AnswerSetRef != nil || !strings.Contains(string(preparations.answered.GetAnswerSet()), "Imageless result") {
		t.Fatalf("forwarded %+v", preparations.answered)
	}
	var body preparationAnswersAccepted
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil || body.OperationRevision != "4" || body.QuestionSetRevision != "1" || body.AcceptedAnswerSetRef.RefID != "answer-set-1" || body.Existing {
		t.Fatalf("body %s", response.Body.String())
	}
	duplicate := controlv1.AcceptResultDecision_ACCEPT_RESULT_DECISION_DUPLICATE
	preparations.answers.Decision = &duplicate
	if replay := h.post(t, path, answersBody, "Bearer "+localToken, "application/json"); replay.Code != http.StatusOK || !strings.Contains(replay.Body.String(), `"existing":true`) {
		t.Fatalf("duplicate command: %d %s", replay.Code, replay.Body.String())
	}
	stale := controlv1.AcceptResultDecision_ACCEPT_RESULT_DECISION_STALE
	preparations.answers.Decision = &stale
	if response := h.post(t, path, answersBody, "Bearer "+localToken, "application/json"); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), codeRevisionConflict) {
		t.Fatalf("a second answer set for the revision: %d %s", response.Code, response.Body.String())
	}
	for message, code := range map[string]string{"REVISION_CONFLICT": codeRevisionConflict, "OPERATION_TERMINAL": codeAborted, "IDEMPOTENCY_CONFLICT": codeIdempotencyConflict} {
		preparations.answersErr = connect.NewError(connect.CodeAborted, errors.New(message))
		if response := h.post(t, path, answersBody, "Bearer "+localToken, "application/json"); response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), code) {
			t.Fatalf("%s: %d %s", message, response.Code, response.Body.String())
		}
	}
	preparations.answersErr = nil
	wrongOperation := strings.Replace(answersBody, preparationOperation, "op-other", 1)
	if response := h.post(t, path, wrongOperation, "Bearer "+localToken, "application/json"); response.Code != http.StatusBadRequest {
		t.Fatalf("an answer set for another operation: %d", response.Code)
	}
	if response := h.post(t, path, answersBody, "Bearer "+readerToken, "application/json"); response.Code != http.StatusForbidden {
		t.Fatalf("an actor without component.prepare: %d", response.Code)
	}
}

func TestPreparationReadResolvesArtifacts(t *testing.T) {
	revision, generation, round, questionSetRevision := uint64(3), uint64(1), uint64(1), uint64(1)
	status, stage := controlv1.PublicStatus_PUBLIC_STATUS_PENDING, controlv1.BusinessStage_BUSINESS_STAGE_AWAITING_INPUT
	questionSet := `{"schemaVersion":1,"operationId":"` + preparationOperation + `","questionSetRevision":"1","round":"1","questions":[{"questionId":"q-1-content-image","text":"Image?","materialUnknown":"content.image"}]}`
	preparations := &stubPreparations{detail: &controlv1.ReadPreparationResponse{Status: &status, BusinessStage: &stage, OperationRevision: &revision, ExecutionGeneration: &generation,
		Preparation: &controlv1.PreparationProjection{Round: &round, QuestionSetRef: evidenceRef("question-set-1"), QuestionSetRevision: &questionSetRevision, QuestionSetExpiresAt: timestamppb.New(time.Date(2026, 9, 20, 10, 0, 0, 0, time.UTC))},
		Input:       []byte(preparationInput), QuestionSet: []byte(questionSet)}}
	h := newPreparationHarnessWithDisclosure(t, preparations)
	response := h.get(t, "/v1/operations/"+preparationOperation+"/preparation", "Bearer "+localToken, nil)
	if response.Code != http.StatusOK {
		t.Fatalf("status %d: %s", response.Code, response.Body.String())
	}
	var detail preparationDetail
	if err := json.Unmarshal(response.Body.Bytes(), &detail); err != nil || detail.BusinessStage != "awaiting_input" || detail.Status != "pending" || detail.OperationRevision != "3" || detail.Preparation.QuestionSetRevision != "1" || detail.Preparation.QuestionSetExpiresAt != "2026-09-20T10:00:00Z" || string(detail.QuestionSet) != questionSet || string(detail.Input) != preparationInput || detail.Brief != nil {
		t.Fatalf("body %s", response.Body.String())
	}
	if preparations.read.GetOperationId() != preparationOperation || preparations.read.GetContext().GetActorId() != "developer-fixture-3" {
		t.Fatalf("read %+v", preparations.read)
	}
	preparations.readErr = connect.NewError(connect.CodePermissionDenied, errors.New("PERMISSION_DENIED"))
	if response := h.get(t, "/v1/operations/"+preparationOperation+"/preparation", "Bearer "+localToken, nil); response.Code != http.StatusNotFound {
		t.Fatalf("a scope Control refuses is absent: %d", response.Code)
	}
}
