package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"

	controlv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1"
)

const (
	localOperation    = "op-local-fixture-1"
	localCommand      = "cmd-local-fixture-1"
	localRequestHash  = "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	localCheckBody    = `{"commandId":"` + localCommand + `","fixtureId":"plain-v1"}`
	localCheckNewline = `{"commandId":"` + localCommand + `","fixtureId":"newline-v1"}`
)

// stubCommander stands in for Control's admission and reserved lane. It records
// what the boundary actually sent, so a test can prove that the caller's
// identity came from the authenticated context rather than from the body.
type stubCommander struct {
	admitCalls  atomic.Int32
	cancelCalls atomic.Int32

	admitted  *controlv1.AdmitOperationRequest
	canceled  *controlv1.ControlCommandRequest
	accepted  *controlv1.AdmitOperationResponse
	commanded *controlv1.ControlCommandResponse
	admitErr  error
	cancelErr error
}

func (s *stubCommander) AdmitOperation(
	_ context.Context,
	request *connect.Request[controlv1.AdmitOperationRequest],
) (*connect.Response[controlv1.AdmitOperationResponse], error) {
	s.admitCalls.Add(1)
	s.admitted = request.Msg
	if s.admitErr != nil {
		return nil, s.admitErr
	}
	return connect.NewResponse(s.accepted), nil
}

func (s *stubCommander) Cancel(
	_ context.Context,
	request *connect.Request[controlv1.ControlCommandRequest],
) (*connect.Response[controlv1.ControlCommandResponse], error) {
	s.cancelCalls.Add(1)
	s.canceled = request.Msg
	if s.cancelErr != nil {
		return nil, s.cancelErr
	}
	return connect.NewResponse(s.commanded), nil
}

func acceptedResponse(existing bool) *controlv1.AdmitOperationResponse {
	revision := uint64(1)
	funding := controlv1.FundingState_FUNDING_STATE_NOT_APPLICABLE
	return &controlv1.AdmitOperationResponse{
		OperationId:       text(localOperation),
		OperationRevision: &revision,
		AcceptedAt:        timestamppb.New(time.Date(2026, 9, 10, 9, 0, 0, 0, time.UTC)),
		QueueExpiresAt:    timestamppb.New(time.Date(2026, 9, 10, 9, 15, 0, 0, time.UTC)),
		RequestDigest:     text(localRequestHash),
		Existing:          flag(existing),
		FundingState:      &funding,
	}
}

func canceledResponse(coalesced bool) *controlv1.ControlCommandResponse {
	revision := uint64(4)
	state := controlv1.ControlState_CONTROL_STATE_RUNNING
	status := controlv1.PublicStatus_PUBLIC_STATUS_CANCELED
	return &controlv1.ControlCommandResponse{
		OperationId:         text(localOperation),
		OperationRevision:   &revision,
		ControlState:        &state,
		Status:              &status,
		ReasonCode:          text("CANCEL_REQUESTED"),
		FenceAcknowledgedAt: timestamppb.New(time.Date(2026, 9, 10, 9, 1, 0, 0, time.UTC)),
		Coalesced:           flag(coalesced),
		TrackedCommandId:    text(localCommand),
	}
}

// commandHarness wires the command surface to a Control stand-in under a chosen
// serving profile.
type commandHarness struct {
	handler   http.Handler
	logs      *syncBuffer
	commander *stubCommander
}

func newCommandHarness(t *testing.T, commander *stubCommander, servesLocalChecks bool) *commandHarness {
	t.Helper()
	logs := &syncBuffer{}
	logger := logging.New(logs, logging.Identity{
		ServiceVersion:    "0.0.0-test",
		ServiceInstanceID: "test-instance",
		Environment:       "test",
	}, slog.LevelDebug)

	server := NewServer(Dependencies{
		Logger:             logger,
		Identities:         testProfile(t),
		Commands:           commander,
		ServesLocalChecks:  servesLocalChecks,
		ControlCallTimeout: 5 * time.Second,
	})
	return &commandHarness{handler: server.PublicHandler(), logs: logs, commander: commander}
}

func (h *commandHarness) post(t *testing.T, path, body, authorization, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, request)
	return recorder
}

func (h *commandHarness) submitLocalCheck(t *testing.T, body, authorization string) *httptest.ResponseRecorder {
	t.Helper()
	return h.post(t, routeLocalChecks, body, authorization, "application/json")
}

func acceptingCommander() *stubCommander {
	return &stubCommander{accepted: acceptedResponse(false), commanded: canceledResponse(false)}
}

// TestLocalCheckRouteIsAbsentOutsideItsProfile proves the route is not merely
// denied outside the controlled local profile: it does not exist, answers the
// same way as any undeclared path, and reaches no admission call.
func TestLocalCheckRouteIsAbsentOutsideItsProfile(t *testing.T) {
	h := newCommandHarness(t, acceptingCommander(), false)

	recorder := h.submitLocalCheck(t, localCheckBody, bearer(localToken))
	requireEnvelope(t, recorder, http.StatusNotFound, codeNotFound)
	if calls := h.commander.admitCalls.Load(); calls != 0 {
		t.Fatalf("an absent route reached admission %d times", calls)
	}

	// The same body under the same credential must be indistinguishable from a
	// request to a path this service never declared.
	undeclared := h.post(t, "/v1/not-an-operation", localCheckBody, bearer(localToken), "application/json")
	if undeclared.Code != recorder.Code {
		t.Fatalf("the absent route answers %d but an undeclared path answers %d", recorder.Code, undeclared.Code)
	}
}

// TestLocalCheckRequiresItsOwnAction keeps the two identity outcomes distinct
// and proves that holding another action is not enough.
func TestLocalCheckRequiresItsOwnAction(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		authorization string
		status        int
		code          string
	}{
		{"no credential", "", http.StatusUnauthorized, codeUnauthenticated},
		{"unknown credential", bearer("controlled-unknown-token-00001"), http.StatusUnauthorized, codeUnauthenticated},
		{"another action", bearer(readerToken), http.StatusForbidden, codePermissionDenied},
		{"definition developer", bearer(developerToken), http.StatusForbidden, codePermissionDenied},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newCommandHarness(t, acceptingCommander(), true)
			recorder := h.submitLocalCheck(t, localCheckBody, testCase.authorization)
			requireEnvelope(t, recorder, testCase.status, testCase.code)
			if calls := h.commander.admitCalls.Load(); calls != 0 {
				t.Fatalf("a denied request reached admission %d times", calls)
			}
		})
	}
}

// TestLocalCheckRejectsInputOutsideItsContract covers the strict decoding rules
// the approved input states: exactly two members, no identity override, no
// arbitrary text, code or URL, and a 1,024-byte ceiling.
func TestLocalCheckRejectsInputOutsideItsContract(t *testing.T) {
	oversized := `{"commandId":"` + localCommand + strings.Repeat("x", config.LocalCheckBodyMaxBytes) + `","fixtureId":"plain-v1"}`

	for _, testCase := range []struct {
		name string
		body string
		code string
	}{
		{"unknown member", `{"commandId":"c-1","fixtureId":"plain-v1","tenantId":"tenant-fixture-1"}`, codeInvalidArgument},
		{"actor override", `{"commandId":"c-1","fixtureId":"plain-v1","actorId":"developer-fixture-1"}`, codeInvalidArgument},
		{"operation override", `{"commandId":"c-1","fixtureId":"plain-v1","operationId":"op-1"}`, codeInvalidArgument},
		{"duplicate member", `{"commandId":"c-1","commandId":"c-2","fixtureId":"plain-v1"}`, codeInvalidArgument},
		{"explicit null", `{"commandId":"c-1","fixtureId":null}`, codeInvalidArgument},
		{"missing fixtureId", `{"commandId":"c-1"}`, codeInvalidArgument},
		{"missing commandId", `{"fixtureId":"plain-v1"}`, codeInvalidArgument},
		{"unregistered fixture", `{"commandId":"c-1","fixtureId":"plain-v2"}`, codeInvalidArgument},
		{"arbitrary text", `{"commandId":"c-1","fixtureId":"AnvilKit"}`, codeInvalidArgument},
		{"source code", `{"commandId":"c-1","fixtureId":"export function Button(){}"}`, codeInvalidArgument},
		{"url", `{"commandId":"c-1","fixtureId":"https://example.invalid/fixture"}`, codeInvalidArgument},
		{"path", `{"commandId":"c-1","fixtureId":"../../etc/passwd"}`, codeInvalidArgument},
		{"wrong type", `{"commandId":1,"fixtureId":"plain-v1"}`, codeInvalidArgument},
		{"malformed commandId", `{"commandId":"../escape","fixtureId":"plain-v1"}`, codeInvalidArgument},
		{"not an object", `["plain-v1"]`, codeInvalidArgument},
		{"two documents", localCheckBody + localCheckBody, codeInvalidArgument},
		{"empty body", ``, codeInvalidArgument},
		{"above the ceiling", oversized, codeInvalidArgument},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newCommandHarness(t, acceptingCommander(), true)
			recorder := h.submitLocalCheck(t, testCase.body, bearer(localToken))
			requireEnvelope(t, recorder, http.StatusBadRequest, testCase.code)
			if calls := h.commander.admitCalls.Load(); calls != 0 {
				t.Fatalf("a rejected body reached admission %d times", calls)
			}
		})
	}

	t.Run("without the JSON content type", func(t *testing.T) {
		h := newCommandHarness(t, acceptingCommander(), true)
		recorder := h.post(t, routeLocalChecks, localCheckBody, bearer(localToken), "text/plain")
		requireEnvelope(t, recorder, http.StatusBadRequest, codeInvalidArgument)
	})

	t.Run("at the ceiling", func(t *testing.T) {
		if len(localCheckBody) > config.LocalCheckBodyMaxBytes {
			t.Fatalf("the accepted body is already above the ceiling")
		}
	})
}

// TestLocalCheckAcceptsBothRetainedFixtures proves the accepted acknowledgement
// and what the boundary forwards: the fixed kind, the API intake source, the
// selected fixture and an identity taken from the credential, never the body.
func TestLocalCheckAcceptsBothRetainedFixtures(t *testing.T) {
	for _, fixture := range []struct {
		name string
		body string
		want string
	}{
		{"plain", localCheckBody, "plain-v1"},
		{"newline", localCheckNewline, "newline-v1"},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			h := newCommandHarness(t, acceptingCommander(), true)
			recorder := h.submitLocalCheck(t, fixture.body, bearer(localToken))

			if recorder.Code != http.StatusAccepted {
				t.Fatalf("expected HTTP 202, got %d (%s)", recorder.Code, recorder.Body.String())
			}
			if location := recorder.Header().Get("Location"); location != "/v1/operations/"+localOperation {
				t.Fatalf("the acceptance points at %q", location)
			}
			body := decodeBody(t, recorder)
			for member, want := range map[string]any{
				"operationId":       localOperation,
				"operationRevision": "1",
				"acceptedAt":        "2026-09-10T09:00:00Z",
				"requestDigest":     localRequestHash,
				"queueExpiresAt":    "2026-09-10T09:15:00Z",
				"existing":          false,
			} {
				if body[member] != want {
					t.Fatalf("the acceptance reports %s=%v, want %v", member, body[member], want)
				}
			}

			sent := h.commander.admitted
			if sent.GetKind() != controlv1.OperationKind_OPERATION_KIND_LOCAL_CHECK {
				t.Fatalf("admission received kind %v", sent.GetKind())
			}
			if sent.GetIntakeSource() != controlv1.IntakeSource_INTAKE_SOURCE_API {
				t.Fatalf("admission received intake source %v", sent.GetIntakeSource())
			}
			if sent.GetLocalCheckFixtureId() != fixture.want {
				t.Fatalf("admission received fixture %q, want %q", sent.GetLocalCheckFixtureId(), fixture.want)
			}
			if sent.GetCommandId() != localCommand {
				t.Fatalf("admission received command %q", sent.GetCommandId())
			}
			// The local developer's tenant, not a tenant any body could name.
			if got := sent.GetContext().GetTenantId(); got != "tenant-fixture-3" {
				t.Fatalf("admission received tenant %q", got)
			}
			if got := sent.GetContext().GetActorId(); got != "developer-fixture-3" {
				t.Fatalf("admission received actor %q", got)
			}
			if got := sent.GetContext().GetDestinationMethod(); got != admitOperationProcedure {
				t.Fatalf("admission received destination %q", got)
			}
			// No activation, funding, subject or source revision is ever sent
			// for this kind.
			if sent.ActivationId != nil || sent.AuthorizedFundingRef != nil ||
				sent.OriginalSourceRevision != nil || len(sent.SubjectRefs) != 0 || len(sent.ApiSubjectRefs) != 0 {
				t.Fatal("admission received a business authority field for a local-check")
			}
		})
	}
}

// TestLocalCheckReplayReturnsTheOriginalIdentity proves that a retried command
// is not a second acceptance: it answers 200 with the original identity and
// revision, and claims no new Location.
func TestLocalCheckReplayReturnsTheOriginalIdentity(t *testing.T) {
	commander := &stubCommander{accepted: acceptedResponse(true)}
	h := newCommandHarness(t, commander, true)

	recorder := h.submitLocalCheck(t, localCheckBody, bearer(localToken))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 on replay, got %d (%s)", recorder.Code, recorder.Body.String())
	}
	if location := recorder.Header().Get("Location"); location != "" {
		t.Fatalf("a replay claimed a new Location %q", location)
	}
	body := decodeBody(t, recorder)
	if body["existing"] != true {
		t.Fatal("a replay does not report existing")
	}
	if body["operationId"] != localOperation || body["operationRevision"] != "1" {
		t.Fatalf("a replay changed the original identity: %v", body)
	}
	if body["requestDigest"] != localRequestHash {
		t.Fatal("a replay does not carry the original request digest")
	}
}

// TestLocalCheckConflictAndCapacity maps the two admission outcomes that the
// route's own contract makes unambiguous.
func TestLocalCheckConflictAndCapacity(t *testing.T) {
	t.Run("changed fixture under the same command", func(t *testing.T) {
		commander := &stubCommander{admitErr: connect.NewError(connect.CodeAborted, errAdmission)}
		h := newCommandHarness(t, commander, true)
		recorder := h.submitLocalCheck(t, localCheckNewline, bearer(localToken))
		requireEnvelope(t, recorder, http.StatusConflict, codeIdempotencyConflict)
	})

	t.Run("occupied local capacity", func(t *testing.T) {
		commander := &stubCommander{admitErr: connect.NewError(connect.CodeResourceExhausted, errAdmission)}
		h := newCommandHarness(t, commander, true)
		recorder := h.submitLocalCheck(t, localCheckBody, bearer(localToken))
		body := requireEnvelope(t, recorder, http.StatusTooManyRequests, codeOverloaded)
		if body["reason"] != overloadReasonQueueFull {
			t.Fatalf("the overload response names reason %v", body["reason"])
		}
		if body["retryAfterMs"] != float64(overloadRetryAfterMs) {
			t.Fatalf("the overload response carries retryAfterMs %v", body["retryAfterMs"])
		}
		if header := recorder.Header().Get("Retry-After"); header != strconv.Itoa(overloadRetryAfterMs/1000) {
			t.Fatalf("the overload response sets Retry-After %q", header)
		}
		requireMatchesSchema(t, "urn:anvilkit:error-envelope:v1", recorder.Body.Bytes())
	})
}

// TestLocalCheckUnknownIntakeIsNeverAnAcknowledgment proves that nothing short
// of a contract-shaped durable acceptance is reported as one.
func TestLocalCheckUnknownIntakeIsNeverAnAcknowledgment(t *testing.T) {
	incomplete := acceptedResponse(false)
	incomplete.RequestDigest = nil
	unidentified := acceptedResponse(false)
	unidentified.OperationId = nil

	for _, testCase := range []struct {
		name      string
		commander *stubCommander
	}{
		{"control unavailable", &stubCommander{admitErr: connect.NewError(connect.CodeUnavailable, errAdmission)}},
		{"intake timed out", &stubCommander{admitErr: connect.NewError(connect.CodeDeadlineExceeded, errAdmission)}},
		{"transport lost", &stubCommander{admitErr: errAdmission}},
		{"acceptance without a request digest", &stubCommander{accepted: incomplete}},
		{"acceptance without an identity", &stubCommander{accepted: unidentified}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newCommandHarness(t, testCase.commander, true)
			recorder := h.submitLocalCheck(t, localCheckBody, bearer(localToken))
			requireEnvelope(t, recorder, http.StatusServiceUnavailable, codeDependencyUnavailable)
			if recorder.Header().Get("Location") != "" {
				t.Fatal("an unconfirmed intake claimed an operation location")
			}
		})
	}
}

// TestCancelStaysAvailableAtOccupiedCapacity proves the reserved lane: the
// local slot being occupied refuses new intake and leaves cancellation, and its
// duplicate, working.
func TestCancelStaysAvailableAtOccupiedCapacity(t *testing.T) {
	commander := &stubCommander{
		admitErr:  connect.NewError(connect.CodeResourceExhausted, errAdmission),
		commanded: canceledResponse(false),
	}
	h := newCommandHarness(t, commander, true)

	requireEnvelope(t, h.submitLocalCheck(t, localCheckBody, bearer(localToken)),
		http.StatusTooManyRequests, codeOverloaded)

	recorder := h.post(t, "/v1/operations/"+localOperation+"/cancel",
		`{"commandId":"`+localCommand+`","expectedOperationRevision":"3","reasonCode":"CANCEL_REQUESTED"}`,
		bearer(localToken), "application/json")
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("cancellation answered %d at occupied capacity (%s)", recorder.Code, recorder.Body.String())
	}
	body := decodeBody(t, recorder)
	for member, want := range map[string]any{
		"operationId":         localOperation,
		"operationRevision":   "4",
		"controlState":        "running",
		"status":              "canceled",
		"reasonCode":          "CANCEL_REQUESTED",
		"fenceAcknowledgedAt": "2026-09-10T09:01:00Z",
		"coalesced":           false,
		"trackedCommandId":    localCommand,
	} {
		if body[member] != want {
			t.Fatalf("the cancellation result reports %s=%v, want %v", member, body[member], want)
		}
	}
	if got := commander.canceled.GetExpectedOperationRevision(); got != 3 {
		t.Fatalf("the reserved lane received expected revision %d", got)
	}
	if got := commander.canceled.GetContext().GetDestinationMethod(); got != cancelProcedure {
		t.Fatalf("the reserved lane received destination %q", got)
	}

	// The duplicate command coalesces onto the tracked one and still answers.
	commander.commanded = canceledResponse(true)
	duplicate := h.post(t, "/v1/operations/"+localOperation+"/cancel",
		`{"commandId":"`+localCommand+`","expectedOperationRevision":"3","reasonCode":"CANCEL_REQUESTED"}`,
		bearer(localToken), "application/json")
	if duplicate.Code != http.StatusAccepted {
		t.Fatalf("a duplicate cancellation answered %d", duplicate.Code)
	}
	if decodeBody(t, duplicate)["coalesced"] != true {
		t.Fatal("a duplicate cancellation does not report coalescing")
	}
}

// TestCancelRejectsInputOutsideItsContract holds the reserved lane to the same
// strict decoding as every other command.
func TestCancelRejectsInputOutsideItsContract(t *testing.T) {
	for _, testCase := range []struct {
		name string
		path string
		body string
	}{
		{"unknown member", "/v1/operations/" + localOperation + "/cancel", `{"commandId":"c-1","expectedOperationRevision":"3","force":true}`},
		{"duplicate member", "/v1/operations/" + localOperation + "/cancel", `{"commandId":"c-1","commandId":"c-2","expectedOperationRevision":"3"}`},
		{"explicit null", "/v1/operations/" + localOperation + "/cancel", `{"commandId":"c-1","expectedOperationRevision":null}`},
		{"missing revision", "/v1/operations/" + localOperation + "/cancel", `{"commandId":"c-1"}`},
		{"numeric revision", "/v1/operations/" + localOperation + "/cancel", `{"commandId":"c-1","expectedOperationRevision":3}`},
		{"negative revision", "/v1/operations/" + localOperation + "/cancel", `{"commandId":"c-1","expectedOperationRevision":"-1"}`},
		{"malformed reason", "/v1/operations/" + localOperation + "/cancel", `{"commandId":"c-1","expectedOperationRevision":"3","reasonCode":"cancel requested"}`},
		{"malformed operation", "/v1/operations/..%2fescape/cancel", `{"commandId":"c-1","expectedOperationRevision":"3"}`},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newCommandHarness(t, acceptingCommander(), true)
			recorder := h.post(t, testCase.path, testCase.body, bearer(localToken), "application/json")
			requireEnvelope(t, recorder, http.StatusBadRequest, codeInvalidArgument)
			if calls := h.commander.cancelCalls.Load(); calls != 0 {
				t.Fatalf("a rejected command reached the reserved lane %d times", calls)
			}
		})
	}
}

// TestCancelReportsOnlyWhatControlEstablished proves the mapping of the lane's
// failures, including the family-only conflict the private transport carries.
func TestCancelReportsOnlyWhatControlEstablished(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"conflicting state", connect.NewError(connect.CodeAborted, errAdmission), http.StatusConflict, codeAborted},
		{"unknown operation", connect.NewError(connect.CodeNotFound, errAdmission), http.StatusNotFound, codeNotFound},
		{"refused precondition", connect.NewError(connect.CodeFailedPrecondition, errAdmission), http.StatusConflict, codeChangeBlocked},
		{"control unavailable", connect.NewError(connect.CodeUnavailable, errAdmission), http.StatusServiceUnavailable, codeDependencyUnavailable},
		{"Control denied the current fixture scope", connect.NewError(connect.CodePermissionDenied, errAdmission), http.StatusForbidden, codePermissionDenied},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newCommandHarness(t, &stubCommander{cancelErr: testCase.err}, true)
			recorder := h.post(t, "/v1/operations/"+localOperation+"/cancel",
				`{"commandId":"`+localCommand+`","expectedOperationRevision":"3"}`,
				bearer(localToken), "application/json")
			body := requireEnvelope(t, recorder, testCase.status, testCase.code)
			// The failure names the operation it is about, so the reply and the
			// completion record identify the same operation.
			if body["operationId"] != localOperation {
				t.Fatalf("the failure envelope names operation %v", body["operationId"])
			}
			requireMatchesSchema(t, "urn:anvilkit:error-envelope:v1", recorder.Body.Bytes())
		})
	}
}

// TestUnsupportedLocalControlCommandsAreAbsent proves that hold, resume and
// definition changes are not served by this stage at all, which is the approved
// precondition behaviour for the fixed local profile.
func TestUnsupportedLocalControlCommandsAreAbsent(t *testing.T) {
	for _, path := range []string{
		"/v1/operations/" + localOperation + "/hold",
		"/v1/operations/" + localOperation + "/resume",
		"/v1/operations/definition-changes",
	} {
		t.Run(path, func(t *testing.T) {
			h := newCommandHarness(t, acceptingCommander(), true)
			recorder := h.post(t, path, `{"commandId":"c-1","expectedOperationRevision":"3"}`,
				bearer(localToken), "application/json")
			requireEnvelope(t, recorder, http.StatusNotFound, codeNotFound)
		})
	}
}

// TestLocalCheckMethodIsDeclaredOnce proves a non-POST method on the declared
// path is answered as an absent operation rather than by describing the surface.
func TestLocalCheckMethodIsDeclaredOnce(t *testing.T) {
	h := newCommandHarness(t, acceptingCommander(), true)
	request := httptest.NewRequest(http.MethodGet, routeLocalChecks, nil)
	request.Header.Set("Authorization", bearer(localToken))
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, request)
	requireEnvelope(t, recorder, http.StatusNotFound, codeNotFound)
}

// TestCommandRecordsCarryNoSensitiveContent holds the command surface to the
// logging contract: one client record per call attempt, the operation named,
// and no body, credential or fixture content anywhere in the stream.
func TestCommandRecordsCarryNoSensitiveContent(t *testing.T) {
	h := newCommandHarness(t, acceptingCommander(), true)
	if recorder := h.submitLocalCheck(t, localCheckBody, bearer(localToken)); recorder.Code != http.StatusAccepted {
		t.Fatalf("the fixture submission answered %d", recorder.Code)
	}

	records := h.logs.String()
	if strings.Count(records, logging.EventRPCClientCompleted) != 1 {
		t.Fatalf("expected one client record per call attempt:\n%s", records)
	}
	if !strings.Contains(records, admitOperationProcedure) {
		t.Fatal("the client record does not name the private method it called")
	}
	if !strings.Contains(records, localOperation) {
		t.Fatal("the completion record does not name the operation the request is about")
	}
	for _, forbidden := range []string{localToken, "Bearer", localCheckBody, "plain-v1"} {
		if strings.Contains(records, forbidden) {
			t.Fatalf("the log stream carries %q:\n%s", forbidden, records)
		}
	}
}

// TestAcceptedFixturesEqualTheRetainedEnum holds this service's accepted input
// to the parent contract rather than to a copy of it: the set the route admits
// must be exactly urn:anvilkit:agent-enums:v1#/$defs/localCheckFixtureId, so a
// fixture added or removed upstream cannot silently diverge here.
func TestAcceptedFixturesEqualTheRetainedEnum(t *testing.T) {
	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		t.Fatalf("verifying the retained contract inputs: %v", err)
	}
	var enums struct {
		Defs map[string]struct {
			Enum []string `json:"enum"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(inputs["agent-enums-v1.schema.json"], &enums); err != nil {
		t.Fatalf("decoding the retained enumerations: %v", err)
	}

	retained := enums.Defs["localCheckFixtureId"].Enum
	if len(retained) == 0 {
		t.Fatal("the retained enumerations declare no local-check fixture")
	}
	if len(retained) != len(localCheckFixtureIds) {
		t.Fatalf("this service admits %d fixtures, the contract declares %d", len(localCheckFixtureIds), len(retained))
	}
	for _, name := range retained {
		if _, admitted := localCheckFixtureIds[name]; !admitted {
			t.Fatalf("the contract declares fixture %q, which this service refuses", name)
		}
	}
}

// TestPublicEnumNamesEqualTheRetainedEnums holds the private-to-public
// translation to the parent contract. The maps are written out so a renamed or
// added private member cannot silently produce a public value no consumer
// recognizes; this proves they still cover exactly the shared enumerations.
func TestPublicEnumNamesEqualTheRetainedEnums(t *testing.T) {
	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		t.Fatalf("verifying the retained contract inputs: %v", err)
	}
	var enums struct {
		Defs map[string]struct {
			Enum []string `json:"enum"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal(inputs["agent-enums-v1.schema.json"], &enums); err != nil {
		t.Fatalf("decoding the retained enumerations: %v", err)
	}

	for _, mapping := range []struct {
		name     string
		declared []string
		produced map[string]struct{}
	}{
		{"controlState", enums.Defs["controlState"].Enum, valuesOf(controlStateNames)},
		{"publicStatus", enums.Defs["publicStatus"].Enum, valuesOf(publicStatusNames)},
	} {
		t.Run(mapping.name, func(t *testing.T) {
			if len(mapping.declared) == 0 {
				t.Fatalf("the retained enumerations declare no %s", mapping.name)
			}
			if len(mapping.declared) != len(mapping.produced) {
				t.Fatalf("this service produces %d %s values, the contract declares %d",
					len(mapping.produced), mapping.name, len(mapping.declared))
			}
			for _, member := range mapping.declared {
				if _, produced := mapping.produced[member]; !produced {
					t.Fatalf("the contract declares %s %q, which this service never produces", mapping.name, member)
				}
			}
		})
	}
}

func valuesOf[K comparable](mapping map[K]string) map[string]struct{} {
	out := make(map[string]struct{}, len(mapping))
	for _, value := range mapping {
		out[value] = struct{}{}
	}
	return out
}

// errAdmission stands for a private failure whose cause never reaches a public
// body. Only the RPC status is ever mapped.
var errAdmission = errorString("control refused the command")

type errorString string

func (e errorString) Error() string { return string(e) }
