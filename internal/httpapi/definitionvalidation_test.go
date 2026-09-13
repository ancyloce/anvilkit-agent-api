package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/contracts/fixtures"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"

	definitionvalidationv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/definitionvalidationv1"
	"github.com/ancyloce/anvilkit-agent-api/internal/contracts/definitionvalidationv1/definitionvalidationv1connect"
)

const (
	// developerToken holds definition.validate; readerToken does not. localToken
	// is the controlled local developer of the fixed local-check profile and is
	// the only identity holding local-check.create and operation.cancel.
	developerToken = "controlled-developer-token-0001"
	readerToken    = "controlled-reader-token-000001"
	localToken     = "controlled-local-token-0000001"

	fixtureDigest  = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	fixtureDigest2 = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func text(value string) *string { return &value }
func flag(value bool) *bool     { return &value }

// stubControl stands in for anvilkit-agent-control. The same type satisfies the
// client interface this service consumes and the generated handler interface, so
// one stub serves both the in-process and the real-transport tests.
type stubControl struct {
	calls    int
	received *definitionvalidationv1.ValidateDefinitionRequest
	response *definitionvalidationv1.ValidateDefinitionResponse
	err      error
}

func (s *stubControl) ValidateDefinition(
	_ context.Context,
	request *connect.Request[definitionvalidationv1.ValidateDefinitionRequest],
) (*connect.Response[definitionvalidationv1.ValidateDefinitionResponse], error) {
	s.calls++
	s.received = request.Msg
	if s.err != nil {
		return nil, s.err
	}
	return connect.NewResponse(s.response), nil
}

func invalidReportStub() *stubControl {
	return &stubControl{response: &definitionvalidationv1.ValidateDefinitionResponse{
		Valid:             flag(false),
		DescriptorDigest:  text(fixtureDigest),
		RuntimeProfileRef: text("fixture-runtime"),
		Issues: []*definitionvalidationv1.ValidationIssue{{
			Code:    text("PROFILE_QUALIFICATION_FAILED"),
			Path:    text(""),
			Message: text("Fixture runtime is not qualified"),
		}},
	}}
}

func validReportStub() *stubControl {
	return &stubControl{response: &definitionvalidationv1.ValidateDefinitionResponse{
		Valid:             flag(true),
		DefinitionDigest:  text(fixtureDigest2),
		DescriptorDigest:  text(fixtureDigest),
		RuntimeProfileRef: text("fixture-runtime"),
	}}
}

// harness wires the boundary to a controlled identity profile and a Control
// stand-in, capturing the log stream so records can be asserted on.
type harness struct {
	server  *Server
	handler http.Handler
	logs    *bytes.Buffer
}

func newHarness(t *testing.T, control DefinitionValidator) *harness {
	t.Helper()
	logs := &bytes.Buffer{}
	logger := logging.New(logs, logging.Identity{
		ServiceVersion:    "0.0.0-test",
		ServiceInstanceID: "test-instance",
		Environment:       "test",
	}, slog.LevelDebug)

	server := NewServer(Dependencies{Logger: logger, Identities: testProfile(t), Validator: control, ControlCallTimeout: 5 * time.Second})
	return &harness{server: server, handler: server.PublicHandler(), logs: logs}
}

func testProfile(t *testing.T) *identity.Profile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "identities.json")
	document := `{
      "identities": [
        {"token": "` + developerToken + `", "actorId": "developer-fixture-1",
         "tenantId": "tenant-fixture-1", "grantedActions": ["definition.validate"]},
        {"token": "` + readerToken + `", "actorId": "developer-fixture-2",
         "tenantId": "tenant-fixture-2", "grantedActions": ["operation.read"]},
        {"token": "` + localToken + `", "actorId": "developer-fixture-3",
         "tenantId": "tenant-fixture-3",
         "grantedActions": ["local-check.create", "operation.cancel", "operation.read", "component.prepare"]}
      ]
    }`
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatalf("writing the controlled profile: %v", err)
	}
	profile, err := identity.LoadProfile(path)
	if err != nil {
		t.Fatalf("loading the controlled profile: %v", err)
	}
	return profile
}

func (h *harness) post(t *testing.T, body, authorization string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, routeDefinitionValidations, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, request)
	return recorder
}

func bearer(token string) string { return "Bearer " + token }

func decodeBody(t *testing.T, recorder *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("the response body is not JSON: %v (%q)", err, recorder.Body.String())
	}
	return body
}

// requireEnvelope asserts a failure response and returns the decoded envelope.
func requireEnvelope(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) map[string]any {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("expected HTTP %d, got %d (%s)", status, recorder.Code, recorder.Body.String())
	}
	body := decodeBody(t, recorder)
	if body["code"] != code {
		t.Fatalf("expected code %s, got %v", code, body["code"])
	}
	if _, present := body["message"]; !present {
		t.Fatal("the envelope omits message")
	}
	retryable, present := body["retryable"].(bool)
	if !present {
		t.Fatal("the envelope omits retryable")
	}
	if want := errorFamilies[code].retryable; retryable != want {
		t.Fatalf("code %s must report retryable=%v, got %v", code, want, retryable)
	}
	if body["requestId"] != recorder.Header().Get("X-Request-Id") {
		t.Fatal("the envelope requestId and the X-Request-Id header disagree")
	}
	return body
}

// TestValidateDefinitionReturnsTheRetainedInvalidReport holds the response to
// the parent's own example: an invalid definition answers 200 with issues and
// no definitionDigest, and no operation identity appears anywhere.
func TestValidateDefinitionReturnsTheRetainedInvalidReport(t *testing.T) {
	var expected map[string]any
	if err := json.Unmarshal(fixtures.ValidateDefinitionInvalidReport, &expected); err != nil {
		t.Fatalf("the retained report fixture is unreadable: %v", err)
	}

	control := invalidReportStub()
	h := newHarness(t, control)
	recorder := h.post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d (%s)", recorder.Code, recorder.Body.String())
	}
	got := decodeBody(t, recorder)
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("the response does not reproduce the retained report.\n got: %v\nwant: %v", got, expected)
	}
	if _, present := got["definitionDigest"]; present {
		t.Fatal("an invalid definition must carry no definitionDigest")
	}
	if _, present := got["operationId"]; present {
		t.Fatal("validation must not produce an operation identity")
	}
	if control.calls != 1 {
		t.Fatalf("expected exactly one Control call, got %d", control.calls)
	}
}

func TestValidateDefinitionReturnsAValidReport(t *testing.T) {
	h := newHarness(t, validReportStub())
	recorder := h.post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d (%s)", recorder.Code, recorder.Body.String())
	}
	got := decodeBody(t, recorder)
	if got["valid"] != true {
		t.Fatalf("expected valid=true, got %v", got["valid"])
	}
	if got["definitionDigest"] != fixtureDigest2 {
		t.Fatalf("expected the digest Control returned, got %v", got["definitionDigest"])
	}
	issues, ok := got["issues"].([]any)
	if !ok || len(issues) != 0 {
		t.Fatalf("a valid report carries an empty issues array, got %v", got["issues"])
	}
}

// requestWith builds a validation request around an arbitrary definition body.
func requestWith(definition string) string {
	return `{"definition":` + definition +
		`,"descriptorDigest":"` + fixtureDigest +
		`","runtimeProfileRef":"fixture-runtime","policyRefs":["component-generation-pilot-v1"]}`
}

// TestValidateDefinitionForwardsTheSubmittedBytesUnchanged shows that presence
// and bounded numeric values survive the private call: the definition crosses as
// its exact submitted bytes, so no integer is widened, rounded or re-formatted.
func TestValidateDefinitionForwardsTheSubmittedBytesUnchanged(t *testing.T) {
	definition := `{"schemaVersion":1,` +
		`"beyondFloat64":9007199254740993,` +
		`"unsignedMax":"18446744073709551615",` +
		`"signedMin":-9223372036854775808,` +
		`"highPrecision":0.1000000000000000055511151231257827,` +
		`"exponent":1e3,` +
		`"trailingZero":1.50,` +
		`"emptyString":"",` +
		`"spaced":{ "inner" : [ 1 , 2 ] }}`

	control := invalidReportStub()
	h := newHarness(t, control)
	recorder := h.post(t, requestWith(definition), bearer(developerToken))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d (%s)", recorder.Code, recorder.Body.String())
	}

	if got := string(control.received.DefinitionJson); got != definition {
		t.Fatalf("the definition bytes changed in transit.\n got: %s\nwant: %s", got, definition)
	}
	if control.received.DescriptorDigest == nil || *control.received.DescriptorDigest != fixtureDigest {
		t.Fatal("descriptorDigest did not reach Control with its value present")
	}
	if control.received.RuntimeProfileRef == nil || *control.received.RuntimeProfileRef != "fixture-runtime" {
		t.Fatal("runtimeProfileRef did not reach Control with its value present")
	}
	if !reflect.DeepEqual(control.received.PolicyRefs, []string{"component-generation-pilot-v1"}) {
		t.Fatalf("policyRefs did not reach Control intact: %v", control.received.PolicyRefs)
	}
}

// TestValidateDefinitionRoundTripsOverRealConnectTransport exercises the
// generated bindings over a real HTTP/2 Connect connection rather than an
// in-process interface, so the wire encoding is part of the evidence.
func TestValidateDefinitionRoundTripsOverGRPC(t *testing.T) {
	control := invalidReportStub()
	mux := http.NewServeMux()
	mux.Handle(definitionvalidationv1connect.NewDefinitionValidationHandler(control))
	private := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") || r.Header.Get("Authorization") != "Bearer controlled-private-validation-token-0001" {
			http.Error(w, "private transport mismatch", http.StatusUnauthorized)
			return
		}
		mux.ServeHTTP(w, r)
	})
	backend := httptest.NewUnstartedServer(h2c.NewHandler(private, &http2.Server{}))
	backend.Start()
	defer backend.Close()

	client, err := NewControlClient(ControlClientConfig{Endpoint: backend.URL, DialTimeout: 5 * time.Second, ValidationToken: "controlled-private-validation-token-0001"})
	if err != nil {
		t.Fatalf("building the Control client: %v", err)
	}

	logs := &bytes.Buffer{}
	logger := logging.New(logs, logging.Identity{
		ServiceVersion: "0.0.0-test", ServiceInstanceID: "test-instance", Environment: "test",
	}, slog.LevelDebug)
	h := &harness{logs: logs}
	h.server = NewServer(Dependencies{Logger: logger, Identities: testProfile(t), Validator: client.Validation, ControlCallTimeout: 5 * time.Second})
	h.handler = h.server.PublicHandler()

	definition := `{"schemaVersion":1,"beyondFloat64":9007199254740993,"spaced":{ "inner" : true }}`
	recorder := h.post(t, requestWith(definition), bearer(developerToken))
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200 over the real transport, got %d (%s)", recorder.Code, recorder.Body.String())
	}
	if got := string(control.received.DefinitionJson); got != definition {
		t.Fatalf("the real transport changed the definition bytes.\n got: %s\nwant: %s", got, definition)
	}

	var expected map[string]any
	if err := json.Unmarshal(fixtures.ValidateDefinitionInvalidReport, &expected); err != nil {
		t.Fatalf("the retained report fixture is unreadable: %v", err)
	}
	if got := decodeBody(t, recorder); !reflect.DeepEqual(got, expected) {
		t.Fatalf("the real transport changed the report.\n got: %v\nwant: %v", got, expected)
	}
	if !strings.Contains(logs.String(), `"eventName":"rpc.client.completed"`) {
		t.Fatal("the executed Control call produced no completion record")
	}
}

func TestValidateDefinitionRequiresATrustedIdentityHoldingTheAction(t *testing.T) {
	cases := []struct {
		name          string
		authorization string
		status        int
		code          string
	}{
		{"no credential", "", http.StatusUnauthorized, codeUnauthenticated},
		{"wrong scheme", "Basic " + developerToken, http.StatusUnauthorized, codeUnauthenticated},
		{"unmapped credential", bearer("unmapped-token-000000000001"), http.StatusUnauthorized, codeUnauthenticated},
		{"actor without the action", bearer(readerToken), http.StatusForbidden, codePermissionDenied},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			control := invalidReportStub()
			h := newHarness(t, control)
			recorder := h.post(t, string(fixtures.ValidateDefinitionRequest), testCase.authorization)
			requireEnvelope(t, recorder, testCase.status, testCase.code)
			if control.calls != 0 {
				t.Fatal("a denied request must not reach Control")
			}
		})
	}
}

func TestValidateDefinitionDeniesWithoutAConfiguredProfile(t *testing.T) {
	control := invalidReportStub()
	logger := logging.New(&bytes.Buffer{}, logging.Identity{
		ServiceVersion: "0.0.0-test", ServiceInstanceID: "test-instance", Environment: "test",
	}, slog.LevelDebug)
	server := NewServer(Dependencies{Logger: logger, Validator: control, ControlCallTimeout: 5 * time.Second})
	h := &harness{server: server, handler: server.PublicHandler(), logs: &bytes.Buffer{}}

	recorder := h.post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))
	requireEnvelope(t, recorder, http.StatusUnauthorized, codeUnauthenticated)
	if control.calls != 0 {
		t.Fatal("an unconfigured profile must deny before any Control call")
	}
}

func TestValidateDefinitionRejectsUnusableRequests(t *testing.T) {
	oversized := requestWith(`{"schemaVersion":1,"pad":"` + strings.Repeat("x", config.RequestBodyMaxBytes) + `"}`)

	cases := []struct {
		name string
		body string
		code string
	}{
		{"duplicate top-level member", `{"definition":{"schemaVersion":1},"definition":{"schemaVersion":1},"descriptorDigest":"` + fixtureDigest + `","runtimeProfileRef":"fixture-runtime","policyRefs":["p1"]}`, codeInvalidArgument},
		{"duplicate member inside the definition", requestWith(`{"schemaVersion":1,"entry":"a","entry":"b"}`), codeInvalidArgument},
		{"explicit null member", `{"definition":{"schemaVersion":1},"descriptorDigest":null,"runtimeProfileRef":"fixture-runtime","policyRefs":["p1"]}`, codeInvalidArgument},
		{"explicit null inside the definition", requestWith(`{"schemaVersion":1,"entry":null}`), codeInvalidArgument},
		{"undeclared member", `{"definition":{"schemaVersion":1},"descriptorDigest":"` + fixtureDigest + `","runtimeProfileRef":"fixture-runtime","policyRefs":["p1"],"commandId":"c1"}`, codeInvalidArgument},
		{"missing member", `{"definition":{"schemaVersion":1},"descriptorDigest":"` + fixtureDigest + `","runtimeProfileRef":"fixture-runtime"}`, codeInvalidArgument},
		{"body above the ceiling", oversized, codeInvalidArgument},
		{"not json", `definition`, codeInvalidArgument},
		{"array at the root", `[{"definition":{"schemaVersion":1}}]`, codeInvalidArgument},
		{"malformed digest", `{"definition":{"schemaVersion":1},"descriptorDigest":"sha256:zz","runtimeProfileRef":"fixture-runtime","policyRefs":["p1"]}`, codeInvalidArgument},
		{"uppercase digest", `{"definition":{"schemaVersion":1},"descriptorDigest":"sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA","runtimeProfileRef":"fixture-runtime","policyRefs":["p1"]}`, codeInvalidArgument},
		{"malformed runtime profile", `{"definition":{"schemaVersion":1},"descriptorDigest":"` + fixtureDigest + `","runtimeProfileRef":".leading","policyRefs":["p1"]}`, codeInvalidArgument},
		{"empty policyRefs", `{"definition":{"schemaVersion":1},"descriptorDigest":"` + fixtureDigest + `","runtimeProfileRef":"fixture-runtime","policyRefs":[]}`, codeInvalidArgument},
		{"repeated policyRefs", `{"definition":{"schemaVersion":1},"descriptorDigest":"` + fixtureDigest + `","runtimeProfileRef":"fixture-runtime","policyRefs":["p1","p1"]}`, codeInvalidArgument},
		{"too many policyRefs", `{"definition":{"schemaVersion":1},"descriptorDigest":"` + fixtureDigest + `","runtimeProfileRef":"fixture-runtime","policyRefs":` + policyRefList(policyRefsMax+1) + `}`, codeInvalidArgument},
		{"definition is not an object", requestWith(`["schemaVersion"]`), codeInvalidArgument},
		{"definition omits schemaVersion", requestWith(`{"entry":"plan"}`), codeInvalidArgument},
		{"member of the wrong type", `{"definition":{"schemaVersion":1},"descriptorDigest":7,"runtimeProfileRef":"fixture-runtime","policyRefs":["p1"]}`, codeInvalidArgument},
		{"unsupported schema version", requestWith(`{"schemaVersion":2}`), codeUnsupportedSchema},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			control := invalidReportStub()
			h := newHarness(t, control)
			recorder := h.post(t, testCase.body, bearer(developerToken))
			requireEnvelope(t, recorder, http.StatusBadRequest, testCase.code)
			if control.calls != 0 {
				t.Fatal("a rejected request must not reach Control")
			}
		})
	}
}

func policyRefList(count int) string {
	references := make([]string, 0, count)
	for index := range count {
		references = append(references, `"policy-`+string(rune('a'+index%26))+string(rune('a'+index/26))+`"`)
	}
	return "[" + strings.Join(references, ",") + "]"
}

// TestValidateDefinitionMapsControlFailures pins the public consequence of each
// private failure family this route can meet.
func TestValidateDefinitionMapsControlFailures(t *testing.T) {
	cases := []struct {
		name   string
		code   connect.Code
		status int
		public string
	}{
		{"rejected input", connect.CodeInvalidArgument, http.StatusBadRequest, codeInvalidArgument},
		{"absent reference", connect.CodeNotFound, http.StatusNotFound, codeNotFound},
		{"incompatible profile", connect.CodeFailedPrecondition, http.StatusConflict, codeProfileQualification},
		{"unavailable", connect.CodeUnavailable, http.StatusServiceUnavailable, codeDependencyUnavailable},
		{"deadline exceeded", connect.CodeDeadlineExceeded, http.StatusServiceUnavailable, codeDependencyUnavailable},
		{"overloaded without a reason", connect.CodeResourceExhausted, http.StatusServiceUnavailable, codeDependencyUnavailable},
		{"internal", connect.CodeInternal, http.StatusServiceUnavailable, codeDependencyUnavailable},
		// Control refusing this service's own workload identity is a deployment
		// fault. It is never reflected back as the caller's authorization
		// problem, which would tell a permitted developer they lack permission.
		{"control refuses our credential", connect.CodeUnauthenticated, http.StatusServiceUnavailable, codeDependencyUnavailable},
		{"control denies our service", connect.CodePermissionDenied, http.StatusServiceUnavailable, codeDependencyUnavailable},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			control := &stubControl{err: connect.NewError(testCase.code, errors.New("control refused"))}
			h := newHarness(t, control)
			recorder := h.post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))
			body := requireEnvelope(t, recorder, testCase.status, testCase.public)

			if strings.Contains(strings.ToLower(body["message"].(string)), "control refused") {
				t.Fatal("the dependency's own error text must not reach the caller")
			}
			if testCase.status == http.StatusServiceUnavailable {
				if recorder.Header().Get("Retry-After") == "" {
					t.Fatal("a 503 must carry Retry-After")
				}
				if _, present := body["retryAfterMs"]; !present {
					t.Fatal("a 503 must carry retryAfterMs")
				}
			}
		})
	}
}

// TestValidateDefinitionReportsAnUnreachableControlAsUnavailable uses a real
// closed endpoint rather than a synthesized error.
func TestValidateDefinitionReportsAnUnreachableControlAsUnavailable(t *testing.T) {
	backend := httptest.NewUnstartedServer(h2c.NewHandler(http.NewServeMux(), &http2.Server{}))
	backend.Start()
	endpoint := backend.URL
	backend.Close()

	client, err := NewControlClient(ControlClientConfig{Endpoint: endpoint, DialTimeout: time.Second, ValidationToken: "controlled-private-validation-token-0001"})
	if err != nil {
		t.Fatalf("building the Control client: %v", err)
	}
	logger := logging.New(&bytes.Buffer{}, logging.Identity{
		ServiceVersion: "0.0.0-test", ServiceInstanceID: "test-instance", Environment: "test",
	}, slog.LevelDebug)
	server := NewServer(Dependencies{Logger: logger, Identities: testProfile(t), Validator: client.Validation, ControlCallTimeout: 2 * time.Second})
	h := &harness{server: server, handler: server.PublicHandler(), logs: &bytes.Buffer{}}

	recorder := h.post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))
	requireEnvelope(t, recorder, http.StatusServiceUnavailable, codeDependencyUnavailable)
}

// TestValidateDefinitionRefusesAReportOutsideItsContract keeps a dependency
// defect from becoming a public body the schema would reject.
func TestValidateDefinitionRefusesAReportOutsideItsContract(t *testing.T) {
	manyIssues := make([]*definitionvalidationv1.ValidationIssue, config.ValidationMaxIssues+1)
	for index := range manyIssues {
		manyIssues[index] = &definitionvalidationv1.ValidationIssue{
			Code: text("X"), Path: text(""), Message: text("m"),
		}
	}

	cases := map[string]*definitionvalidationv1.ValidateDefinitionResponse{
		"valid is absent": {DescriptorDigest: text(fixtureDigest), RuntimeProfileRef: text("fixture-runtime")},
		"valid without a digest": {
			Valid: flag(true), DescriptorDigest: text(fixtureDigest), RuntimeProfileRef: text("fixture-runtime")},
		"valid carrying issues": {
			Valid: flag(true), DefinitionDigest: text(fixtureDigest2), DescriptorDigest: text(fixtureDigest),
			RuntimeProfileRef: text("fixture-runtime"),
			Issues:            []*definitionvalidationv1.ValidationIssue{{Code: text("X"), Path: text(""), Message: text("m")}}},
		"invalid carrying a digest": {
			Valid: flag(false), DefinitionDigest: text(fixtureDigest2), DescriptorDigest: text(fixtureDigest),
			RuntimeProfileRef: text("fixture-runtime"),
			Issues:            []*definitionvalidationv1.ValidationIssue{{Code: text("X"), Path: text(""), Message: text("m")}}},
		"invalid without issues": {
			Valid: flag(false), DescriptorDigest: text(fixtureDigest), RuntimeProfileRef: text("fixture-runtime")},
		"more issues than the contract allows": {
			Valid: flag(false), DescriptorDigest: text(fixtureDigest), RuntimeProfileRef: text("fixture-runtime"),
			Issues: manyIssues},
		"issue omitting a member": {
			Valid: flag(false), DescriptorDigest: text(fixtureDigest), RuntimeProfileRef: text("fixture-runtime"),
			Issues: []*definitionvalidationv1.ValidationIssue{{Code: text("X"), Message: text("m")}}},
		"malformed descriptor digest": {
			Valid: flag(false), DescriptorDigest: text("not-a-digest"), RuntimeProfileRef: text("fixture-runtime"),
			Issues: []*definitionvalidationv1.ValidationIssue{{Code: text("X"), Path: text(""), Message: text("m")}}},
	}

	for name, response := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, &stubControl{response: response})
			recorder := h.post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))
			requireEnvelope(t, recorder, http.StatusServiceUnavailable, codeDependencyUnavailable)
		})
	}
}

func TestUndeclaredOperationsAreReportedAsAbsent(t *testing.T) {
	h := newHarness(t, invalidReportStub())
	cases := []struct {
		method string
		path   string
	}{
		{http.MethodGet, routeDefinitionValidations},
		{http.MethodDelete, routeDefinitionValidations},
		{http.MethodPost, "/v1/operations/generations"},
		{http.MethodGet, "/"},
		{http.MethodPost, "/anvilkit.control.v1.Control/AdmitOperation"},
	}
	for _, testCase := range cases {
		t.Run(testCase.method+" "+testCase.path, func(t *testing.T) {
			request := httptest.NewRequest(testCase.method, testCase.path, nil)
			request.Header.Set("Authorization", bearer(developerToken))
			recorder := httptest.NewRecorder()
			h.handler.ServeHTTP(recorder, request)
			requireEnvelope(t, recorder, http.StatusNotFound, codeNotFound)
		})
	}
}

func TestEveryResponseCarriesAServerAssignedRequestID(t *testing.T) {
	h := newHarness(t, invalidReportStub())

	request := httptest.NewRequest(http.MethodPost, routeDefinitionValidations,
		strings.NewReader(string(fixtures.ValidateDefinitionRequest)))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", bearer(developerToken))
	request.Header.Set("X-Request-Id", "client-supplied-identifier")
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, request)

	assigned := recorder.Header().Get("X-Request-Id")
	if assigned == "" {
		t.Fatal("every response carries X-Request-Id")
	}
	if assigned == "client-supplied-identifier" {
		t.Fatal("a client-supplied request identifier must not be adopted")
	}
	if !strings.HasPrefix(assigned, "req-") || len(assigned) > 128 {
		t.Fatalf("the request identifier is not a bounded values-v1 identifier: %q", assigned)
	}

	// A denied request carries one too.
	denied := h.post(t, string(fixtures.ValidateDefinitionRequest), "")
	if denied.Header().Get("X-Request-Id") == "" {
		t.Fatal("a denied response carries X-Request-Id")
	}
}

// TestLogsCarryNoCredentialOrRequestBody holds the boundary to the logging
// contract: records identify the exchange, never its content.
func TestLogsCarryNoCredentialOrRequestBody(t *testing.T) {
	h := newHarness(t, invalidReportStub())
	definition := `{"schemaVersion":1,"entry":"a-distinctive-definition-value"}`
	if recorder := h.post(t, requestWith(definition), bearer(developerToken)); recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", recorder.Code)
	}
	h.post(t, requestWith(definition), bearer("unmapped-token-000000000001"))

	recorded := h.logs.String()
	// schemaVersion is deliberately absent from this list: it is a required
	// field of the log record envelope itself, not submitted content.
	for _, forbidden := range []string{
		developerToken,
		"unmapped-token-000000000001",
		"a-distinctive-definition-value",
		"Bearer",
		fixtureDigest,
		"policyRefs",
		requestWith(definition),
	} {
		if strings.Contains(recorded, forbidden) {
			t.Fatalf("the log stream carries %q", forbidden)
		}
	}
	if !strings.Contains(recorded, `"eventName":"rpc.server.completed"`) {
		t.Fatal("an executed request produced no completion record")
	}
	if !strings.Contains(recorded, `"actorId":"developer-fixture-1"`) {
		t.Fatal("the completion record omits the resolved actor")
	}
	if occurrences := strings.Count(recorded, `"eventName":"rpc.server.completed"`); occurrences != 2 {
		t.Fatalf("expected one completion record per request, got %d for two requests", occurrences)
	}
}

// peerEnum is contracts/telemetry/log-record-v1.schema.json#/$defs/peer.
var peerEnum = map[string]bool{
	"anvilkit-agent-api": true, "anvilkit-agent-control": true, "anvilkit-agent-workflow": true,
	"anvilkit-agent-model-proxy": true, "anvilkit-component-codegen": true,
	"anvilkit-component-validator": true, "anvilkit-component-preview": true,
	"anvilkit-job-access-proxy": true, "anvilkit-export-worker": true, "browser": true,
	"studio-server-proxy": true, "candidate": true, "pagix-api": true, "model-provider": true,
	"artifact-api": true, "temporal": true, "kubernetes-api": true, "unauthenticated": true,
}

// TestLogRecordsSatisfyTheTelemetryContract holds every emitted record to the
// required envelope and to the conditional rules the log schema states for
// rpc.* events.
func TestLogRecordsSatisfyTheTelemetryContract(t *testing.T) {
	h := newHarness(t, invalidReportStub())
	h.post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))
	h.post(t, string(fixtures.ValidateDefinitionRequest), "")
	h.post(t, string(fixtures.ValidateDefinitionRequest), bearer(readerToken))

	request := httptest.NewRequest(http.MethodGet, "/v1/operations/unknown", nil)
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, request)

	lines := strings.Split(strings.TrimSpace(h.logs.String()), "\n")
	if len(lines) < 4 {
		t.Fatalf("expected a record per request, got %d", len(lines))
	}

	sawUnauthenticatedPeer := false
	for index, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("record %d is not JSON: %v", index, err)
		}

		for _, required := range []string{
			"schemaVersion", "timestamp", "severity", "eventName", "origin",
			"service.name", "service.version", "service.instance.id", "environment",
		} {
			if _, present := record[required]; !present {
				t.Fatalf("record %d omits the required field %s", index, required)
			}
		}
		if record["service.name"] != logging.ServiceName {
			t.Fatalf("record %d carries the wrong service name: %v", index, record["service.name"])
		}
		if timestamp, _ := record["timestamp"].(string); !strings.HasSuffix(timestamp, "Z") {
			t.Fatalf("record %d has a non-UTC timestamp: %v", index, record["timestamp"])
		}

		eventName, _ := record["eventName"].(string)
		if !strings.HasPrefix(eventName, "rpc.") {
			continue
		}

		// Every rpc.server.* record also names its request and trace origin.
		if strings.HasPrefix(eventName, "rpc.server.") {
			for _, required := range []string{"requestId", "trace.source"} {
				if _, present := record[required]; !present {
					t.Fatalf("record %d (%s) omits the required field %s", index, eventName, required)
				}
			}
			if record["trace.source"] == "linked" {
				if _, present := record["link.traceId"]; !present {
					t.Fatalf("record %d links a trace without link.traceId", index)
				}
			}
		}

		// Every rpc.* record names both peers, its protocol and its route.
		for _, required := range []string{"caller", "callee", "protocol", "routeTemplate"} {
			if _, present := record[required]; !present {
				t.Fatalf("record %d (%s) omits the required field %s", index, eventName, required)
			}
		}
		for _, field := range []string{"caller", "callee"} {
			if peer, _ := record[field].(string); !peerEnum[peer] {
				t.Fatalf("record %d (%s) has %s=%q, which is not a declared peer", index, eventName, field, peer)
			}
		}
		if eventName == logging.EventRPCClientCompleted {
			if _, present := record["transportAttempt"]; !present {
				t.Fatalf("record %d omits transportAttempt on a client call", index)
			}
		}

		// The unauthenticated peer is reserved for a denial that resolved no
		// actor, and must not carry an actor or tenant.
		if record["caller"] == "unauthenticated" {
			sawUnauthenticatedPeer = true
			if record["outcome"] != "denied" || record["error.code"] != codeUnauthenticated {
				t.Fatalf("record %d uses the unauthenticated peer without a matching denial: %v", index, record)
			}
			if _, present := record["actorId"]; present {
				t.Fatalf("record %d carries an actor with the unauthenticated peer", index)
			}
			if _, present := record["tenantId"]; present {
				t.Fatalf("record %d carries a tenant with the unauthenticated peer", index)
			}
		} else if record["error.code"] == codeUnauthenticated {
			t.Fatalf("record %d denies with UNAUTHENTICATED but names peer %v", index, record["caller"])
		}
	}

	if !sawUnauthenticatedPeer {
		t.Fatal("a request with no credential produced no unauthenticated-peer record")
	}
}

func TestDrainingReplicaRefusesNewRequests(t *testing.T) {
	control := invalidReportStub()
	h := newHarness(t, control)
	h.server.BeginDrain()

	recorder := h.post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))
	requireEnvelope(t, recorder, http.StatusServiceUnavailable, codeDependencyUnavailable)
	if control.calls != 0 {
		t.Fatal("a draining replica must not start new Control work")
	}

	private := httptest.NewRecorder()
	h.server.PrivateHandler().ServeHTTP(private, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if private.Code != http.StatusServiceUnavailable {
		t.Fatalf("a draining replica is not ready, got HTTP %d", private.Code)
	}
}

func TestHealthEndpointsReportSeparateConcerns(t *testing.T) {
	h := newHarness(t, invalidReportStub())
	private := h.server.PrivateHandler()

	for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusOK} {
		recorder := httptest.NewRecorder()
		private.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != want {
			t.Fatalf("%s returned HTTP %d, expected %d", path, recorder.Code, want)
		}
	}

	// Health is not reachable from the public listener.
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	requireEnvelope(t, recorder, http.StatusNotFound, codeNotFound)
}

func TestRetainedFixtureProvenanceIsPresent(t *testing.T) {
	var provenance struct {
		Source struct {
			Path   string `json:"path"`
			Digest string `json:"digest"`
		} `json:"source"`
		Files map[string]struct {
			ExampleKey string `json:"exampleKey"`
		} `json:"files"`
	}
	if err := json.Unmarshal(fixtures.Provenance, &provenance); err != nil {
		t.Fatalf("the fixture provenance is unreadable: %v", err)
	}
	if provenance.Source.Path == "" || !strings.HasPrefix(provenance.Source.Digest, "sha256:") {
		t.Fatal("the fixture provenance records no traceable source")
	}
	if len(provenance.Files) != 2 {
		t.Fatalf("expected provenance for both retained files, got %d", len(provenance.Files))
	}
}

func TestControlClientRejectsUnusableEndpoints(t *testing.T) {
	for _, endpoint := range []string{"", "://", "grpc://control", "control:8080"} {
		if _, err := NewControlClient(ControlClientConfig{Endpoint: endpoint, DialTimeout: time.Second, ValidationToken: "controlled-private-validation-token-0001"}); err == nil {
			t.Fatalf("expected %q to be refused", endpoint)
		}
	}
	if _, err := NewControlClient(ControlClientConfig{Endpoint: "https://control.internal:8443", DialTimeout: time.Second, ValidationToken: "controlled-private-validation-token-0001"}); err == nil {
		t.Fatal("a TLS endpoint without client identity must be refused")
	}
}

func TestValidationNumericSchemaVersion(t *testing.T) {
	for _, version := range []string{"1", "1.0", "1e0", "0.1e1", "1.00000000000000000000"} {
		t.Run(version, func(t *testing.T) {
			stub := validReportStub()
			definition := `{"schemaVersion":` + version + `}`
			response := newHarness(t, stub).post(t, requestWith(definition), bearer(developerToken))
			if response.Code != 200 || string(stub.received.DefinitionJson) != definition {
				t.Fatal("numeric version was refused or altered")
			}
		})
	}
	for _, version := range []string{`"1"`, "1.00000000000000000001", "1e999999999", "true"} {
		stub := validReportStub()
		response := newHarness(t, stub).post(t, requestWith(`{"schemaVersion":`+version+`}`), bearer(developerToken))
		if response.Code != 400 || stub.calls != 0 {
			t.Fatal("invalid version reached Control")
		}
	}
}

func TestValidationIssueCharacterBounds(t *testing.T) {
	stub := invalidReportStub()
	issue := stub.response.Issues[0]
	issue.Code, issue.Path, issue.Message = text(strings.Repeat("é", 128)), text(strings.Repeat("界", 1024)), text(strings.Repeat("😀", 512))
	response := newHarness(t, stub).post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))
	if response.Code != 200 {
		t.Fatal("contract-valid Unicode issue was refused")
	}
	issue.Message = text(strings.Repeat("😀", 513))
	response = newHarness(t, stub).post(t, string(fixtures.ValidateDefinitionRequest), bearer(developerToken))
	if response.Code != 503 {
		t.Fatal("oversized issue was accepted")
	}
}

// TestPublicTraceInputStartsANewTrace holds the boundary to the rule that a
// caller's trace context is a link at most, never this request's trace and never
// an identity.
func TestPublicTraceInputStartsANewTrace(t *testing.T) {
	const suppliedTrace = "4bf92f3577b34da6a3ce929d0e0e4736"

	cases := []struct {
		name        string
		traceparent string
		source      string
		link        string
	}{
		{"absent", "", "new", ""},
		{"well formed", "00-" + suppliedTrace + "-53ce929d0e0e4736-01", "linked", suppliedTrace},
		{"unsupported version", "01-" + suppliedTrace + "-53ce929d0e0e4736-01", "rejected", ""},
		{"zero trace id", "00-" + strings.Repeat("0", 32) + "-53ce929d0e0e4736-01", "rejected", ""},
		{"zero parent id", "00-" + suppliedTrace + "-0000000000000000-01", "rejected", ""},
		{"short trace id", "00-abcd-53ce929d0e0e4736-01", "rejected", ""},
		{"uppercase hex", "00-4BF92F3577B34DA6A3CE929D0E0E4736-53ce929d0e0e4736-01", "rejected", ""},
		{"not a context", "definitely-not-a-traceparent", "rejected", ""},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			h := newHarness(t, invalidReportStub())
			request := httptest.NewRequest(http.MethodPost, routeDefinitionValidations,
				strings.NewReader(string(fixtures.ValidateDefinitionRequest)))
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Authorization", bearer(developerToken))
			if testCase.traceparent != "" {
				request.Header.Set("traceparent", testCase.traceparent)
			}
			recorder := httptest.NewRecorder()
			h.handler.ServeHTTP(recorder, request)

			record := serverCompletionRecord(t, h)
			if record["trace.source"] != testCase.source {
				t.Fatalf("expected trace.source %q, got %v", testCase.source, record["trace.source"])
			}
			if testCase.link == "" {
				if _, present := record["link.traceId"]; present {
					t.Fatal("a record without a link must carry no link.traceId")
				}
			} else if record["link.traceId"] != testCase.link {
				t.Fatalf("expected link.traceId %q, got %v", testCase.link, record["link.traceId"])
			}
			// The caller's context never becomes this request's own trace.
			if record["traceId"] == suppliedTrace {
				t.Fatal("the caller's trace context was adopted as this request's trace")
			}
		})
	}
}

// serverCompletionRecord returns the rpc.server.completed record of the request
// just served. A request also emits a client record, so the stream is searched
// rather than read positionally.
func serverCompletionRecord(t *testing.T, h *harness) map[string]any {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("a log record is not JSON: %v", err)
		}
		if record["eventName"] == logging.EventRPCServerCompleted {
			return record
		}
	}
	t.Fatal("the request produced no rpc.server.completed record")
	return nil
}
