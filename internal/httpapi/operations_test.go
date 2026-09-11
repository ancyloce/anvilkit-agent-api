package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ancyloce/anvilkit-agent-api/internal/contracts"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"
	"github.com/ancyloce/anvilkit-agent-api/internal/readmodel"

	controlv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1"
)

// The fixture operation belongs to readerToken's tenant, which is the only
// identity in the controlled profile that holds operation.read.
const (
	fixtureOperation = "op-fixture-1"
	fixtureTenant    = "tenant-fixture-2"
	otherOperation   = "op-fixture-2"
)

// fakeProjection stands in for the database so the boundary's own rules are
// provable without one. Unit tests may not require PostgreSQL; the real queries
// are held to the schema by the integration tests instead.
type fakeProjection struct {
	mu sync.Mutex

	view       readmodel.OperationView
	viewErr    error
	events     []readmodel.Event
	eventsErr  error
	coveredSeq int64
	retained   int64

	steps        []readmodel.StepExecution
	snapshotMore bool
	pingErr      error

	reads     atomic.Int32
	afterRead func()
}

func (f *fakeProjection) ReadOperation(_ context.Context, tenantID, operationID string) (readmodel.OperationView, error) {
	if f.afterRead != nil {
		defer f.afterRead()
	}
	f.reads.Add(1)
	if f.viewErr != nil {
		return readmodel.OperationView{}, f.viewErr
	}
	if tenantID != fixtureTenant || operationID != f.view.OperationID {
		return readmodel.OperationView{}, readmodel.ErrOperationNotFound
	}
	return f.view, nil
}

func (f *fakeProjection) ReadEvents(
	_ context.Context, tenantID, operationID string, afterSeq int64, limit int,
) (readmodel.EventPage, error) {
	if f.afterRead != nil {
		defer f.afterRead()
	}
	f.reads.Add(1)
	if f.eventsErr != nil {
		return readmodel.EventPage{}, f.eventsErr
	}
	if tenantID != fixtureTenant || operationID != f.view.OperationID {
		return readmodel.EventPage{}, readmodel.ErrOperationNotFound
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	page := readmodel.EventPage{CoveredSeq: f.coveredSeq, RetainedFromSeq: f.retained}
	for _, event := range f.events {
		sequence, err := parseEventSeq(event.EventSeq)
		if err != nil || int64(sequence) <= afterSeq {
			continue
		}
		if len(page.Events) == limit {
			break
		}
		page.Events = append(page.Events, event)
	}
	return page, nil
}

func (f *fakeProjection) ReadSnapshot(
	_ context.Context, tenantID, operationID string, boundCoveredSeq int64, stepOffset int,
) (readmodel.Snapshot, bool, error) {
	if f.afterRead != nil {
		defer f.afterRead()
	}
	f.reads.Add(1)
	if tenantID != fixtureTenant || operationID != f.view.OperationID {
		return readmodel.Snapshot{}, false, readmodel.ErrOperationNotFound
	}
	if stepOffset > 0 && boundCoveredSeq != f.coveredSeq {
		return readmodel.Snapshot{}, false, readmodel.ErrSnapshotExpired
	}
	return readmodel.Snapshot{
		SchemaVersion: 1,
		OperationID:   operationID,
		CoveredSeq:    fmt.Sprint(f.coveredSeq),
		GeneratedAt:   time.Now().UTC().Format("2006-01-02T15:04:05Z"),
		Operation:     f.view,
		Steps:         f.steps,
	}, f.snapshotMore && stepOffset == 0, nil
}

func (f *fakeProjection) Ping(context.Context) error { return f.pingErr }

func (f *fakeProjection) appendEvent(event readmodel.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	if sequence, err := parseEventSeq(event.EventSeq); err == nil {
		f.coveredSeq = int64(sequence)
	}
}

// fakeDisclosure stands in for Control's authorization method.
type fakeDisclosure struct {
	decision  controlv1.DisclosureDecision
	scope     string
	freshFor  time.Duration
	err       error
	calls     atomic.Int32
	denyAfter int32
}

func (f *fakeDisclosure) GetDisclosureAuthorization(
	_ context.Context,
	request *connect.Request[controlv1.GetDisclosureAuthorizationRequest],
) (*connect.Response[controlv1.GetDisclosureAuthorizationResponse], error) {
	calls := f.calls.Add(1)
	if f.err != nil {
		return nil, f.err
	}

	decision := f.decision
	if f.denyAfter > 0 && calls > f.denyAfter {
		decision = controlv1.DisclosureDecision_DISCLOSURE_DECISION_DENY
	}
	scope := f.scope
	if scope == "" {
		scope = request.Msg.GetOperationId()
	}
	freshFor := f.freshFor
	if freshFor == 0 {
		freshFor = 30 * time.Second
	}
	freshUntil := timestamppb.New(time.Now().Add(freshFor))
	revision := uint64(7)
	return connect.NewResponse(&controlv1.GetDisclosureAuthorizationResponse{
		Decision:           &decision,
		BoundResourceScope: &scope,
		FreshUntil:         freshUntil,
		EvidenceRevision:   &revision,
	}), nil
}

func allowingDisclosure() *fakeDisclosure {
	return &fakeDisclosure{decision: controlv1.DisclosureDecision_DISCLOSURE_DECISION_ALLOW}
}

// syncBuffer collects log records written by request and stream goroutines
// while a test reads them, so an assertion about the record stream never races
// the connection that is still producing it.
type syncBuffer struct {
	mu      sync.Mutex
	written bytes.Buffer
}

func (b *syncBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written.Write(payload)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written.String()
}

// readHarness wires the authorized read surface to controlled stand-ins.
type readHarness struct {
	server     *Server
	handler    http.Handler
	logs       *syncBuffer
	projection *fakeProjection
	disclosure *fakeDisclosure
}

func newReadHarness(t *testing.T, projection *fakeProjection, disclosure *fakeDisclosure) *readHarness {
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
		Disclosure:         disclosure,
		ReadModel:          projection,
		ControlCallTimeout: 5 * time.Second,
	})
	server.readRoleSeenAt.Store(time.Now().UnixNano())
	return &readHarness{
		server: server, handler: server.PublicHandler(), logs: logs,
		projection: projection, disclosure: disclosure,
	}
}

func (h *readHarness) get(t *testing.T, path, authorization string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, path, nil)
	if authorization != "" {
		request.Header.Set("Authorization", authorization)
	}
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	recorder := httptest.NewRecorder()
	h.handler.ServeHTTP(recorder, request)
	return recorder
}

// fixtureProjection is one committed running generation with three events.
func fixtureProjection() *fakeProjection {
	return &fakeProjection{
		view: readmodel.OperationView{
			OperationID:       fixtureOperation,
			Kind:              "generation",
			Status:            "running",
			BusinessStage:     "generating",
			ControlState:      "running",
			CleanupState:      "not_required",
			FinancialState:    "reserved",
			CoveredSeq:        "3",
			OperationRevision: "2",
			DefinitionSegment: "1",
			CurrentStep:       "code",
			StepExecutionID:   "se-code-1",
			AcceptedAt:        "2026-09-08T10:15:00Z",
			FirstPermitAt:     "2026-09-08T10:16:00Z",
			ActiveDeadline:    "2026-09-08T10:36:00Z",
		},
		events:     fixtureEvents(1, 3),
		coveredSeq: 3,
		retained:   1,
	}
}

func fixtureEvents(from, to uint64) []readmodel.Event {
	events := make([]readmodel.Event, 0, to-from+1)
	for sequence := from; sequence <= to; sequence++ {
		events = append(events, readmodel.Event{
			SchemaVersion: 1,
			OperationID:   fixtureOperation,
			EventSeq:      fmt.Sprint(sequence),
			Type:          "operation.lifecycle",
			OccurredAt:    "2026-09-08T10:15:00Z",
			Payload: json.RawMessage(
				`{"status":"running","businessStage":"generating","operationRevision":"2","cleanupState":"not_required"}`),
		})
	}
	return events
}

// contractSchemas compiles the retained public schemas so a response body can
// be held to the parent contract rather than to this test's expectations.
func contractSchemas(t *testing.T) *jsonschema.Compiler {
	t.Helper()
	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		t.Fatalf("verifying the retained contract inputs: %v", err)
	}

	compiler := jsonschema.NewCompiler()
	compiler.UseLoader(retainedSchemasOnly{})
	compiler.AssertFormat()
	for name, raw := range inputs {
		if !strings.HasSuffix(name, ".schema.json") {
			continue
		}
		var document any
		if err := json.Unmarshal(raw, &document); err != nil {
			t.Fatalf("decoding retained %s: %v", name, err)
		}
		identifier, _ := document.(map[string]any)["$id"].(string)
		if err := compiler.AddResource(identifier, document); err != nil {
			t.Fatalf("registering retained %s: %v", name, err)
		}
	}
	return compiler
}

// retainedSchemasOnly refuses to resolve anything that did not travel with this
// clone, so a test can never silently validate against a network document.
type retainedSchemasOnly struct{}

func (retainedSchemasOnly) Load(string) (any, error) {
	return nil, errors.New("only retained schemas are resolvable")
}

func requireMatchesSchema(t *testing.T, uri string, body []byte) {
	t.Helper()
	schema, err := contractSchemas(t).Compile(uri)
	if err != nil {
		t.Fatalf("compiling %s: %v", uri, err)
	}
	var instance any
	if err := json.Unmarshal(body, &instance); err != nil {
		t.Fatalf("the response body is not JSON: %v", err)
	}
	if err := schema.Validate(instance); err != nil {
		t.Fatalf("the response does not satisfy %s: %v\n%s", uri, err, body)
	}
}

// TestOperationViewCarriesTheParentExampleLosslessly holds this service's view
// type to the parent's own example: every declared member survives a decode and
// re-encode, so a field cannot be silently dropped or renamed here.
func TestOperationViewCarriesTheParentExampleLosslessly(t *testing.T) {
	inputs, err := contracts.VerifiedInputs()
	if err != nil {
		t.Fatalf("verifying the retained contract inputs: %v", err)
	}

	var examples []json.RawMessage
	if err := json.Unmarshal(inputs["operation-view.example.json"], &examples); err != nil {
		t.Fatalf("decoding the retained view examples: %v", err)
	}
	if len(examples) == 0 {
		t.Fatal("the retained view examples are empty")
	}

	for index, example := range examples {
		var view readmodel.OperationView
		decoder := json.NewDecoder(bytes.NewReader(example))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&view); err != nil {
			t.Fatalf("example %d does not decode into the view type: %v", index, err)
		}
		encoded, err := json.Marshal(view)
		if err != nil {
			t.Fatalf("example %d does not re-encode: %v", index, err)
		}

		var original, round map[string]any
		mustUnmarshal(t, example, &original)
		mustUnmarshal(t, encoded, &round)

		// reconciliation is the one member no committed record available to the
		// API read role can supply, so it is compared separately and its
		// absence is the documented gap rather than a silent loss.
		delete(original, "reconciliation")
		delete(round, "reconciliation")
		if fmt.Sprint(original) != fmt.Sprint(round) {
			t.Fatalf("example %d does not survive the view type\n have %v\n want %v", index, round, original)
		}
	}
}

func mustUnmarshal(t *testing.T, raw []byte, target any) {
	t.Helper()
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatalf("decoding: %v", err)
	}
}

// TestReadOperationReturnsTheCommittedProjection checks that the served view is
// the committed record and satisfies the public schema.
func TestReadOperationReturnsTheCommittedProjection(t *testing.T) {
	h := newReadHarness(t, fixtureProjection(), allowingDisclosure())

	recorder := h.get(t, "/v1/operations/"+fixtureOperation, bearer(readerToken), nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d (%s)", recorder.Code, recorder.Body.String())
	}
	requireMatchesSchema(t, "urn:anvilkit:operation-view:v1#/$defs/OperationView", recorder.Body.Bytes())

	body := decodeBody(t, recorder)
	if body["operationId"] != fixtureOperation || body["coveredSeq"] != "3" {
		t.Fatalf("the view does not match the committed record: %v", body)
	}
	if _, present := body["changeState"]; present {
		t.Fatal("an inapplicable member must be omitted, never emitted as null or empty")
	}
	if h.disclosure.calls.Load() != 1 {
		t.Fatalf("expected exactly one disclosure read, got %d", h.disclosure.calls.Load())
	}
}

// TestProtectedReadsRequireTheOperationReadAction holds all three surfaces to
// the same identity rules, and proves a denied caller never reaches a
// projection.
func TestProtectedReadsRequireTheOperationReadAction(t *testing.T) {
	paths := []string{
		"/v1/operations/" + fixtureOperation,
		"/v1/operations/" + fixtureOperation + "/snapshot",
		"/v1/operations/" + fixtureOperation + "/events",
	}
	cases := []struct {
		name          string
		authorization string
		status        int
		code          string
	}{
		{"no credential", "", http.StatusUnauthorized, codeUnauthenticated},
		{"unknown credential", bearer("not-a-configured-token-0001"), http.StatusUnauthorized, codeUnauthenticated},
		{"wrong scheme", "Basic " + readerToken, http.StatusUnauthorized, codeUnauthenticated},
		{"action not held", bearer(developerToken), http.StatusForbidden, codePermissionDenied},
	}

	for _, path := range paths {
		for _, testCase := range cases {
			t.Run(path+"/"+testCase.name, func(t *testing.T) {
				h := newReadHarness(t, fixtureProjection(), allowingDisclosure())
				recorder := h.get(t, path, testCase.authorization, nil)
				requireEnvelope(t, recorder, testCase.status, testCase.code)
				if h.projection.reads.Load() != 0 {
					t.Fatal("a denied caller must not reach the read model")
				}
				if h.disclosure.calls.Load() != 0 {
					t.Fatal("an unauthenticated caller must not consume an authorization read")
				}
			})
		}
	}
}

// TestProtectedReadsStopWithoutCurrentEvidence covers every way disclosure can
// fail to authorize a read. Negative evidence denies; evidence that cannot be
// established fails closed as an unavailable dependency. Neither reaches a
// projection, and neither reveals whether the operation exists.
func TestProtectedReadsStopWithoutCurrentEvidence(t *testing.T) {
	cases := []struct {
		name       string
		disclosure *fakeDisclosure
		status     int
		code       string
	}{
		{
			"negative evidence",
			&fakeDisclosure{decision: controlv1.DisclosureDecision_DISCLOSURE_DECISION_DENY},
			http.StatusForbidden, codePermissionDenied,
		},
		{
			"undecided evidence",
			&fakeDisclosure{decision: controlv1.DisclosureDecision_DISCLOSURE_DECISION_UNSPECIFIED},
			http.StatusServiceUnavailable, codeDependencyUnavailable,
		},
		{
			"evidence bound to another resource",
			&fakeDisclosure{
				decision: controlv1.DisclosureDecision_DISCLOSURE_DECISION_ALLOW,
				scope:    otherOperation,
			},
			http.StatusServiceUnavailable, codeDependencyUnavailable,
		},
		{
			"grant inside the clock margin",
			&fakeDisclosure{
				decision: controlv1.DisclosureDecision_DISCLOSURE_DECISION_ALLOW,
				freshFor: time.Second,
			},
			http.StatusServiceUnavailable, codeDependencyUnavailable,
		},
		{
			"already expired grant",
			&fakeDisclosure{
				decision: controlv1.DisclosureDecision_DISCLOSURE_DECISION_ALLOW,
				freshFor: -time.Minute,
			},
			http.StatusServiceUnavailable, codeDependencyUnavailable,
		},
		{
			"unreachable authority",
			&fakeDisclosure{err: connect.NewError(connect.CodeUnavailable, errors.New("closed"))},
			http.StatusServiceUnavailable, codeDependencyUnavailable,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			h := newReadHarness(t, fixtureProjection(), testCase.disclosure)
			recorder := h.get(t, "/v1/operations/"+fixtureOperation, bearer(readerToken), nil)
			requireEnvelope(t, recorder, testCase.status, testCase.code)
			if h.projection.reads.Load() != 0 {
				t.Fatal("no projection may be read without current evidence")
			}
		})
	}
}

// TestReadOperationReportsAnUnseenOperationAsAbsent keeps another tenant's
// operation indistinguishable from one that does not exist.
func TestReadOperationReportsAnUnseenOperationAsAbsent(t *testing.T) {
	h := newReadHarness(t, fixtureProjection(), allowingDisclosure())
	recorder := h.get(t, "/v1/operations/"+otherOperation, bearer(readerToken), nil)
	body := requireEnvelope(t, recorder, http.StatusNotFound, codeNotFound)
	if strings.Contains(strings.ToLower(fmt.Sprint(body["message"])), "tenant") {
		t.Fatal("the absence message must not hint at another tenant")
	}
}

// TestReadEventsReplaysStrictlyAfterTheCursor holds replay to sequence order
// and to the numeric comparison the value contract requires.
func TestReadEventsReplaysStrictlyAfterTheCursor(t *testing.T) {
	projection := fixtureProjection()
	projection.events = fixtureEvents(1, 12)
	projection.coveredSeq = 12
	h := newReadHarness(t, projection, allowingDisclosure())

	recorder := h.get(t, "/v1/operations/"+fixtureOperation+"/events?afterSeq=9", bearer(readerToken), nil)
	if recorder.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d (%s)", recorder.Code, recorder.Body.String())
	}

	var page struct {
		OperationID     string            `json:"operationId"`
		Events          []json.RawMessage `json:"events"`
		CoveredSeq      string            `json:"coveredSeq"`
		RetainedFromSeq string            `json:"retainedFromSeq"`
	}
	mustUnmarshal(t, recorder.Body.Bytes(), &page)

	// Ten and above, not "10" > "9" lexicographically: a text comparison would
	// have returned nothing here.
	if len(page.Events) != 3 {
		t.Fatalf("expected events 10 to 12, got %d", len(page.Events))
	}
	if page.CoveredSeq != "12" || page.RetainedFromSeq != "1" {
		t.Fatalf("the page misreports its sequences: %+v", page)
	}
	for _, event := range page.Events {
		requireMatchesSchema(t, "urn:anvilkit:operation-event-payloads:v1", event)
	}
}

// TestReadEventsBoundsTheReplayPage refuses a page size outside the contract's
// ceiling rather than quietly clamping it.
func TestReadEventsBoundsTheReplayPage(t *testing.T) {
	h := newReadHarness(t, fixtureProjection(), allowingDisclosure())
	for _, limit := range []string{"0", "-1", "201", "not-a-number"} {
		t.Run(limit, func(t *testing.T) {
			recorder := h.get(t,
				"/v1/operations/"+fixtureOperation+"/events?limit="+limit, bearer(readerToken), nil)
			requireEnvelope(t, recorder, http.StatusBadRequest, codeInvalidArgument)
		})
	}
}

// TestReadEventsRejectsAnotherOperationsCursor covers the two cursor failures
// and proves the presented value never reaches a log field.
func TestReadEventsRejectsAnotherOperationsCursor(t *testing.T) {
	foreign := encodeEventCursor(otherOperation, 4)

	cases := []struct {
		name      string
		cursor    string
		status    int
		code      string
		wantPrint bool
	}{
		{"another operation", foreign, http.StatusConflict, codeRevisionConflict, true},
		{"not a cursor", "this-is-not-a-cursor!!", http.StatusBadRequest, codeInvalidArgument, true},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			h := newReadHarness(t, fixtureProjection(), allowingDisclosure())
			recorder := h.get(t, "/v1/operations/"+fixtureOperation+"/events", bearer(readerToken),
				map[string]string{"Last-Event-ID": testCase.cursor})
			requireEnvelope(t, recorder, testCase.status, testCase.code)

			logs := h.logs.String()
			if strings.Contains(logs, testCase.cursor) {
				t.Fatal("the presented cursor must never appear in a log field")
			}
			if testCase.wantPrint && !strings.Contains(logs, cursorFingerprint(testCase.cursor)) {
				t.Fatal("an unusable cursor must be recorded as its irreversible fingerprint")
			}
		})
	}
}

// TestSnapshotPagesBindOneCoveredSeq walks the handshake: the first page issues
// a cursor, the next page reuses the same bound sequence, and a projection that
// has moved on restarts the handshake instead of mixing two snapshots.
func TestSnapshotPagesBindOneCoveredSeq(t *testing.T) {
	projection := fixtureProjection()
	projection.snapshotMore = true
	projection.steps = []readmodel.StepExecution{{
		StepExecutionID:   "se-code-1",
		OperationID:       fixtureOperation,
		DefinitionSegment: "1",
		DefinitionDigest:  fixtureDigest,
		StepID:            "code",
		Visit:             "1",
		ActionID:          "component.code",
		ActionVersion:     "1.0.0",
		Status:            "started",
		StartedAt:         "2026-09-08T10:17:20Z",
		InputRefs:         json.RawMessage(`{}`),
		OutputRefs:        json.RawMessage(`{}`),
		CoveredSeq:        "3",
	}}
	h := newReadHarness(t, projection, allowingDisclosure())

	first := h.get(t, "/v1/operations/"+fixtureOperation+"/snapshot", bearer(readerToken), nil)
	if first.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d (%s)", first.Code, first.Body.String())
	}
	requireMatchesSchema(t, "urn:anvilkit:operation-snapshot:v1#/$defs/OperationSnapshotV1", first.Body.Bytes())

	body := decodeBody(t, first)
	cursor, present := body["nextStepsCursor"].(string)
	if !present || cursor == "" {
		t.Fatalf("expected a further page to be offered: %v", body)
	}
	if len(cursor) > maxStepsCursorLength {
		t.Fatalf("the steps cursor exceeds the parameter's ceiling: %d characters", len(cursor))
	}

	decoded, fault := decodeSnapshotCursor(cursor, fixtureOperation)
	if fault != nil {
		t.Fatalf("the issued cursor does not decode: %v", fault)
	}
	if decoded.CoveredSeq != projection.coveredSeq || decoded.StepOffset != len(projection.steps) {
		t.Fatalf("the cursor does not bind this snapshot: %+v", decoded)
	}

	next := h.get(t, "/v1/operations/"+fixtureOperation+"/snapshot?stepsCursor="+cursor, bearer(readerToken), nil)
	if next.Code != http.StatusOK {
		t.Fatalf("expected the next page, got %d (%s)", next.Code, next.Body.String())
	}
	if decodeBody(t, next)["coveredSeq"] != body["coveredSeq"] {
		t.Fatal("every page of one snapshot must report the same coveredSeq")
	}

	// The projection advances between pages; the handshake restarts rather than
	// serving a page from a snapshot that no longer exists.
	projection.appendEvent(fixtureEvents(4, 4)[0])
	advanced := h.get(t, "/v1/operations/"+fixtureOperation+"/snapshot?stepsCursor="+cursor, bearer(readerToken), nil)
	requireEnvelope(t, advanced, http.StatusConflict, codeRestartRequired)
}

// TestSnapshotCursorExpiresWithItsProtectionWindow refuses a page claimed after
// the window rather than serving one from an unprotected range.
func TestSnapshotCursorExpiresWithItsProtectionWindow(t *testing.T) {
	h := newReadHarness(t, fixtureProjection(), allowingDisclosure())
	stale := encodeSnapshotCursor(fixtureOperation, snapshotCursor{
		CoveredSeq:   3,
		IssuedAtUnix: time.Now().Add(-2 * time.Minute).Unix(),
		StepOffset:   1,
	})

	recorder := h.get(t, "/v1/operations/"+fixtureOperation+"/snapshot?stepsCursor="+stale, bearer(readerToken), nil)
	requireEnvelope(t, recorder, http.StatusConflict, codeRestartRequired)
	if h.projection.reads.Load() != 0 {
		t.Fatal("an expired handshake must not read a snapshot page")
	}
}

// TestSnapshotRejectsAnotherOperationsCursor keeps a snapshot cursor bound to
// the operation it was issued for.
func TestSnapshotRejectsAnotherOperationsCursor(t *testing.T) {
	h := newReadHarness(t, fixtureProjection(), allowingDisclosure())
	foreign := encodeSnapshotCursor(otherOperation, snapshotCursor{
		CoveredSeq: 3, IssuedAtUnix: time.Now().Unix(), StepOffset: 1,
	})
	recorder := h.get(t,
		"/v1/operations/"+fixtureOperation+"/snapshot?stepsCursor="+foreign, bearer(readerToken), nil)
	requireEnvelope(t, recorder, http.StatusConflict, codeRevisionConflict)
}

// TestProtectedReadsRejectAnUnusableOperationIdentifier keeps an arbitrary path
// segment from reaching Control or the database.
func TestProtectedReadsRejectAnUnusableOperationIdentifier(t *testing.T) {
	h := newReadHarness(t, fixtureProjection(), allowingDisclosure())
	recorder := h.get(t, "/v1/operations/"+strings.Repeat("x", 200), bearer(readerToken), nil)
	requireEnvelope(t, recorder, http.StatusBadRequest, codeInvalidArgument)
	if h.disclosure.calls.Load() != 0 {
		t.Fatal("an unusable identifier must not consume an authorization read")
	}
}

// TestCompletionRecordsCarryTheOperationIdentity holds the logging contract's
// rule that a record about an operation names it, without carrying a body.
func TestCompletionRecordsCarryTheOperationIdentity(t *testing.T) {
	h := newReadHarness(t, fixtureProjection(), allowingDisclosure())
	h.get(t, "/v1/operations/"+fixtureOperation, bearer(readerToken), nil)

	var found bool
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		var record map[string]any
		mustUnmarshal(t, []byte(line), &record)
		if record["eventName"] != logging.EventRPCServerCompleted {
			continue
		}
		found = true
		if record["operationId"] != fixtureOperation {
			t.Fatalf("the completion record omits the operation identity: %v", record)
		}
		if record["routeTemplate"] != "GET "+routeOperation {
			t.Fatalf("the record names %v rather than the declared route", record["routeTemplate"])
		}
	}
	if !found {
		t.Fatal("no completion record was emitted")
	}
}

// TestReadinessFollowsTheProjectionReadRole holds readiness to what this
// replica can serve: not draining, and a read role that answered recently.
// Control's reachability is deliberately not an input.
func TestReadinessFollowsTheProjectionReadRole(t *testing.T) {
	projection := fixtureProjection()
	h := newReadHarness(t, projection, allowingDisclosure())
	private := h.server.PrivateHandler()

	probe := func() int {
		recorder := httptest.NewRecorder()
		private.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/readyz", nil))
		return recorder.Code
	}

	if status := probe(); status != http.StatusOK {
		t.Fatalf("expected a ready replica, got %d", status)
	}

	// The read role stops answering; readiness follows within the contract's
	// window without any request having to fail first.
	h.server.readRoleSeenAt.Store(time.Now().Add(-time.Minute).UnixNano())
	if status := probe(); status != http.StatusServiceUnavailable {
		t.Fatalf("expected an unready replica, got %d", status)
	}

	projection.pingErr = nil
	h.server.probeReadRole(context.Background())
	if status := probe(); status != http.StatusOK {
		t.Fatalf("expected readiness to recover, got %d", status)
	}

	h.server.BeginDrain()
	if status := probe(); status != http.StatusServiceUnavailable {
		t.Fatalf("a draining replica is never ready, got %d", status)
	}
}

// A grant must still be current when the read finishes, including before SSE
// headers commit. The database delay crosses the real clock margin.
func TestProtectedReadsRejectExpiryDuringDatabaseRead(t *testing.T) {
	for _, suffix := range []string{"", "/snapshot", "/events", "/events?sse=true"} {
		t.Run(suffix, func(t *testing.T) {
			projection := fixtureProjection()
			projection.afterRead = func() { time.Sleep(40 * time.Millisecond) }
			disclosure := allowingDisclosure()
			disclosure.freshFor = 2*time.Second + 20*time.Millisecond
			h := newReadHarness(t, projection, disclosure)
			request := httptest.NewRequest(http.MethodGet, "/v1/operations/"+fixtureOperation+suffix, nil)
			request.Header.Set("Authorization", bearer(readerToken))
			if strings.Contains(suffix, "sse=true") {
				request.Header.Set("Accept", "text/event-stream")
			}
			response := httptest.NewRecorder()
			h.handler.ServeHTTP(response, request)
			requireEnvelope(t, response, 503, codeDependencyUnavailable)
		})
	}
}

func TestReplayCursorPreservesAllowedIdentifiersAndLargeSequences(t *testing.T) {
	operation := "fixture:operation:one"
	cursor := encodeEventCursor(operation, 17)
	if sequence, fault := decodeEventCursor(cursor, operation); fault != nil || sequence != 17 {
		t.Fatal("a colon in an allowed operation identifier broke its cursor")
	}
	h := newReadHarness(t, fixtureProjection(), allowingDisclosure())
	response := h.get(t, "/v1/operations/"+fixtureOperation+"/events?afterSeq=18446744073709551615", bearer(readerToken), nil)
	if response.Code != 200 {
		t.Fatal("a bounded uint64 cursor was refused")
	}
	var page eventPage
	mustUnmarshal(t, response.Body.Bytes(), &page)
	if len(page.Events) != 0 {
		t.Fatal("a cursor ahead of all stored events wrapped around and replayed older data")
	}
}
