//go:build integration

package readmodel_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ancyloce/anvilkit-agent-api/internal/httpapi"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"
	"github.com/ancyloce/anvilkit-agent-api/internal/readmodel"

	controlv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1"
)

const readerToken = "controlled-reader-token-integration"

// allowingDisclosure isolates the read-model regression from Control. The
// parent api-disclosure proof exercises the real Control and API processes.
type allowingDisclosure struct{}

func (allowingDisclosure) GetDisclosureAuthorization(
	_ context.Context,
	request *connect.Request[controlv1.GetDisclosureAuthorizationRequest],
) (*connect.Response[controlv1.GetDisclosureAuthorizationResponse], error) {
	decision := controlv1.DisclosureDecision_DISCLOSURE_DECISION_ALLOW
	scope := request.Msg.GetOperationId()
	revision := uint64(1)
	return connect.NewResponse(&controlv1.GetDisclosureAuthorizationResponse{
		Decision:           &decision,
		BoundResourceScope: &scope,
		FreshUntil:         timestamppb.New(time.Now().Add(30 * time.Second)),
		EvidenceRevision:   &revision,
	}), nil
}

// recordBuffer collects log records while streams are still producing them.
type recordBuffer struct {
	mu      sync.Mutex
	written strings.Builder
}

func (b *recordBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written.Write(payload)
}

func (b *recordBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.written.String()
}

// apiReplica is one running API process backed by the real database.
type apiReplica struct {
	server  *httpapi.Server
	base    string
	logs    *recordBuffer
	stop    context.CancelFunc
	backend *httptest.Server
}

func (r *apiReplica) close() {
	r.stop()
	r.backend.Close()
}

// startReplica runs an API replica with its own read pool and shared catch-up,
// so a restart in a test is a real second process-equivalent rather than a
// rebound handler.
func startReplica(t *testing.T, dsn string, listen bool) *apiReplica {
	t.Helper()
	reader, err := readmodel.Open(context.Background(), dsn)
	if err != nil {
		t.Fatalf("opening the read model: %v", err)
	}

	logs := &recordBuffer{}
	logger := logging.New(logs, logging.Identity{
		ServiceVersion: "0.0.0-integration", ServiceInstanceID: "integration", Environment: "test",
	}, slog.LevelInfo)

	server := httpapi.NewServer(httpapi.Dependencies{
		Logger:             logger,
		Identities:         integrationProfile(t),
		Disclosure:         allowingDisclosure{},
		ReadModel:          reader,
		ControlCallTimeout: 5 * time.Second,
	})

	ctx, stop := context.WithCancel(context.Background())
	go server.Hub().Run(ctx)
	go server.WatchReadRole(ctx)
	if listen {
		hints := make(chan struct{}, 1)
		go reader.Listen(ctx, hints)
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case <-hints:
					server.Hub().Wake()
				}
			}
		}()
	}

	backend := httptest.NewServer(server.PublicHandler())
	replica := &apiReplica{server: server, base: backend.URL, logs: logs, backend: backend, stop: func() {
		stop()
		reader.Close()
	}}
	t.Cleanup(replica.close)
	return replica
}

func integrationProfile(t *testing.T) *identity.Profile {
	t.Helper()
	path := filepath.Join(t.TempDir(), "identities.json")
	document := `{"identities":[{"token":"` + readerToken + `","actorId":"developer-fixture-1",` +
		`"tenantId":"` + tenantA + `","grantedActions":["operation.read"]}]}`
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatalf("writing the controlled profile: %v", err)
	}
	profile, err := identity.LoadProfile(path)
	if err != nil {
		t.Fatalf("loading the controlled profile: %v", err)
	}
	return profile
}

// liveStream is one subscription under test.
type liveStream struct {
	response *http.Response
	reader   *bufio.Scanner
	cancel   context.CancelFunc
}

func (s *liveStream) close() {
	s.cancel()
	_ = s.response.Body.Close()
}

// openStream subscribes and returns the connection without consuming it, so a
// test can also choose not to read.
func openStream(t *testing.T, base, operationID, lastEventID string) *liveStream {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/v1/operations/"+operationID+"/events", nil)
	if err != nil {
		cancel()
		t.Fatalf("building the subscription: %v", err)
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Authorization", "Bearer "+readerToken)
	if lastEventID != "" {
		request.Header.Set("Last-Event-ID", lastEventID)
	}

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		cancel()
		t.Fatalf("subscribing: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		cancel()
		t.Fatalf("expected HTTP 200, got %d (%s)", response.StatusCode, body)
	}

	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	stream := &liveStream{response: response, reader: scanner, cancel: cancel}
	t.Cleanup(stream.close)
	return stream
}

// nextEvent reads frames until one carries a durable event, and returns its
// sequence and reconnect cursor.
func (s *liveStream) nextEvent(t *testing.T) (int64, string) {
	t.Helper()
	var cursor string
	for s.reader.Scan() {
		line := s.reader.Text()
		switch {
		case strings.HasPrefix(line, "id: "):
			cursor = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			var event struct {
				EventSeq string `json:"eventSeq"`
			}
			if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &event); err != nil {
				continue
			}
			if event.EventSeq == "" {
				continue
			}
			sequence, err := strconv.ParseInt(event.EventSeq, 10, 64)
			if err != nil {
				t.Fatalf("the event sequence is not a counter: %q", event.EventSeq)
			}
			return sequence, cursor
		}
	}
	t.Fatal("the stream ended before the next event arrived")
	return 0, ""
}

// requireGapless asserts the sequences a client assembled are 1..n in order.
func requireGapless(t *testing.T, received []int64, expected int) {
	t.Helper()
	if len(received) != expected {
		t.Fatalf("expected %d events, assembled %d: %v", expected, len(received), received)
	}
	for index, sequence := range received {
		if sequence != int64(index+1) {
			t.Fatalf("the assembled stream has a gap at position %d: %v", index, received)
		}
	}
}

// TestInterleavedCommitsAndReconnectsHaveNoGaps is the central recovery claim:
// events committed while a subscriber is connected, disconnected and
// reconnecting are delivered exactly once in order, with the durable cursor as
// the only thing carried across the break.
func TestInterleavedCommitsAndReconnectsHaveNoGaps(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)

	replica := startReplica(t, dsn, true)
	const total = 60

	// The writer commits throughout, so the subscriber's reconnect happens
	// against a moving projection rather than a quiesced one.
	var writing sync.WaitGroup
	writing.Add(1)
	go func() {
		defer writing.Done()
		for index := 0; index < total; index++ {
			commitEvent(t, admin, operationA, 0, index%2 == 0)
			time.Sleep(15 * time.Millisecond)
		}
	}()

	var (
		received []int64
		cursor   string
	)
	stream := openStream(t, replica.base, operationA, "")
	for len(received) < 20 {
		sequence, id := stream.nextEvent(t)
		received = append(received, sequence)
		cursor = id
	}
	stream.close()

	writing.Wait()

	resumed := openStream(t, replica.base, operationA, cursor)
	for len(received) < total {
		sequence, _ := resumed.nextEvent(t)
		received = append(received, sequence)
	}
	requireGapless(t, received, total)
}

// TestEventsArriveWithoutTheWakeHint proves the notification really is only a
// hint: nothing here sends one, and the shared catch-up delivers anyway.
func TestEventsArriveWithoutTheWakeHint(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)

	// The replica runs with no listener at all, which is the strongest form of
	// a lost notification.
	replica := startReplica(t, dsn, false)
	stream := openStream(t, replica.base, operationA, "")

	var received []int64
	for index := 0; index < 5; index++ {
		commitEvent(t, admin, operationA, 0, false)
		sequence, _ := stream.nextEvent(t)
		received = append(received, sequence)
	}
	requireGapless(t, received, 5)
}

// TestAnotherReplicaResumesFromTheDurableCursor covers an API restart: the
// process holding the connection goes away entirely, and a different replica
// with its own pool and catch-up continues the same stream from the cursor.
func TestAnotherReplicaResumesFromTheDurableCursor(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)
	for index := 0; index < 8; index++ {
		commitEvent(t, admin, operationA, 0, false)
	}

	first := startReplica(t, dsn, true)
	stream := openStream(t, first.base, operationA, "")

	var (
		received []int64
		cursor   string
	)
	for len(received) < 4 {
		sequence, id := stream.nextEvent(t)
		received = append(received, sequence)
		cursor = id
	}

	// The replica that issued the cursor is gone before the client returns.
	stream.close()
	first.close()

	second := startReplica(t, dsn, true)
	resumed := openStream(t, second.base, operationA, cursor)
	for len(received) < 8 {
		sequence, _ := resumed.nextEvent(t)
		received = append(received, sequence)
	}
	requireGapless(t, received, 8)
}

// TestStatusSnapshotAndEventsAgree holds the three surfaces to one committed
// position: a snapshot's coveredSeq, the view's coveredSeq and the replay
// page's coveredSeq describe the same operation state.
func TestStatusSnapshotAndEventsAgree(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)
	for index := 1; index <= 6; index++ {
		sequence := commitEvent(t, admin, operationA, 0, false)
		seedStep(t, admin, operationA, index, sequence)
	}

	replica := startReplica(t, dsn, false)
	view := getJSON(t, replica.base+"/v1/operations/"+operationA)
	snapshot := getJSON(t, replica.base+"/v1/operations/"+operationA+"/snapshot")
	page := getJSON(t, replica.base+"/v1/operations/"+operationA+"/events")

	if view["coveredSeq"] != "6" {
		t.Fatalf("the view reports coveredSeq %v", view["coveredSeq"])
	}
	if snapshot["coveredSeq"] != view["coveredSeq"] || page["coveredSeq"] != view["coveredSeq"] {
		t.Fatalf("the three surfaces disagree: view %v, snapshot %v, page %v",
			view["coveredSeq"], snapshot["coveredSeq"], page["coveredSeq"])
	}
	if steps, ok := snapshot["steps"].([]any); !ok || len(steps) != 6 {
		t.Fatalf("the snapshot does not carry the committed steps: %v", snapshot["steps"])
	}
	if events, ok := page["events"].([]any); !ok || len(events) != 6 {
		t.Fatalf("the replay page does not carry the committed events: %v", page["events"])
	}
}

func getJSON(t *testing.T, address string) map[string]any {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, address, nil)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	request.Header.Set("Authorization", "Bearer "+readerToken)

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("requesting %s: %v", address, err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading %s: %v", address, err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("expected HTTP 200 from %s, got %d (%s)", address, response.StatusCode, body)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("%s did not return JSON: %v", address, err)
	}
	return decoded
}

// TestASubscriberThatStopsReadingIsDisconnected exercises the slow-consumer
// bound against a real socket. It waits out the contract's thirty-second
// window, so it is the longest check in this suite by design.
func TestASubscriberThatStopsReadingIsDisconnected(t *testing.T) {
	admin, dsn := provision(t)
	seedOperation(t, admin, operationA, tenantA)

	replica := startReplica(t, dsn, false)
	stream := openStream(t, replica.base, operationA, "")

	// Enough committed bytes to fill the subscriber backlog, the socket buffers
	// and anything in between, so the connection is genuinely stuck rather than
	// merely idle.
	for index := 0; index < 400; index++ {
		commitEvent(t, admin, operationA, 20000, false)
	}

	// The client stops reading here. The server holds a bounded backlog, stops
	// advancing this subscriber's position, and disconnects it once it has been
	// unable to drain for the contract's bound.
	t.Log("waiting out the slow-consumer bound; this check is intentionally slow")
	time.Sleep(35 * time.Second)

	drained := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, stream.response.Body)
		drained <- err
	}()

	select {
	case err := <-drained:
		// The server ends a chunked stream by closing it, so the client sees
		// either a clean end or a truncated body. Either way the connection is
		// gone, which is what the bound promises.
		t.Logf("the stream ended after the slow-consumer bound: %v", err)
	case <-time.After(30 * time.Second):
		t.Fatal("the server did not disconnect a subscriber that stopped reading")
	}

	// A blocked write must also stop at the current authorization deadline,
	// which can precede the thirty-second slow-consumer deadline.
	if logs := replica.logs.String(); !strings.Contains(logs, "slow_consumer") && !strings.Contains(logs, "authorization_expired") {
		t.Fatal("the closed record must name the bound that stopped the blocked write")
	}

	// The operation is untouched: a disconnected slow consumer never cancels
	// the work it was watching.
	view := getJSON(t, replica.base+"/v1/operations/"+operationA)
	if view["status"] != "running" {
		t.Fatalf("a slow consumer must not change the operation, got %v", view["status"])
	}
	if fmt.Sprint(view["coveredSeq"]) != "400" {
		t.Fatalf("expected the committed position to stand, got %v", view["coveredSeq"])
	}
}
