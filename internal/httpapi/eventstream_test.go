package httpapi

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/readmodel"

	controlv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/controlv1"
)

// frameWait bounds how long a test waits for the next frame. It is longer than
// the shared catch-up interval so a live event has a tick to arrive in, and
// short enough that a stalled stream fails the test rather than the run.
const frameWait = 5 * time.Second

// sseFrame is one parsed server-sent event.
type sseFrame struct {
	Event   string
	ID      string
	Data    string
	Retry   string
	Comment string
}

// sseSession is a live subscription under test.
type sseSession struct {
	frames   chan sseFrame
	finished chan struct{}
	response *http.Response
	cancel   context.CancelFunc
}

func (s *sseSession) close() {
	s.cancel()
	if s.response != nil {
		_ = s.response.Body.Close()
	}
}

// next returns the next frame or fails the test.
func (s *sseSession) next(t *testing.T) sseFrame {
	t.Helper()
	select {
	case frame, open := <-s.frames:
		if !open {
			t.Fatal("the stream closed before the expected frame arrived")
		}
		return frame
	case <-time.After(frameWait):
		t.Fatal("timed out waiting for the next frame")
		return sseFrame{}
	}
}

// requireClosed asserts the server ended the stream.
func (s *sseSession) requireClosed(t *testing.T) {
	t.Helper()
	select {
	case <-s.finished:
	case <-time.After(frameWait):
		t.Fatal("timed out waiting for the stream to close")
	}
}

// subscribe opens one SSE subscription against a real HTTP server.
func subscribe(t *testing.T, base, operationID, token, lastEventID string) *sseSession {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	request, err := http.NewRequestWithContext(ctx, http.MethodGet,
		base+"/v1/operations/"+operationID+"/events", nil)
	if err != nil {
		cancel()
		t.Fatalf("building the subscription request: %v", err)
	}
	request.Header.Set("Accept", "text/event-stream")
	request.Header.Set("Authorization", bearer(token))
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
		t.Fatalf("expected HTTP 200 for the stream, got %d (%s)", response.StatusCode, body)
	}
	if mediaType := response.Header.Get("Content-Type"); !strings.HasPrefix(mediaType, "text/event-stream") {
		response.Body.Close()
		cancel()
		t.Fatalf("expected an event stream, got %q", mediaType)
	}

	session := &sseSession{
		frames:   make(chan sseFrame, 64),
		finished: make(chan struct{}),
		response: response,
		cancel:   cancel,
	}
	go readFrames(response.Body, session)
	t.Cleanup(session.close)
	return session
}

func readFrames(body io.Reader, session *sseSession) {
	defer close(session.finished)
	defer close(session.frames)

	scanner := bufio.NewScanner(body)
	var frame sseFrame
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			session.frames <- frame
			frame = sseFrame{}
		case strings.HasPrefix(line, ":"):
			frame.Comment = strings.TrimPrefix(line, ":")
		case strings.HasPrefix(line, "event: "):
			frame.Event = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "id: "):
			frame.ID = strings.TrimPrefix(line, "id: ")
		case strings.HasPrefix(line, "data: "):
			frame.Data = strings.TrimPrefix(line, "data: ")
		case strings.HasPrefix(line, "retry: "):
			frame.Retry = strings.TrimPrefix(line, "retry: ")
		}
	}
}

// streamHarness runs the read surface behind a real listener with its shared
// catch-up running, which is the only way the reconnect and close rules can be
// observed the way a browser sees them.
type streamHarness struct {
	*readHarness
	base string
}

func newStreamHarness(t *testing.T, projection *fakeProjection, disclosure *fakeDisclosure) *streamHarness {
	t.Helper()
	inner := newReadHarness(t, projection, disclosure)

	ctx, stop := context.WithCancel(context.Background())
	go inner.server.Hub().Run(ctx)

	backend := httptest.NewServer(inner.handler)
	t.Cleanup(func() {
		stop()
		backend.Close()
	})
	return &streamHarness{readHarness: inner, base: backend.URL}
}

// TestEventStreamReplaysThenStreamsLiveEvents covers the whole subscription:
// committed events replay in sequence order, each frame carries a resumable
// cursor, and an event committed after the subscription arrives through the
// shared catch-up.
func TestEventStreamReplaysThenStreamsLiveEvents(t *testing.T) {
	projection := fixtureProjection()
	h := newStreamHarness(t, projection, allowingDisclosure())

	session := subscribe(t, h.base, fixtureOperation, readerToken, "")
	for sequence := uint64(1); sequence <= 3; sequence++ {
		frame := session.next(t)
		requireDurableFrame(t, frame, sequence)
	}

	projection.appendEvent(fixtureEvents(4, 4)[0])
	h.server.Hub().Wake()
	requireDurableFrame(t, session.next(t), 4)
}

func requireDurableFrame(t *testing.T, frame sseFrame, sequence uint64) {
	t.Helper()
	if frame.Event != "operation.lifecycle" {
		t.Fatalf("expected the registered event type as the frame name, got %q", frame.Event)
	}

	// The id is opaque to a client but must locate exactly this event, so a
	// reconnect resumes from the position the client actually received.
	decoded, fault := decodeEventCursor(frame.ID, fixtureOperation)
	if fault != nil {
		t.Fatalf("the frame cursor does not decode: %v", fault)
	}
	if decoded != sequence {
		t.Fatalf("expected cursor for sequence %d, got %d", sequence, decoded)
	}

	var event map[string]any
	if err := json.Unmarshal([]byte(frame.Data), &event); err != nil {
		t.Fatalf("the frame data is not JSON: %v (%q)", err, frame.Data)
	}
	if event["eventSeq"] != fmt.Sprint(sequence) || event["operationId"] != fixtureOperation {
		t.Fatalf("the frame body does not carry its own stream key: %v", event)
	}
	requireMatchesSchema(t, "urn:anvilkit:operation-event-payloads:v1", []byte(frame.Data))
}

// TestEventStreamResumesAfterTheCursorItIssued reconnects with the cursor from
// a previous connection and receives only what follows it.
func TestEventStreamResumesAfterTheCursorItIssued(t *testing.T) {
	projection := fixtureProjection()
	projection.events = fixtureEvents(1, 6)
	projection.coveredSeq = 6
	h := newStreamHarness(t, projection, allowingDisclosure())

	first := subscribe(t, h.base, fixtureOperation, readerToken, "")
	var lastID string
	for sequence := uint64(1); sequence <= 6; sequence++ {
		frame := first.next(t)
		requireDurableFrame(t, frame, sequence)
		lastID = frame.ID
	}
	first.close()

	// Two further events are committed while nobody is connected, which is the
	// case a durable cursor exists for.
	projection.appendEvent(fixtureEvents(7, 7)[0])
	projection.appendEvent(fixtureEvents(8, 8)[0])

	resumed := subscribe(t, h.base, fixtureOperation, readerToken, lastID)
	requireDurableFrame(t, resumed.next(t), 7)
	requireDurableFrame(t, resumed.next(t), 8)

	// A reconnecting subscriber's decoded starting position equals the previous
	// connection's last sent position, which is what makes reconnection
	// continuity visible in the records without a cursor ever being logged.
	subscribedFrom, closedLastSent := awaitSubscriptionRecords(t, h.logs, 2, 1)
	if len(subscribedFrom) < 2 || subscribedFrom[1] != "6" {
		t.Fatalf("expected the reconnect to be recorded as fromSeq 6, got %v", subscribedFrom)
	}
	if closedLastSent[0] != subscribedFrom[1] {
		t.Fatalf("expected reconnection continuity, got lastSentSeq %v then fromSeq %v",
			closedLastSent, subscribedFrom)
	}
	if strings.Contains(h.logs.String(), lastID) {
		t.Fatal("the presented cursor must never appear in a log field")
	}
}

// awaitSubscriptionRecords waits for the expected subscription records, because
// a connection's closing record is written as that connection ends rather than
// when the client stops reading it.
func awaitSubscriptionRecords(t *testing.T, logs *syncBuffer, wantSubscribed, wantClosed int) (subscribedFrom, closedLastSent []string) {
	t.Helper()
	deadline := time.Now().Add(frameWait)
	for {
		subscribedFrom, closedLastSent = subscriptionRecords(t, logs.String())
		if len(subscribedFrom) >= wantSubscribed && len(closedLastSent) >= wantClosed {
			return subscribedFrom, closedLastSent
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d subscribed and %d closed records; saw %v and %v",
				wantSubscribed, wantClosed, subscribedFrom, closedLastSent)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func subscriptionRecords(t *testing.T, logs string) (subscribedFrom, closedLastSent []string) {
	t.Helper()
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		var record struct {
			EventName  string `json:"eventName"`
			Attributes struct {
				FromSeq     string `json:"fromSeq"`
				LastSentSeq string `json:"lastSentSeq"`
			} `json:"attributes"`
		}
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue
		}
		switch record.EventName {
		case "sse.subscribed":
			subscribedFrom = append(subscribedFrom, record.Attributes.FromSeq)
		case "sse.closed":
			closedLastSent = append(closedLastSent, record.Attributes.LastSentSeq)
		}
	}
	return subscribedFrom, closedLastSent
}

// TestEventStreamSendsSnapshotRequiredWhenTheCursorPredatesRetention refuses to
// skip a lost range and sends the client to the snapshot handshake instead.
func TestEventStreamSendsSnapshotRequiredWhenTheCursorPredatesRetention(t *testing.T) {
	projection := fixtureProjection()
	projection.events = fixtureEvents(5, 8)
	projection.coveredSeq = 8
	projection.retained = 5
	h := newStreamHarness(t, projection, allowingDisclosure())

	session := subscribe(t, h.base, fixtureOperation, readerToken, encodeEventCursor(fixtureOperation, 1))
	frame := session.next(t)
	if frame.Event != "snapshot-required" {
		t.Fatalf("expected the snapshot-required frame, got %q", frame.Event)
	}
	if frame.ID != "" {
		t.Fatal("a non-durable frame must carry no cursor")
	}

	var hint snapshotRequiredFrame
	if err := json.Unmarshal([]byte(frame.Data), &hint); err != nil {
		t.Fatalf("the frame data is not JSON: %v", err)
	}
	if hint.OperationID != fixtureOperation || hint.CoveredSeqHint != "8" {
		t.Fatalf("the handshake hint is not the committed position: %+v", hint)
	}
	session.requireClosed(t)

	if !strings.Contains(h.logs.String(), closeReasonRetentionRestart) {
		t.Fatal("the closed record must name the retention restart")
	}
}

// TestEventStreamClosesOnDrainWithAReconnectHint keeps a drained replica from
// cutting a client loose without guidance, and keeps the hint randomized so the
// connections it sheds do not return together.
func TestEventStreamClosesOnDrainWithAReconnectHint(t *testing.T) {
	h := newStreamHarness(t, fixtureProjection(), allowingDisclosure())

	session := subscribe(t, h.base, fixtureOperation, readerToken, "")
	for sequence := uint64(1); sequence <= 3; sequence++ {
		requireDurableFrame(t, session.next(t), sequence)
	}

	h.server.BeginDrain()
	frame := session.next(t)
	if frame.Retry == "" {
		t.Fatalf("a drained replica must send a reconnect hint, got %+v", frame)
	}

	pause, err := time.ParseDuration(frame.Retry + "ms")
	if err != nil {
		t.Fatalf("the reconnect hint is not a duration: %v", err)
	}
	if pause < config.ReconnectHintMin || pause > config.ReconnectHintMax {
		t.Fatalf("the reconnect hint %v is outside the contract's range", pause)
	}
	session.requireClosed(t)

	if !strings.Contains(h.logs.String(), closeReasonDrain) {
		t.Fatal("the closed record must name the drain")
	}
}

// TestEventStreamClosesWhenDisclosureIsRevoked holds a live stream to current
// evidence: a grant is renewed on its schedule, and a renewal that comes back
// negative ends the stream rather than letting it coast.
func TestEventStreamClosesWhenDisclosureIsRevoked(t *testing.T) {
	disclosure := &fakeDisclosure{
		decision:  controlv1.DisclosureDecision_DISCLOSURE_DECISION_ALLOW,
		freshFor:  3 * time.Second,
		denyAfter: 1,
	}
	h := newStreamHarness(t, fixtureProjection(), disclosure)

	session := subscribe(t, h.base, fixtureOperation, readerToken, "")
	for sequence := uint64(1); sequence <= 3; sequence++ {
		requireDurableFrame(t, session.next(t), sequence)
	}

	session.requireClosed(t)
	if calls := disclosure.calls.Load(); calls < 2 {
		t.Fatalf("expected the stream to renew its grant, got %d authorization reads", calls)
	}
	if !strings.Contains(h.logs.String(), closeReasonAuthorizationExpiry) {
		t.Fatal("the closed record must name the authorization expiry")
	}
}

// TestSubscriberBacklogStopsAdvancingAtItsBounds proves the bounded buffer
// never drops an event to make room. A full backlog simply stops advancing the
// subscriber's position, so the events stay committed and the subscriber
// resumes exactly where it stopped.
func TestSubscriberBacklogStopsAdvancingAtItsBounds(t *testing.T) {
	member := &subscriber{notify: make(chan struct{}, 1)}

	overflowing := make([]readmodel.Event, 0, config.SubscriberBufferEvents+50)
	for sequence := 1; sequence <= config.SubscriberBufferEvents+50; sequence++ {
		overflowing = append(overflowing, readmodel.Event{
			OperationID: fixtureOperation,
			EventSeq:    fmt.Sprint(sequence),
			Type:        "operation.lifecycle",
			Payload:     json.RawMessage(`{}`),
		})
	}
	member.deliver(overflowing)

	position, hasRoom := member.readPosition()
	if hasRoom {
		t.Fatal("a filled backlog must report no room")
	}
	if position != uint64(config.SubscriberBufferEvents) {
		t.Fatalf("the position must stop at the buffered bound, got %d", position)
	}
	if member.blockedFor(time.Now()) <= 0 {
		t.Fatal("a full backlog must start the slow-consumer clock")
	}

	// Draining restores room and clears the clock, so a consumer that catches
	// up is not disconnected for having been briefly behind.
	if drained := member.drain(); len(drained) != config.SubscriberBufferEvents {
		t.Fatalf("expected the whole backlog, got %d", len(drained))
	}
	if _, hasRoom := member.readPosition(); !hasRoom {
		t.Fatal("a drained backlog must accept events again")
	}
	if member.blockedFor(time.Now()) != 0 {
		t.Fatal("draining must stop the slow-consumer clock")
	}
}

// TestSharedCatchupServesEverySubscriberFromOneRead holds the rule that a
// browser costs a buffer, not a database read of its own.
func TestSharedCatchupServesEverySubscriberFromOneRead(t *testing.T) {
	projection := fixtureProjection()
	hub := newStreamHub(projection)

	first := hub.attach(fixtureOperation, fixtureTenant, 0)
	second := hub.attach(fixtureOperation, fixtureTenant, 0)
	third := hub.attach(fixtureOperation, fixtureTenant, 2)

	hub.catchUp(context.Background())
	if reads := projection.reads.Load(); reads != 1 {
		t.Fatalf("expected one shared read for three subscribers, got %d", reads)
	}

	if delivered := len(first.drain()); delivered != 3 {
		t.Fatalf("expected the first subscriber to receive three events, got %d", delivered)
	}
	if delivered := len(second.drain()); delivered != 3 {
		t.Fatalf("expected the second subscriber to receive three events, got %d", delivered)
	}
	// The third joined later and must receive only what follows its own
	// position, never the whole shared batch.
	if delivered := len(third.drain()); delivered != 1 {
		t.Fatalf("expected the later subscriber to receive one event, got %d", delivered)
	}
}

// TestSharedCatchupIgnoresABlockedSubscribersPosition keeps one browser that
// stopped reading from pinning the shared read and starving the others.
func TestSharedCatchupIgnoresABlockedSubscribersPosition(t *testing.T) {
	projection := fixtureProjection()
	projection.events = fixtureEvents(1, 12)
	projection.coveredSeq = 12
	hub := newStreamHub(projection)

	blocked := hub.attach(fixtureOperation, fixtureTenant, 0)
	blocked.queue = make([]readmodel.Event, config.SubscriberBufferEvents)
	blocked.blockedSince = time.Now()

	healthy := hub.attach(fixtureOperation, fixtureTenant, 10)
	hub.catchUp(context.Background())

	if delivered := len(healthy.drain()); delivered != 2 {
		t.Fatalf("expected the healthy subscriber to advance, got %d events", delivered)
	}
	if position, _ := blocked.readPosition(); position != 0 {
		t.Fatalf("a blocked subscriber must not advance, got %d", position)
	}
}

// TestHeartbeatFrameCarriesNoCursor keeps a transient keep-alive from looking
// like a resumable position.
func TestHeartbeatFrameCarriesNoCursor(t *testing.T) {
	recorder := httptest.NewRecorder()
	stream := newEventStreamWriter(recorder)
	if err := stream.writeHeartbeat(); err != nil {
		t.Fatalf("writing the heartbeat: %v", err)
	}
	if got := recorder.Body.String(); got != ":hb\n\n" {
		t.Fatalf("expected a bare comment line, got %q", got)
	}
}

func TestSubscriberByteLimitIncludesTheWholeNextEvent(t *testing.T) {
	member := &subscriber{notify: make(chan struct{}, 1)}
	events := fixtureEvents(1, 100)
	for i := range events {
		events[i].Payload = json.RawMessage(`{"reasonCode":"` + strings.Repeat("x", 20000) + `"}`)
	}
	member.deliver(events)
	if member.queuedBytes > config.SubscriberBufferBytes || member.position >= 100 {
		t.Fatal("the next event overflowed the byte ceiling")
	}
	if _, room := member.readPosition(); room {
		t.Fatal("the blocked buffer must not pin shared catch-up")
	}
	position := member.position
	member.drain()
	member.deliver(events)
	if member.position <= position {
		t.Fatal("draining did not resume from the original position")
	}
}

func TestReplayStopsWritingAtAuthorizationExpiry(t *testing.T) {
	recorder := httptest.NewRecorder()
	stream := newEventStreamWriter(recorder)
	stream.authorizationDeadline = time.Now().Add(-time.Second)
	var position uint64
	if err := stream.writeEvents(fixtureOperation, fixtureEvents(1, 3), &position); !errors.Is(err, errDisclosureExpired) {
		t.Fatalf("expected expiry, got %v", err)
	}
	if recorder.Body.Len() != 0 || position != 0 {
		t.Fatal("expired replay wrote protected data or advanced its cursor")
	}
}
