package httpapi

import (
	"context"
	"sync"
	"time"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/readmodel"
)

// Close reasons recorded when a subscription ends. They are log values, not
// wire frames: the only non-durable frame the contract defines is
// snapshot-required, and a drain additionally sets the SSE retry field.
const (
	closeReasonClientGone          = "client_gone"
	closeReasonAuthorizationExpiry = "authorization_expired"
	closeReasonSlowConsumer        = "slow_consumer"
	closeReasonDrain               = "drain"
	closeReasonRetentionRestart    = "retention_restart"
)

// EventReader is the slice of the read model a subscription needs.
type EventReader interface {
	ReadEvents(ctx context.Context, tenantID, operationID string, afterSeq int64, limit int) (readmodel.EventPage, error)
}

// ProjectionReader is everything the authorized read surface reads.
//
// It exists so the boundary's own rules — disclosure, cursors, page bounds,
// close reasons — are provable without a database, which the delivery order
// requires of unit tests. *readmodel.ReadModel is its only production
// implementation.
type ProjectionReader interface {
	EventReader
	ReadOperation(ctx context.Context, tenantID, operationID string) (readmodel.OperationView, error)
	ReadSnapshot(
		ctx context.Context,
		tenantID, operationID string,
		boundCoveredSeq int64,
		stepOffset int,
	) (readmodel.Snapshot, bool, error)
	Ping(ctx context.Context) error
}

// streamHub owns the catch-up that every subscriber of an operation shares.
//
// One goroutine reads for the whole process: a browser costs a bounded buffer
// and a writer, never a database connection or a polling loop of its own. A
// NOTIFY hint only shortens the wait before the next read, so a lost hint costs
// latency and never completeness.
type streamHub struct {
	reader EventReader
	wake   chan struct{}

	mu      sync.Mutex
	streams map[string]*operationStream
}

// operationStream is the set of subscribers reading one operation.
type operationStream struct {
	operationID string
	tenantID    string
	subscribers map[*subscriber]struct{}
}

// subscriber is one connection's bounded backlog and position.
//
// The backlog is bounded in both events and bytes. When it is full the
// subscriber's position simply stops advancing: the events stay committed in
// the database, so a subscriber that drains resumes exactly where it stopped
// and no event is dropped to make room. A subscriber that stays full past the
// slow-consumer bound is disconnected and recovers through replay.
type subscriber struct {
	mu             sync.Mutex
	queue          []readmodel.Event
	queuedBytes    int
	position       uint64
	blockedSince   time.Time
	closeReason    string
	coveredSeqHint int64
	notify         chan struct{}
}

func newStreamHub(reader EventReader) *streamHub {
	return &streamHub{
		reader:  reader,
		wake:    make(chan struct{}, 1),
		streams: map[string]*operationStream{},
	}
}

// Wake asks for an immediate catch-up. A pending request is as good as several,
// because the read it triggers returns everything committed.
func (h *streamHub) Wake() {
	select {
	case h.wake <- struct{}{}:
	default:
	}
}

// Run drives the shared catch-up until ctx ends.
func (h *streamHub) Run(ctx context.Context) {
	ticker := time.NewTicker(config.SharedCatchup)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-h.wake:
		}
		h.catchUp(ctx)
	}
}

// attach registers a subscriber that has already replayed up to position.
func (h *streamHub) attach(operationID, tenantID string, position uint64) *subscriber {
	member := &subscriber{position: position, notify: make(chan struct{}, 1)}

	h.mu.Lock()
	defer h.mu.Unlock()
	stream, known := h.streams[operationID]
	if !known {
		stream = &operationStream{
			operationID: operationID,
			tenantID:    tenantID,
			subscribers: map[*subscriber]struct{}{},
		}
		h.streams[operationID] = stream
	}
	stream.subscribers[member] = struct{}{}
	return member
}

func (h *streamHub) detach(operationID string, member *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	stream, known := h.streams[operationID]
	if !known {
		return
	}
	delete(stream.subscribers, member)
	if len(stream.subscribers) == 0 {
		delete(h.streams, operationID)
	}
}

// catchUp reads one bounded batch for every operation that has a subscriber
// accepting events, then fans it out.
func (h *streamHub) catchUp(ctx context.Context) {
	h.mu.Lock()
	pending := make([]*operationStream, 0, len(h.streams))
	for _, stream := range h.streams {
		pending = append(pending, stream)
	}
	h.mu.Unlock()

	for _, stream := range pending {
		if ctx.Err() != nil {
			return
		}
		h.catchUpStream(ctx, stream)
	}
}

func (h *streamHub) catchUpStream(ctx context.Context, stream *operationStream) {
	h.mu.Lock()
	members := make([]*subscriber, 0, len(stream.subscribers))
	for member := range stream.subscribers {
		members = append(members, member)
	}
	h.mu.Unlock()

	// A subscriber whose backlog is full is not waiting for more events, so it
	// is left out of the read window. Without that, one blocked browser would
	// pin the shared read at its own position and starve every other
	// subscriber of the same operation.
	lowest, accepting := uint64(0), false
	for _, member := range members {
		position, hasRoom := member.readPosition()
		if !hasRoom {
			continue
		}
		if !accepting || position < lowest {
			lowest, accepting = position, true
		}
	}
	if !accepting {
		return
	}

	page, err := h.reader.ReadEvents(ctx, stream.tenantID, stream.operationID, storedEventSequence(lowest), config.ReplayPageEvents)
	if err != nil {
		// A failed catch-up is retried on the next tick. The events stay
		// committed, so nothing is lost by waiting.
		return
	}

	for _, member := range members {
		position, hasRoom := member.readPosition()
		if !hasRoom {
			continue
		}
		if page.SnapshotRequired(storedEventSequence(position)) {
			// Retention advanced past this subscriber's position while it was
			// connected. Skipping the gap is never allowed, so the connection
			// restarts through the snapshot handshake instead.
			member.requestSnapshotRestart(page.CoveredSeq)
			continue
		}
		member.deliver(page.Events)
	}

	if len(page.Events) == config.ReplayPageEvents {
		// The batch was full, so more is already committed. Ask for another
		// pass rather than waiting out the interval.
		h.Wake()
	}
}

// readPosition reports the subscriber's last delivered sequence and whether its
// backlog still has room for more.
func (s *subscriber) readPosition() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.position, s.closeReason == "" && s.hasRoomLocked()
}

func (s *subscriber) hasRoomLocked() bool {
	return len(s.queue) < config.SubscriberBufferEvents && s.queuedBytes < config.SubscriberBufferBytes && s.blockedSince.IsZero()
}

// deliver appends the events after this subscriber's position, up to its
// bounds. Its position advances only for events actually buffered.
func (s *subscriber) deliver(events []readmodel.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, event := range events {
		sequence, err := parseEventSeq(event.EventSeq)
		if err != nil || sequence <= s.position {
			continue
		}
		encoded, err := marshalEvent(event)
		if err != nil || !s.hasRoomLocked() || s.queuedBytes+len(encoded) > config.SubscriberBufferBytes {
			if s.blockedSince.IsZero() {
				s.blockedSince = time.Now()
			}
			break
		}
		s.queue = append(s.queue, event)
		s.queuedBytes += len(encoded)
		s.position = sequence
	}
	s.signalLocked()
}

// drain removes and returns the buffered events.
func (s *subscriber) drain() []readmodel.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	buffered := s.queue
	s.queue, s.queuedBytes = nil, 0
	s.blockedSince = time.Time{}
	return buffered
}

// blockedFor reports how long the backlog has been full.
func (s *subscriber) blockedFor(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.blockedSince.IsZero() {
		return 0
	}
	return now.Sub(s.blockedSince)
}

func (s *subscriber) pendingClose() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeReason
}

func (s *subscriber) signalLocked() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// requestSnapshotRestart ends the connection through the snapshot handshake and
// carries the sequence the client should recover to.
func (s *subscriber) requestSnapshotRestart(coveredSeqHint int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closeReason == "" {
		s.closeReason, s.coveredSeqHint = closeReasonRetentionRestart, coveredSeqHint
	}
	s.signalLocked()
}

func (s *subscriber) restartHint() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.coveredSeqHint
}
