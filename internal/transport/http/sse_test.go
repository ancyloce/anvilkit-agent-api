package http_test

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-contrib/sse"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	httptransport "github.com/ancyloce/anvilkit-agent-api/internal/transport/http"
)

// streamFake is a Control whose event stream is scripted per test: it can
// emit a burst of frames and then block until the caller's context ends,
// and it records how the stream finished.
type streamFake struct {
	fakeControl
	burst     int
	filler    string
	blockAt   int
	returned  atomic.Int32
	streamErr atomic.Value // streamResult
	emitted   atomic.Int32
}

type streamResult struct{ err error }

func (f *streamFake) StreamEvents(ctx context.Context, p application.Principal, id, after string, emit func(application.EventFrame) error) error {
	err := f.stream(ctx, p, id, emit)
	f.streamErr.Store(streamResult{err})
	f.returned.Add(1)
	return err
}

func (f *streamFake) result() error { r, _ := f.streamErr.Load().(streamResult); return r.err }

func (f *streamFake) stream(ctx context.Context, p application.Principal, id string, emit func(application.EventFrame) error) error {
	if _, err := f.GetOperation(ctx, p, id); err != nil {
		return err
	}
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	for i := 1; i <= f.burst; i++ {
		if f.blockAt > 0 && i > f.blockAt {
			<-ctx.Done()
			return ctx.Err()
		}
		frame := application.EventFrame{OperationID: id, EventSeq: itoa(i), TransitionID: "t" + itoa(i) + f.filler, EventType: "operation.changed", Revision: itoa(i), OccurredAt: now, Lifecycle: "running", Phase: "running", Control: "none", Cleanup: "pending", Finance: "not_funded"}
		if err := emit(frame); err != nil {
			return err
		}
		f.emitted.Add(1)
	}
	return nil
}

func itoa(i int) string { return strconv.Itoa(i) }

func streamOptions(bounds httptransport.StreamBounds) httptransport.Options {
	o := testOptions()
	o.Stream = bounds
	return o
}

func newStreamServer(t *testing.T, fake *streamFake, bounds httptransport.StreamBounds) (*httptest.Server, http.Handler) {
	t.Helper()
	fake.commands, fake.ops, fake.events = map[string]application.CommandIdentity{}, map[string]application.OperationView{}, map[string][]application.EventFrame{}
	srv, err := httptransport.NewServer(streamOptions(bounds), verifier{}, fake, func(context.Context) error { return nil })
	require.NoError(t, err)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	_, _, _ = do(t, ts, "POST", "/api/v1/operations", "token-tenant-a-0123456789", createBody("s1"))
	return ts, srv.Handler()
}

// rawGet opens the SSE request on a plain TCP connection so the test
// controls exactly when (and whether) the response body is read.
func rawGet(t *testing.T, ts *httptest.Server, path string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(ts.URL, "http://"))
	require.NoError(t, err)
	_, err = conn.Write([]byte("GET " + path + " HTTP/1.1\r\nHost: api\r\nAuthorization: Bearer token-tenant-a-0123456789\r\n\r\n"))
	require.NoError(t, err)
	return conn
}

func waitReturned(t *testing.T, fake *streamFake, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for fake.returned.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("the Control stream did not end within %s", within)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Every frame on the wire is what gin-contrib/sse encodes: a finite stream
// decodes with the library's own decoder into the same ids, event names and
// data documents the API was asked to emit.
func TestSSEFramesAreLibraryEncoded(t *testing.T) {
	fake := &streamFake{burst: 5}
	ts, _ := newStreamServer(t, fake, testOptions().Stream)
	resp := sseRequest(t, ts, "/api/v1/operations/op_s1/events", "token-tenant-a-0123456789", nil)
	defer resp.Body.Close()
	events, err := sse.Decode(resp.Body)
	require.NoError(t, err)
	require.Len(t, events, 5)
	for i, ev := range events {
		require.Equal(t, itoa(i+1), ev.Id)
		require.Equal(t, "operation.changed", ev.Event)
		data := ev.Data.(string)
		assertFrameSchema(t, "OperationEventFrame", data)
		require.Contains(t, data, `"eventSeq":"`+itoa(i+1)+`"`)
		require.Contains(t, data, `"transitionId":"t`+itoa(i+1)+`"`)
		require.NotContains(t, data, "failureCode", "absent optional members are omitted")
	}
	waitReturned(t, fake, time.Second)
	require.NoError(t, fake.result())
}

// A consumer that stops reading is disconnected once the bounded buffer
// stays full past the grace period; Control's stream is canceled and
// joined, and the operation itself is untouched (a fresh consumer streams).
func TestSSESlowConsumerIsDisconnectedWithinTheBound(t *testing.T) {
	bounds := httptransport.StreamBounds{HeartbeatInterval: time.Hour, FrameBuffer: 4, SlowConsumerGrace: 300 * time.Millisecond, WriteTimeout: 500 * time.Millisecond}
	fake := &streamFake{burst: 100000, filler: strings.Repeat("x", 8<<10)}
	ts, _ := newStreamServer(t, fake, bounds)
	conn := rawGet(t, ts, "/api/v1/operations/op_s1/events")
	defer conn.Close()

	waitReturned(t, fake, 15*time.Second)
	require.Error(t, fake.result(), "the producer was stopped, not left blocked")
	require.Less(t, int(fake.emitted.Load()), fake.burst, "the burst was cut short by the bound")

	// The server closed the connection: reading now ends with EOF after the buffered bytes.
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(10*time.Second)))
	reader := bufio.NewReader(conn)
	for {
		if _, err := reader.ReadString('\n'); err != nil {
			break
		}
	}

	// The operation is unaffected: a fresh consumer gets a complete stream.
	fresh := &streamFake{burst: 3}
	ts2, _ := newStreamServer(t, fresh, testOptions().Stream)
	resp := sseRequest(t, ts2, "/api/v1/operations/op_s1/events", "token-tenant-a-0123456789", nil)
	defer resp.Body.Close()
	events, err := sse.Decode(resp.Body)
	require.NoError(t, err)
	require.Len(t, events, 3)
}

// A client that disconnects mid-stream cancels Control's stream promptly;
// the handler returns without writing after the connection is gone.
func TestSSEClientDisconnectCancelsTheControlStream(t *testing.T) {
	bounds := httptransport.StreamBounds{HeartbeatInterval: 50 * time.Millisecond, FrameBuffer: 4, SlowConsumerGrace: time.Second, WriteTimeout: time.Second}
	fake := &streamFake{burst: 10, blockAt: 2}
	ts, _ := newStreamServer(t, fake, bounds)
	conn := rawGet(t, ts, "/api/v1/operations/op_s1/events")
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	reader := bufio.NewReader(conn)
	seen := 0
	for seen < 2 {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		if strings.HasPrefix(line, "id:") {
			seen++
		}
	}
	require.NoError(t, conn.Close())

	waitReturned(t, fake, 5*time.Second)
	require.ErrorIs(t, fake.result(), context.Canceled)
	require.Equal(t, int32(2), fake.emitted.Load())
}

// Heartbeats and frames share one writer: with a fast heartbeat and a
// producer that keeps emitting, every frame arrives intact and in order
// between keepalive comments (run with -race to prove the single writer).
func TestSSEHeartbeatsInterleaveWithFramesOnOneWriter(t *testing.T) {
	bounds := httptransport.StreamBounds{HeartbeatInterval: time.Millisecond, FrameBuffer: 2, SlowConsumerGrace: time.Second, WriteTimeout: time.Second}
	fake := &streamFake{burst: 300, filler: strings.Repeat("y", 512)}
	ts, _ := newStreamServer(t, fake, bounds)
	resp := sseRequest(t, ts, "/api/v1/operations/op_s1/events", "token-tenant-a-0123456789", nil)
	defer resp.Body.Close()
	frames, comments, err := readFrames(bufio.NewReader(resp.Body), func(liveFrame) bool { return false })
	require.Error(t, err, "the stream ends with EOF after the burst")
	require.Len(t, frames, 300, "all frames delivered")
	for i, f := range frames {
		require.Equal(t, itoa(i+1), f.ID, "frames arrive in order without interleaved bytes")
		require.True(t, strings.HasSuffix(f.Data, "}"), "a frame's data line is never split by a heartbeat: %q", f.Data[:40])
	}
	require.Greater(t, comments, 0, "heartbeats were written by the same writer")
	waitReturned(t, fake, time.Second)
}

// failingWriter is a response writer whose connection write fails after the
// headers: the handler must notice the failed write (sse.Encode itself
// reports none), stop, and cancel the producer.
type failingWriter struct {
	header   http.Header
	status   int
	writes   atomic.Int32
	deadline atomic.Int32
}

func (w *failingWriter) Header() http.Header              { return w.header }
func (w *failingWriter) WriteHeader(code int)             { w.status = code }
func (w *failingWriter) Flush()                           {}
func (w *failingWriter) SetWriteDeadline(time.Time) error { w.deadline.Add(1); return nil }
func (w *failingWriter) Write([]byte) (int, error) {
	w.writes.Add(1)
	return 0, errors.New("write tcp: broken pipe")
}

func TestSSEWriteFailureStopsTheStreamAndCancelsTheProducer(t *testing.T) {
	fake := &streamFake{burst: 10, blockAt: 3}
	_, handler := newStreamServer(t, fake, httptransport.StreamBounds{HeartbeatInterval: time.Hour, FrameBuffer: 4, SlowConsumerGrace: time.Second, WriteTimeout: time.Second})
	req := httptest.NewRequest("GET", "/api/v1/operations/op_s1/events", nil)
	req.Header.Set("Authorization", "Bearer token-tenant-a-0123456789")
	w := &failingWriter{header: http.Header{}}
	done := make(chan struct{})
	go func() { handler.ServeHTTP(w, req); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler did not return after the write failed")
	}
	require.Equal(t, http.StatusOK, w.status)
	require.Equal(t, int32(1), w.writes.Load(), "the first failed write ends the stream; nothing is written afterwards")
	require.GreaterOrEqual(t, w.deadline.Load(), int32(1), "every write is bounded by a deadline")
	waitReturned(t, fake, time.Second)
	require.ErrorIs(t, fake.result(), context.Canceled, "the producer was canceled and joined")
}

// Without write-deadline support the handler refuses to stream unbounded.
type recorderWithoutDeadline struct{ *httptest.ResponseRecorder }

func TestSSERequiresWriteDeadlineSupport(t *testing.T) {
	fake := &streamFake{burst: 3}
	_, handler := newStreamServer(t, fake, testOptions().Stream)
	req := httptest.NewRequest("GET", "/api/v1/operations/op_s1/events", nil)
	req.Header.Set("Authorization", "Bearer token-tenant-a-0123456789")
	rec := recorderWithoutDeadline{httptest.NewRecorder()}
	handler.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Empty(t, rec.Body.String(), "no frame is written to a connection whose writes cannot be bounded")
}
