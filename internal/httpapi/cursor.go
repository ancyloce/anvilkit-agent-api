package httpapi

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
)

// Cursors locate a position in committed data. They never carry authorization:
// every read that uses one still requires a current disclosure grant bound to
// the operation, so a guessed or copied cursor discloses nothing.
//
// Both forms are base64url text without padding. They are opaque in the sense
// the contract means — a client must not parse or construct one, and the event
// body already carries operationId and eventSeq for deduplication — but they
// carry no secret and need no shared key, so any replica decodes a cursor
// issued by any other. That is what lets a drained connection resume elsewhere.

// maxCursorLength bounds a presented cursor before it is decoded, matching the
// Agent OpenAPI's Last-Event-ID and stepsCursor parameter ceilings.
const maxCursorLength = 256

// maxStepsCursorLength is the stepsCursor parameter's own ceiling.
const maxStepsCursorLength = 128

var cursorEncoding = base64.RawURLEncoding

// encodeEventCursor renders the reconnect cursor for one delivered event.
func encodeEventCursor(operationID string, eventSeq uint64) string {
	return cursorEncoding.EncodeToString([]byte(operationID + ":" + strconv.FormatUint(eventSeq, 10)))
}

// decodeEventCursor resolves a presented reconnect cursor to its sequence and
// confirms it belongs to expectedOperationID.
//
// A cursor of another operation is a conflict rather than a rejection of the
// request's shape: the client holds a valid position in a stream it is not
// reading, and the contract answers that with REVISION_CONFLICT.
func decodeEventCursor(presented, expectedOperationID string) (uint64, *clientFault) {
	if len(presented) > maxCursorLength {
		return 0, newFault(codeInvalidArgument, "the reconnect cursor exceeds the accepted length")
	}
	decoded, err := cursorEncoding.DecodeString(presented)
	if err != nil {
		return 0, newFault(codeInvalidArgument, "the reconnect cursor is not a cursor this service issued")
	}
	separator := strings.LastIndexByte(string(decoded), ':')
	if separator < 0 {
		return 0, newFault(codeInvalidArgument, "the reconnect cursor is not a cursor this service issued")
	}
	operationID, sequence := string(decoded[:separator]), string(decoded[separator+1:])
	if operationID != expectedOperationID {
		return 0, newFault(codeRevisionConflict, "the reconnect cursor belongs to another operation")
	}
	eventSeq, err := strconv.ParseUint(sequence, 10, 64)
	if err != nil {
		return 0, newFault(codeInvalidArgument, "the reconnect cursor does not decode to an event sequence")
	}
	return eventSeq, nil
}

// snapshotCursor is the position of one page inside a bound snapshot.
type snapshotCursor struct {
	// CoveredSeq is the sequence every page of this snapshot binds.
	CoveredSeq int64
	// IssuedAtUnix starts the snapshot protection window.
	IssuedAtUnix int64
	// StepOffset is the number of steps already delivered.
	StepOffset int
}

// encodeSnapshotCursor renders the cursor for the next snapshot page. The
// operation is bound by a short fingerprint rather than its identifier so the
// cursor stays inside the parameter's length ceiling for any legal identifier.
func encodeSnapshotCursor(operationID string, cursor snapshotCursor) string {
	return cursorEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%d:%d:%d",
		operationFingerprint(operationID), cursor.CoveredSeq, cursor.IssuedAtUnix, cursor.StepOffset)))
}

// decodeSnapshotCursor resolves a presented stepsCursor and confirms it was
// issued for this operation.
func decodeSnapshotCursor(presented, expectedOperationID string) (snapshotCursor, *clientFault) {
	malformed := newFault(codeInvalidArgument, "the steps cursor is not a cursor this service issued")

	if len(presented) > maxStepsCursorLength {
		return snapshotCursor{}, newFault(codeInvalidArgument, "the steps cursor exceeds the accepted length")
	}
	decoded, err := cursorEncoding.DecodeString(presented)
	if err != nil {
		return snapshotCursor{}, malformed
	}
	fields := strings.Split(string(decoded), ":")
	if len(fields) != 4 {
		return snapshotCursor{}, malformed
	}
	if fields[0] != operationFingerprint(expectedOperationID) {
		return snapshotCursor{}, newFault(codeRevisionConflict, "the steps cursor belongs to another operation")
	}

	coveredSeq, err := strconv.ParseInt(fields[1], 10, 64)
	if err != nil || coveredSeq < 0 {
		return snapshotCursor{}, malformed
	}
	issuedAt, err := strconv.ParseInt(fields[2], 10, 64)
	if err != nil || issuedAt <= 0 {
		return snapshotCursor{}, malformed
	}
	stepOffset, err := strconv.Atoi(fields[3])
	if err != nil || stepOffset <= 0 {
		return snapshotCursor{}, malformed
	}
	return snapshotCursor{CoveredSeq: coveredSeq, IssuedAtUnix: issuedAt, StepOffset: stepOffset}, nil
}

// operationFingerprint binds a cursor to its operation in a fixed number of
// characters.
func operationFingerprint(operationID string) string {
	digest := sha256.Sum256([]byte(operationID))
	return hex.EncodeToString(digest[:4])
}

// cursorFingerprint renders a cursor that did not decode for a log record.
//
// The telemetry contract forbids logging a presented cursor in any field, so an
// undecodable one is recorded only as this irreversible value: two records for
// the same bad cursor still correlate, and the cursor itself cannot be
// recovered from them.
func cursorFingerprint(presented string) string {
	if presented == "" {
		return ""
	}
	digest := sha256.Sum256([]byte(presented))
	return "fp-" + hex.EncodeToString(digest[:8])
}
