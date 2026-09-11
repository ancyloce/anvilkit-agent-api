package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
)

// Registered error codes from contracts/values/agent-enums-v1.schema.json. The
// envelope's code is an open string routed by family, so only the codes this
// service actually produces are named here.
const (
	codeInvalidArgument     = "INVALID_ARGUMENT"
	codeUnsupportedSchema   = "UNSUPPORTED_SCHEMA"
	codeUnauthenticated     = "UNAUTHENTICATED"
	codePermissionDenied    = "PERMISSION_DENIED"
	codeNotFound            = "NOT_FOUND"
	codeRevisionConflict    = "REVISION_CONFLICT"
	codeIdempotencyConflict = "IDEMPOTENCY_CONFLICT"
	// codeAborted is the ABORTED family itself, reported where the private
	// transport establishes the family but not which of its registered codes
	// applies. The envelope's code is an open string routed by family, and a
	// client treats an unrecognized code inside a known family as that family
	// default, so this states exactly what Control told this service.
	codeAborted               = "ABORTED"
	codeOverloaded            = "OVERLOADED"
	codeChangeBlocked         = "CHANGE_BLOCKED"
	codeRestartRequired       = "RESTART_REQUIRED"
	codeProfileQualification  = "PROFILE_QUALIFICATION_FAILED"
	codeDependencyUnavailable = "DEPENDENCY_UNAVAILABLE"
)

// dependencyRetryAfterMs is the bounded client hint sent with every 503. The
// architecture asks for a bounded hint where one is calculable; this service has
// no queue depth to calculate from, so it advertises a short fixed pause.
const dependencyRetryAfterMs = 1000

// overloadRetryAfterMs is the same bounded hint for a capacity denial. The API
// holds no queue, so the pause is fixed rather than derived from a depth this
// service cannot see.
const overloadRetryAfterMs = 5000

// overloadReasonQueueFull is urn:anvilkit:agent-enums:v1#/$defs/overloadReason.
// It is the only reason the controlled local profile can report, because that
// profile has exactly one capacity bound.
const overloadReasonQueueFull = "queue_full"

// errorEnvelope is urn:anvilkit:error-envelope:v1. Optional members are omitted
// rather than sent as null, and retryable is derived from the code so a caller
// can never be told that a terminal failure is worth retrying.
type errorEnvelope struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	OperationID  string `json:"operationId,omitempty"`
	Retryable    bool   `json:"retryable"`
	RetryAfterMs *int32 `json:"retryAfterMs,omitempty"`
	DetailsRef   string `json:"detailsRef,omitempty"`
	RequestID    string `json:"requestId,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

// family binds a registered code to the HTTP status and retry semantics the
// contract's familyMapping records for it.
type family struct {
	httpStatus int
	retryable  bool
	// errorType is the log-record error.type value for this family.
	errorType string
}

var errorFamilies = map[string]family{
	codeInvalidArgument:       {http.StatusBadRequest, false, "validation"},
	codeUnsupportedSchema:     {http.StatusBadRequest, false, "validation"},
	codeUnauthenticated:       {http.StatusUnauthorized, false, "auth"},
	codePermissionDenied:      {http.StatusForbidden, false, "auth"},
	codeNotFound:              {http.StatusNotFound, false, "validation"},
	codeRevisionConflict:      {http.StatusConflict, false, "conflict"},
	codeIdempotencyConflict:   {http.StatusConflict, false, "conflict"},
	codeAborted:               {http.StatusConflict, false, "conflict"},
	codeOverloaded:            {http.StatusTooManyRequests, true, "overloaded"},
	codeChangeBlocked:         {http.StatusConflict, false, "precondition"},
	codeRestartRequired:       {http.StatusConflict, false, "precondition"},
	codeProfileQualification:  {http.StatusConflict, false, "precondition"},
	codeDependencyUnavailable: {http.StatusServiceUnavailable, true, "unavailable"},
}

// clientFault is a rejection that maps directly onto the error envelope. Its
// message names the rule that was broken; it never repeats a submitted value,
// so no credential, definition source or provider body can reach a response.
type clientFault struct {
	code    string
	message string
	// reason names the exhausted limit on a capacity denial. The envelope
	// requires it with OVERLOADED and forbids it with every other code.
	reason string
}

func newFault(code, message string) *clientFault {
	return &clientFault{code: code, message: message}
}

// overloadFault is the capacity denial. It is built through its own constructor
// because the envelope's OVERLOADED shape is the one that carries a reason, and
// a reason must never be attached to any other code.
func overloadFault(message, reason string) *clientFault {
	return &clientFault{code: codeOverloaded, message: message, reason: reason}
}

func (f *clientFault) Error() string { return f.code + ": " + f.message }

// asFault extracts a clientFault from an error chain.
func asFault(err error) (*clientFault, bool) {
	var fault *clientFault
	if errors.As(err, &fault) {
		return fault, true
	}
	return nil, false
}

// writeError renders a fault as the shared envelope. The status, retryability
// and Retry-After header all follow from the code, so a caller and the log
// record can never disagree about what happened.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, fault *clientFault) {
	registered, known := errorFamilies[fault.code]
	errorType := registered.errorType
	if !known {
		// An unregistered code is a defect in this service, not a caller
		// problem. It becomes a dependency failure rather than a newly invented
		// family, and the request's single completion record classifies it as
		// internal so the defect stays visible without a second record.
		fault = newFault(codeDependencyUnavailable, "the request could not be completed")
		registered = errorFamilies[codeDependencyUnavailable]
		errorType = "internal"
	}

	envelope := errorEnvelope{
		Code:      fault.code,
		Message:   fault.message,
		Retryable: registered.retryable,
		RequestID: requestIDFrom(r.Context()),
		// The record the request is about names its operation, so a failure
		// reply and its completion record identify the same operation.
		OperationID: operationIDFrom(r.Context()),
	}
	switch fault.code {
	case codeDependencyUnavailable:
		hint := int32(dependencyRetryAfterMs)
		envelope.RetryAfterMs = &hint
		w.Header().Set("Retry-After", strconv.Itoa(dependencyRetryAfterMs/1000))
	case codeOverloaded:
		hint := int32(overloadRetryAfterMs)
		envelope.RetryAfterMs = &hint
		envelope.Reason = fault.reason
		w.Header().Set("Retry-After", strconv.Itoa(overloadRetryAfterMs/1000))
	}

	recordOutcome(r.Context(), registered.httpStatus, fault.code, errorType)
	writeJSON(w, registered.httpStatus, envelope)
}

// writeJSON renders one response body. A body this service builds is always
// encodable, so an encoding failure is a defect rather than a caller problem and
// is reported without a contract body.
func writeJSON(w http.ResponseWriter, status int, body any) {
	encoded, err := json.Marshal(body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(append(encoded, '\n'))
}
