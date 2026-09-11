package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/big"
	"net/http"
	"regexp"
	"time"
	"unicode/utf8"

	"connectrpc.com/connect"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"

	definitionvalidationv1 "github.com/ancyloce/anvilkit-agent-api/internal/contracts/definitionvalidationv1"
	"github.com/ancyloce/anvilkit-agent-api/internal/contracts/definitionvalidationv1/definitionvalidationv1connect"
)

// The public route and the action it requires, from the Agent OpenAPI's
// x-anvilkit-required-action.
const (
	routeDefinitionValidations = "/v1/definitions/validations"
	actionDefinitionValidate   = "definition.validate"
)

// validateDefinitionProcedure is the private method this route forwards to.
const validateDefinitionProcedure = definitionvalidationv1connect.DefinitionValidationValidateDefinitionProcedure

// Bounds from urn:anvilkit:definition-validation-interface:v1 and
// urn:anvilkit:definition:v1.
const (
	policyRefsMax         = 16
	identifierMaxLength   = 128
	issueCodeMaxLength    = 128
	issuePathMaxLength    = 1024
	issueMessageMaxLength = 512
)

var (
	// digestPattern is urn:anvilkit:values:v1#/$defs/digest.
	digestPattern = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)
	// identifierPattern is urn:anvilkit:values:v1#/$defs/id.
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

	validateDefinitionMembers = []string{
		"definition", "descriptorDigest", "runtimeProfileRef", "policyRefs",
	}
)

// validateDefinitionRequest is the Request half of the validation interface.
// The definition stays raw so the exact submitted bytes, including their number
// formatting, reach Control unchanged; this service is not its semantic
// authority and must not re-encode it.
type validateDefinitionRequest struct {
	Definition        json.RawMessage `json:"definition"`
	DescriptorDigest  string          `json:"descriptorDigest"`
	RuntimeProfileRef string          `json:"runtimeProfileRef"`
	PolicyRefs        []string        `json:"policyRefs"`
}

// validationReport is the Response half. definitionDigest is omitted rather
// than emptied when Control withholds it, which is how an invalid definition is
// distinguished from a valid one on the wire.
type validationReport struct {
	Valid             bool              `json:"valid"`
	DefinitionDigest  string            `json:"definitionDigest,omitempty"`
	DescriptorDigest  string            `json:"descriptorDigest"`
	RuntimeProfileRef string            `json:"runtimeProfileRef"`
	Issues            []validationIssue `json:"issues"`
}

type validationIssue struct {
	Code    string `json:"code"`
	Path    string `json:"path"`
	StepID  string `json:"stepId,omitempty"`
	Message string `json:"message"`
}

// handleValidateDefinition serves POST /v1/definitions/validations. It
// authenticates the caller, bounds and strictly decodes the body, forwards the
// existing private validation method and returns Control's report unchanged.
// It creates no operation and activates nothing.
func (s *Server) handleValidateDefinition(w http.ResponseWriter, r *http.Request) {
	actor, err := s.identities.Authorize(r.Header.Get("Authorization"), actionDefinitionValidate)
	if err != nil {
		s.writeError(w, r, authorizationFault(err))
		return
	}
	recordActor(r.Context(), actor)

	request, fault := decodeValidateDefinitionRequest(r)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	report, fault := s.forwardValidateDefinition(r.Context(), request)
	if fault != nil {
		s.writeError(w, r, fault)
		return
	}

	recordOutcome(r.Context(), http.StatusOK, "", "")
	writeJSON(w, http.StatusOK, report)
}

// authorizationFault keeps the two identity outcomes distinct: an unresolved
// credential is never reported as a permission problem, and a resolved actor
// missing the action is never reported as an authentication problem.
func authorizationFault(err error) *clientFault {
	switch {
	case errors.Is(err, identity.ErrPermissionDenied):
		return newFault(codePermissionDenied, "the authenticated actor does not hold "+actionDefinitionValidate)
	default:
		return newFault(codeUnauthenticated, "the request carries no accepted platform identity")
	}
}

func decodeValidateDefinitionRequest(r *http.Request) (*validateDefinitionRequest, *clientFault) {
	body, fault := readBoundedBody(r, config.RequestBodyMaxBytes)
	if fault != nil {
		return nil, fault
	}
	members, fault := scanStrictJSON(body)
	if fault != nil {
		return nil, fault
	}
	if fault := requireExactMembers(members, validateDefinitionMembers); fault != nil {
		return nil, fault
	}

	var request validateDefinitionRequest
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, memberTypeFault(err)
	}
	if fault := request.validate(); fault != nil {
		return nil, fault
	}
	return &request, nil
}

// memberTypeFault names the member whose JSON type is wrong without repeating
// the value it carried.
func memberTypeFault(err error) *clientFault {
	var typeError *json.UnmarshalTypeError
	if errors.As(err, &typeError) && typeError.Field != "" {
		return newFault(codeInvalidArgument,
			"the request member "+safeMemberName(typeError.Field)+" has the wrong JSON type")
	}
	return newFault(codeInvalidArgument, "the request body does not match the validation request contract")
}

// validate applies the request bounds the contract states. Semantic acceptance
// of the definition itself belongs to Control.
func (v *validateDefinitionRequest) validate() *clientFault {
	if !digestPattern.MatchString(v.DescriptorDigest) {
		return newFault(codeInvalidArgument, "descriptorDigest must be a sha256 digest string")
	}
	if !isIdentifier(v.RuntimeProfileRef) {
		return newFault(codeInvalidArgument, "runtimeProfileRef must be a values-v1 identifier")
	}
	if len(v.PolicyRefs) == 0 || len(v.PolicyRefs) > policyRefsMax {
		return newFault(codeInvalidArgument, "policyRefs must contain 1 to 16 identifiers")
	}
	seen := make(map[string]struct{}, len(v.PolicyRefs))
	for _, reference := range v.PolicyRefs {
		if !isIdentifier(reference) {
			return newFault(codeInvalidArgument, "policyRefs must contain values-v1 identifiers only")
		}
		if _, repeated := seen[reference]; repeated {
			return newFault(codeInvalidArgument, "policyRefs must not repeat an identifier")
		}
		seen[reference] = struct{}{}
	}
	return v.validateDefinitionEnvelope()
}

// validateDefinitionEnvelope checks only that the definition is an object of a
// schema version this service transports. Everything inside it is Control's
// decision, so no other member is inspected here.
func (v *validateDefinitionRequest) validateDefinitionEnvelope() *clientFault {
	var envelope struct {
		SchemaVersion any `json:"schemaVersion"`
	}
	decoder := json.NewDecoder(bytes.NewReader(v.Definition))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil {
		return newFault(codeInvalidArgument, "definition must be a JSON object")
	}
	if envelope.SchemaVersion == nil {
		return newFault(codeInvalidArgument, "definition omits the required member schemaVersion")
	}
	number, ok := envelope.SchemaVersion.(json.Number)
	if !ok {
		return newFault(codeInvalidArgument, "schemaVersion must be a JSON number")
	}
	// JSON Schema const compares numeric values. Preserve the original bytes
	// for Control, accepting 1.0 and 1e0 while rejecting strings and fractions.
	// The float precheck avoids constructing enormous powers for hostile input;
	// the rational check rejects fractions that would round to one.
	approximate, err := number.Float64()
	if err != nil || approximate != 1 {
		return newFault(codeUnsupportedSchema, "definition declares a schemaVersion this service does not transport")
	}
	exact, ok := new(big.Rat).SetString(number.String())
	if !ok || exact.Cmp(big.NewRat(1, 1)) != 0 {
		return newFault(codeUnsupportedSchema, "definition declares a schemaVersion this service does not transport")
	}
	return nil
}

func isIdentifier(value string) bool {
	return value != "" && len(value) <= identifierMaxLength && identifierPattern.MatchString(value)
}

// forwardValidateDefinition performs the existing private validation call and
// converts its report. One completion record is emitted per call attempt.
func (s *Server) forwardValidateDefinition(
	ctx context.Context,
	request *validateDefinitionRequest,
) (*validationReport, *clientFault) {
	message := &definitionvalidationv1.ValidateDefinitionRequest{
		DefinitionJson:    []byte(request.Definition),
		DescriptorDigest:  &request.DescriptorDigest,
		RuntimeProfileRef: &request.RuntimeProfileRef,
		PolicyRefs:        request.PolicyRefs,
	}

	callContext, cancel := context.WithTimeout(ctx, s.controlCallTimeout)
	defer cancel()

	started := time.Now()
	response, err := s.control.ValidateDefinition(callContext, connect.NewRequest(message))
	elapsed := time.Since(started)

	if err != nil {
		fault := faultFromControlError(err)
		s.logControlCall(ctx, validateDefinitionProcedure, elapsed, connect.CodeOf(err).String(), outcomeForCode(connect.CodeOf(err)), fault)
		return nil, fault
	}

	report, contractErr := validationReportFrom(response.Msg)
	if contractErr != nil {
		fault := newFault(codeDependencyUnavailable, "the validation dependency returned a report outside its contract")
		s.logControlCall(ctx, validateDefinitionProcedure, elapsed, "ok", "error", fault)
		return nil, fault
	}

	s.logControlCall(ctx, validateDefinitionProcedure, elapsed, "ok", "ok", nil)
	return report, nil
}

// logControlCall emits the one completion record a private call attempt owes
// the logging contract. The procedure is passed in so every Control method this
// service calls is recorded under its own route rather than a shared label.
func (s *Server) logControlCall(
	ctx context.Context,
	procedure string,
	elapsed time.Duration,
	rpcCode, outcome string,
	fault *clientFault,
) {
	attributes := []slog.Attr{
		slog.String("eventName", logging.EventRPCClientCompleted),
		slog.String("requestId", requestIDFrom(ctx)),
		slog.String("protocol", "grpc"),
		slog.String("caller", logging.ServiceName),
		slog.String("callee", "anvilkit-agent-control"),
		slog.String("routeTemplate", procedure),
		slog.Int("transportAttempt", 1),
		slog.String("outcome", outcome),
		slog.Int64("durationMs", logging.DurationMs(elapsed)),
		slog.String("rpc.code", rpcCode),
	}
	if operationID := operationIDFrom(ctx); operationID != "" {
		attributes = append(attributes, slog.String("operationId", operationID))
	}
	severity := slog.LevelInfo
	if fault != nil {
		severity = slog.LevelWarn
		attributes = append(attributes,
			slog.String("error.code", fault.code),
			slog.String("error.type", errorFamilies[fault.code].errorType),
			slog.Bool("error.retryable", errorFamilies[fault.code].retryable),
		)
	}
	s.logger.LogAttrs(ctx, severity, "", attributes...)
}

// faultFromControlError maps a private RPC failure onto the public envelope.
//
// The private contract signals failures by family through the RPC status code
// and defines no error-detail message, so a family whose public code cannot be
// recovered from the status alone is reported as a dependency failure rather
// than guessed at. That applies to RESOURCE_EXHAUSTED, whose envelope requires
// an overloadReason the transport does not carry. Control's own authentication
// toward this service is likewise a deployment fault, never the caller's, so it
// is never reflected as a 401 or 403.
func faultFromControlError(err error) *clientFault {
	switch connect.CodeOf(err) {
	case connect.CodeInvalidArgument:
		return newFault(codeInvalidArgument, "the definition or its references were rejected by validation")
	case connect.CodeNotFound:
		return newFault(codeNotFound, "a referenced descriptor, policy or runtime profile is not available in scope")
	case connect.CodeFailedPrecondition:
		return newFault(codeProfileQualification, "the retained contract or runtime profile is incompatible with this definition")
	default:
		return newFault(codeDependencyUnavailable, "the validation dependency is unavailable")
	}
}

func outcomeForCode(code connect.Code) string {
	switch code {
	case connect.CodeInvalidArgument, connect.CodeNotFound:
		return "denied"
	case connect.CodeFailedPrecondition:
		return "conflict"
	case connect.CodeResourceExhausted:
		return "overloaded"
	case connect.CodeUnavailable:
		return "unavailable"
	case connect.CodeDeadlineExceeded:
		return "timeout"
	case connect.CodeCanceled:
		return "canceled"
	case connect.CodeUnauthenticated, connect.CodePermissionDenied:
		return "denied"
	default:
		return "error"
	}
}

// validationReportFrom converts Control's message into the public report and
// holds it to the response contract. A report that breaks the contract is
// refused rather than reshaped, so this service never emits a body that the
// public schema would reject and never invents a digest or drops an issue.
func validationReportFrom(message *definitionvalidationv1.ValidateDefinitionResponse) (*validationReport, error) {
	if message == nil || message.Valid == nil {
		return nil, errors.New("the report omits valid")
	}
	if message.DescriptorDigest == nil || !digestPattern.MatchString(*message.DescriptorDigest) {
		return nil, errors.New("the report omits or malforms descriptorDigest")
	}
	if message.RuntimeProfileRef == nil || !isIdentifier(*message.RuntimeProfileRef) {
		return nil, errors.New("the report omits or malforms runtimeProfileRef")
	}

	report := &validationReport{
		Valid:             *message.Valid,
		DescriptorDigest:  *message.DescriptorDigest,
		RuntimeProfileRef: *message.RuntimeProfileRef,
		Issues:            []validationIssue{},
	}

	if report.Valid {
		if message.DefinitionDigest == nil || !digestPattern.MatchString(*message.DefinitionDigest) {
			return nil, errors.New("a valid report omits or malforms definitionDigest")
		}
		if len(message.Issues) != 0 {
			return nil, errors.New("a valid report carries issues")
		}
		report.DefinitionDigest = *message.DefinitionDigest
		return report, nil
	}

	if message.DefinitionDigest != nil {
		return nil, errors.New("an invalid report carries definitionDigest")
	}
	if len(message.Issues) == 0 || len(message.Issues) > config.ValidationMaxIssues {
		return nil, errors.New("an invalid report carries no issues or more than the contract allows")
	}
	for _, issue := range message.Issues {
		converted, err := validationIssueFrom(issue)
		if err != nil {
			return nil, err
		}
		report.Issues = append(report.Issues, converted)
	}
	return report, nil
}

func validationIssueFrom(issue *definitionvalidationv1.ValidationIssue) (validationIssue, error) {
	if issue == nil || issue.Code == nil || issue.Path == nil || issue.Message == nil {
		return validationIssue{}, errors.New("an issue omits a required member")
	}
	if utf8.RuneCountInString(*issue.Code) == 0 || utf8.RuneCountInString(*issue.Code) > issueCodeMaxLength {
		return validationIssue{}, errors.New("an issue code is out of bounds")
	}
	if utf8.RuneCountInString(*issue.Path) > issuePathMaxLength {
		return validationIssue{}, errors.New("an issue path is out of bounds")
	}
	if utf8.RuneCountInString(*issue.Message) == 0 || utf8.RuneCountInString(*issue.Message) > issueMessageMaxLength {
		return validationIssue{}, errors.New("an issue message is out of bounds")
	}

	converted := validationIssue{
		Code:    *issue.Code,
		Path:    *issue.Path,
		Message: *issue.Message,
	}
	if issue.StepId != nil {
		if !isIdentifier(*issue.StepId) {
			return validationIssue{}, errors.New("an issue stepId is not an identifier")
		}
		converted.StepID = *issue.StepId
	}
	return converted, nil
}
