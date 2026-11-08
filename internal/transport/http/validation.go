package http

import (
	"errors"
	"regexp"
	"strings"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/gorillamux"
	"github.com/gin-gonic/gin"
)

// RB-S13: kin-openapi's error messages carry the rejected input and the schema:
// a SchemaError's "Schema:" and "Value:" dumps, a parameter error's "value
// <input>", a format checker's parse error. So no validation refusal is built
// from err.Error(). Each one is a fixed message, with its location in
// details.field. The dumps are also disabled process-wide, so that nothing that
// does format a SchemaError (a log line, a future handler) prints them.
func init() {
	openapi3.SchemaErrorDetailsDisabled = true
	// The reason names no value (RB-S13).
	openapi3.DefineStringFormatValidator(sequenceFormat, openapi3.NewCallbackValidator(func(s string) error {
		if !canonicalSequence(s) {
			return errors.New("not a canonical sequence within the uint64 bound")
		}
		return nil
	}))
}

// sequenceFormat is this process's format for the contract's canonical
// sequences (RB-A23). Their pattern admits 20-digit values above the uint64
// bound contracts.md §4 sets; boundSequences gives every schema with that
// pattern the format, so each revision and sequence value the validator sees,
// in a body, a parameter or a header, is checked against the bound. The
// contract itself is unchanged.
const sequenceFormat = "anvilkit-uint64-sequence"

// boundSequences sets sequenceFormat on every schema of spec whose pattern is
// the canonical-sequence pattern. The embedded spec inlines some of them
// (Revision is its own schema, not a $ref), so every reachable schema is
// visited. A spec with none, or one that already formats such a schema
// otherwise, is refused rather than left unbounded.
func boundSequences(spec *openapi3.T) error {
	seen := map[*openapi3.Schema]bool{}
	bounded, conflict := 0, false
	var visit func(ref *openapi3.SchemaRef)
	visit = func(ref *openapi3.SchemaRef) {
		if ref == nil || ref.Value == nil || seen[ref.Value] {
			return
		}
		s := ref.Value
		seen[s] = true
		if s.Pattern == sequencePattern.String() {
			if s.Format != "" && s.Format != sequenceFormat {
				conflict = true
			}
			s.Format = sequenceFormat
			bounded++
		}
		for _, p := range s.Properties {
			visit(p)
		}
		for _, list := range []openapi3.SchemaRefs{s.AllOf, s.AnyOf, s.OneOf} {
			for _, r := range list {
				visit(r)
			}
		}
		visit(s.Items)
		visit(s.Not)
		visit(s.AdditionalProperties.Schema)
	}
	params := func(ps openapi3.Parameters) {
		for _, p := range ps {
			if p != nil && p.Value != nil {
				visit(p.Value.Schema)
			}
		}
	}
	body := func(rb *openapi3.RequestBodyRef) {
		if rb != nil && rb.Value != nil {
			for _, mt := range rb.Value.Content {
				visit(mt.Schema)
			}
		}
	}
	if c := spec.Components; c != nil {
		for _, r := range c.Schemas {
			visit(r)
		}
		for _, p := range c.Parameters {
			params(openapi3.Parameters{p})
		}
		for _, h := range c.Headers {
			if h != nil && h.Value != nil {
				visit(h.Value.Schema)
			}
		}
		for _, rb := range c.RequestBodies {
			body(rb)
		}
	}
	if spec.Paths != nil {
		for _, item := range spec.Paths.Map() {
			params(item.Parameters)
			for _, op := range item.Operations() {
				params(op.Parameters)
				body(op.RequestBody)
			}
		}
	}
	if bounded == 0 || conflict {
		return errors.New("the contract's canonical sequences are not the pattern this API bounds")
	}
	return nil
}

// requestValidator validates each request against the embedded spec with
// openapi3filter, which is what oapi-codegen's gin middleware runs. That
// middleware hands its error handler only a flattened message; here the typed
// error reaches requestInvalid.
func requestValidator(spec *openapi3.T) (gin.HandlerFunc, error) {
	if err := boundSequences(spec); err != nil {
		return nil, err
	}
	router, err := gorillamux.NewRouter(spec)
	if err != nil {
		return nil, err
	}
	options := &openapi3filter.Options{AuthenticationFunc: openapi3filter.NoopAuthenticationFunc}
	return func(c *gin.Context) {
		route, params, err := router.FindRoute(c.Request)
		if err != nil {
			fail(c, invalidArgument("no matching operation was found"))
			return
		}
		input := &openapi3filter.RequestValidationInput{Request: c.Request, PathParams: params, Route: route, Options: options}
		if err := openapi3filter.ValidateRequest(c.Request.Context(), input); err != nil {
			fail(c, requestInvalid(err))
			return
		}
		c.Next()
	}, nil
}

// requestInvalid is the refusal of a request the contract rejects. The message
// says what failed: a parameter, or the body with a JSON Schema keyword from
// kin-openapi's fixed set. details.field says where: the parameter's name, or
// the JSON Pointer (RFC 6901) of the body location. A pointer holds only names
// the request schemas declare and array indexes: no request schema is a
// map-typed object, and an undeclared member is reported at its parent object.
// So the pointer never carries client input.
func requestInvalid(err error) *apiError {
	var re *openapi3filter.RequestError
	if !errors.As(err, &re) {
		return invalidArgument("request does not match the API contract")
	}
	if re.Parameter != nil {
		return invalidField("invalid "+re.Parameter.In+" parameter", re.Parameter.Name)
	}
	if re.RequestBody == nil {
		return invalidArgument("request does not match the API contract")
	}
	var se *openapi3.SchemaError
	if !errors.As(re.Err, &se) {
		return invalidArgument("request body does not match the API contract")
	}
	return invalidField("request body: "+schemaFailure(se), jsonPointer(se.JSONPointer()))
}

var schemaKeyword = regexp.MustCompile(`^[A-Za-z]{1,32}$`)

// schemaFailure names the failed keyword in words; SchemaField is set from
// kin-openapi's own keyword literals, and anything else is not named.
func schemaFailure(se *openapi3.SchemaError) string {
	switch se.SchemaField {
	case "required":
		return "a required member is missing"
	case "properties", "additionalProperties":
		return "a member the contract does not define"
	case "type", "nullable":
		return "a value has the wrong type"
	case "enum":
		return "a value is not one of the allowed values"
	}
	if schemaKeyword.MatchString(se.SchemaField) {
		return "a value violates " + se.SchemaField
	}
	return "a value does not match the API contract"
}

var pointerEscape = strings.NewReplacer("~", "~0", "/", "~1")

// jsonPointer is the RFC 6901 pointer of a body location; the document itself
// (an empty path) has none, so the field is omitted.
func jsonPointer(path []string) string {
	var b strings.Builder
	for _, p := range path {
		b.WriteByte('/')
		b.WriteString(pointerEscape.Replace(p))
	}
	return b.String()
}

func invalidField(message, field string) *apiError {
	e := invalidArgument(message)
	e.field = field
	return e
}
