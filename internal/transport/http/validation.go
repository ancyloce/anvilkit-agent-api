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
func init() { openapi3.SchemaErrorDetailsDisabled = true }

// requestValidator validates each request against the embedded spec with
// openapi3filter, which is what oapi-codegen's gin middleware runs. That
// middleware hands its error handler only a flattened message; here the typed
// error reaches requestInvalid.
func requestValidator(spec *openapi3.T) (gin.HandlerFunc, error) {
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
