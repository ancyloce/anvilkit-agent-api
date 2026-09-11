package httpapi

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"unicode/utf8"
)

// maxJSONDepth bounds nesting so a small body cannot force deep recursion or an
// unbounded scan stack.
const maxJSONDepth = 64

// readBoundedBody returns the exact request bytes, refusing anything above limit
// bytes or not valid UTF-8. The ceiling is enforced before any parsing, so an
// oversized body is never decoded.
func readBoundedBody(r *http.Request, limit int64) ([]byte, *clientFault) {
	if !hasJSONContentType(r) {
		return nil, newFault(codeInvalidArgument, "the request must use Content-Type: application/json")
	}

	// Reading one byte past the ceiling distinguishes a body at the limit from
	// one above it without buffering the excess.
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return nil, newFault(codeInvalidArgument, "the request body could not be read")
	}
	if int64(len(body)) > limit {
		return nil, newFault(codeInvalidArgument, "the request body exceeds the route's byte ceiling")
	}
	if len(body) == 0 {
		return nil, newFault(codeInvalidArgument, "the request body is empty")
	}
	if !utf8.Valid(body) {
		return nil, newFault(codeInvalidArgument, "the request body is not valid UTF-8")
	}
	return body, nil
}

// hasJSONContentType accepts application/json with or without parameters.
func hasJSONContentType(r *http.Request) bool {
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		return false
	}
	mediaType, _, _ := bytes.Cut([]byte(contentType), []byte(";"))
	return bytes.EqualFold(bytes.TrimSpace(mediaType), []byte("application/json"))
}

// scanStrictJSON walks one JSON document and applies the public-value rules that
// a Go struct decode cannot express: duplicate member names are rejected at
// every depth, including inside the opaque definition, and an explicit null is
// rejected anywhere because no public field authorizes one. It returns the
// top-level member names in the order they appeared.
func scanStrictJSON(data []byte) ([]string, *clientFault) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()

	type frame struct {
		object    bool
		members   map[string]struct{}
		expectKey bool
	}

	var (
		stack     []*frame
		topLevel  []string
		completed bool
	)

	malformed := newFault(codeInvalidArgument, "the request body is not well-formed JSON")

	valueComplete := func() {
		if len(stack) == 0 {
			completed = true
			return
		}
		if parent := stack[len(stack)-1]; parent.object {
			parent.expectKey = true
		}
	}

	for {
		token, err := decoder.Token()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, malformed
		}
		if completed {
			return nil, newFault(codeInvalidArgument, "the request body must contain exactly one JSON document")
		}

		// A member name is the only token allowed where an object expects one.
		if len(stack) > 0 {
			if current := stack[len(stack)-1]; current.object && current.expectKey {
				if delimiter, isDelimiter := token.(json.Delim); isDelimiter {
					if delimiter != '}' {
						return nil, malformed
					}
					stack = stack[:len(stack)-1]
					valueComplete()
					continue
				}
				name, isString := token.(string)
				if !isString {
					return nil, malformed
				}
				if _, duplicate := current.members[name]; duplicate {
					return nil, newFault(codeInvalidArgument,
						"the request body repeats a JSON member name, which is rejected before decoding")
				}
				current.members[name] = struct{}{}
				if len(stack) == 1 {
					topLevel = append(topLevel, name)
				}
				current.expectKey = false
				continue
			}
		}

		switch value := token.(type) {
		case json.Delim:
			switch value {
			case '{', '[':
				if len(stack) >= maxJSONDepth {
					return nil, newFault(codeInvalidArgument, "the request body nests beyond the accepted depth")
				}
				if len(stack) == 0 && value != '{' {
					return nil, newFault(codeInvalidArgument, "the request body must be a JSON object")
				}
				next := &frame{object: value == '{', expectKey: value == '{'}
				if next.object {
					next.members = map[string]struct{}{}
				}
				stack = append(stack, next)
			case '}', ']':
				if len(stack) == 0 {
					return nil, malformed
				}
				stack = stack[:len(stack)-1]
				valueComplete()
			}
		case nil:
			return nil, newFault(codeInvalidArgument,
				"the request body contains an explicit null; optional members are omitted instead")
		default:
			if len(stack) == 0 {
				return nil, newFault(codeInvalidArgument, "the request body must be a JSON object")
			}
			valueComplete()
		}
	}

	if !completed || len(stack) != 0 {
		return nil, malformed
	}
	return topLevel, nil
}

// requireExactMembers holds a body to a contract that declares no optional
// member: exactly the required names, no more and no fewer.
func requireExactMembers(present []string, required []string) *clientFault {
	return requireMembers(present, required, nil)
}

// requireMembers compares the top-level member names against the members a
// request contract declares, reporting the first unknown or missing one. Every
// required member must be present, and no member outside required plus optional
// is accepted.
func requireMembers(present, required, optional []string) *clientFault {
	declared := make(map[string]struct{}, len(required)+len(optional))
	for _, name := range required {
		declared[name] = struct{}{}
	}
	for _, name := range optional {
		declared[name] = struct{}{}
	}
	seen := make(map[string]struct{}, len(present))
	for _, name := range present {
		if _, known := declared[name]; !known {
			return newFault(codeInvalidArgument, "the request body carries a member the contract does not declare: "+safeMemberName(name))
		}
		seen[name] = struct{}{}
	}
	for _, name := range required {
		if _, found := seen[name]; !found {
			return newFault(codeInvalidArgument, "the request body omits the required member "+name)
		}
	}
	return nil
}

// safeMemberName bounds an echoed member name. Only the name is ever echoed, and
// only after truncation, so an oversized or unusual key cannot enlarge or
// distort the response message.
func safeMemberName(name string) string {
	const limit = 64
	if len(name) > limit {
		name = name[:limit]
	}
	cleaned := make([]rune, 0, len(name))
	for _, character := range name {
		if character < 0x20 || character == 0x7f {
			continue
		}
		cleaned = append(cleaned, character)
	}
	if len(cleaned) == 0 {
		return "(unprintable)"
	}
	return string(cleaned)
}
