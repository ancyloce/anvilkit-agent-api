package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
)

func TestScanStrictJSONAcceptsAWellFormedObject(t *testing.T) {
	members, fault := scanStrictJSON([]byte(`{"a":1,"b":{"c":[1,2,{"d":true}]},"e":"x"}`))
	if fault != nil {
		t.Fatalf("expected acceptance, got %v", fault)
	}
	if got := strings.Join(members, ","); got != "a,b,e" {
		t.Fatalf("expected the top-level members in order, got %q", got)
	}
}

func TestScanStrictJSONRejectsDuplicateMemberNamesAtEveryDepth(t *testing.T) {
	cases := map[string]string{
		"top level":        `{"a":1,"a":2}`,
		"nested object":    `{"a":{"b":1,"b":2}}`,
		"inside an array":  `{"a":[{"b":1,"b":2}]}`,
		"deeply nested":    `{"a":{"b":{"c":{"d":1,"d":2}}}}`,
		"differing values": `{"definition":{"schemaVersion":1},"definition":{"schemaVersion":2}}`,
	}
	for name, document := range cases {
		t.Run(name, func(t *testing.T) {
			_, fault := scanStrictJSON([]byte(document))
			if fault == nil {
				t.Fatal("expected a duplicate member name to be rejected")
			}
			if !strings.Contains(fault.message, "repeats a JSON member name") {
				t.Fatalf("expected the duplicate-name rule to be named, got %q", fault.message)
			}
		})
	}
}

func TestScanStrictJSONRejectsExplicitNulls(t *testing.T) {
	cases := map[string]string{
		"member value":     `{"a":null}`,
		"nested member":    `{"a":{"b":null}}`,
		"array element":    `{"a":[1,null]}`,
		"only member":      `{"definition":null}`,
		"deep array entry": `{"a":{"b":[[null]]}}`,
	}
	for name, document := range cases {
		t.Run(name, func(t *testing.T) {
			_, fault := scanStrictJSON([]byte(document))
			if fault == nil {
				t.Fatal("expected an explicit null to be rejected")
			}
			if !strings.Contains(fault.message, "explicit null") {
				t.Fatalf("expected the null rule to be named, got %q", fault.message)
			}
		})
	}
}

func TestScanStrictJSONRejectsUnusableDocuments(t *testing.T) {
	cases := map[string]string{
		"array at the root":     `[{"a":1}]`,
		"scalar at the root":    `42`,
		"string at the root":    `"definition"`,
		"trailing document":     `{"a":1}{"b":2}`,
		"trailing scalar":       `{"a":1} 7`,
		"unterminated object":   `{"a":1`,
		"not json":              `definition`,
		"unbalanced close":      `{"a":1}}`,
		"beyond accepted depth": "{" + strings.Repeat(`"a":{`, maxJSONDepth+2),
	}
	for name, document := range cases {
		t.Run(name, func(t *testing.T) {
			if _, fault := scanStrictJSON([]byte(document)); fault == nil {
				t.Fatalf("expected %s to be rejected", name)
			}
		})
	}
}

func TestScanStrictJSONAcceptsRepeatedNamesInSiblingObjects(t *testing.T) {
	// The same name in two different objects is not a duplicate.
	if _, fault := scanStrictJSON([]byte(`{"a":{"id":1},"b":{"id":2}}`)); fault != nil {
		t.Fatalf("sibling objects may repeat a name, got %v", fault)
	}
}

func TestReadBoundedBodyEnforcesTheCeilingAndEncoding(t *testing.T) {
	atLimit := `{"pad":"` + strings.Repeat("x", config.RequestBodyMaxBytes-10) + `"}`
	if len(atLimit) != config.RequestBodyMaxBytes {
		t.Fatalf("the fixture body is %d bytes, expected exactly the ceiling", len(atLimit))
	}

	if _, fault := readBoundedBody(jsonRequest(atLimit), config.RequestBodyMaxBytes); fault != nil {
		t.Fatalf("a body exactly at the ceiling is accepted, got %v", fault)
	}

	overLimit := `{"pad":"` + strings.Repeat("x", config.RequestBodyMaxBytes-9) + `"}`
	fault := rejectBody(t, jsonRequest(overLimit))
	if !strings.Contains(fault.message, "byte ceiling") {
		t.Fatalf("expected the ceiling to be named, got %q", fault.message)
	}

	// The ceiling counts UTF-8 bytes, not characters: a body under the limit in
	// characters but over it in bytes is still refused.
	multibyte := `{"pad":"` + strings.Repeat("é", config.RequestBodyMaxBytes/2) + `"}`
	if len([]rune(multibyte)) > config.RequestBodyMaxBytes {
		t.Fatal("the multibyte fixture must be under the ceiling in characters")
	}
	if _, fault := readBoundedBody(jsonRequest(multibyte), config.RequestBodyMaxBytes); fault == nil {
		t.Fatal("expected a body over the ceiling in UTF-8 bytes to be refused")
	}
}

func TestReadBoundedBodyRejectsUnusableRequests(t *testing.T) {
	t.Run("empty body", func(t *testing.T) {
		rejectBody(t, jsonRequest(""))
	})
	t.Run("invalid utf-8", func(t *testing.T) {
		rejectBody(t, jsonRequest("{\"a\":\"\xff\xfe\"}"))
	})
	t.Run("missing content type", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodPost, routeDefinitionValidations, strings.NewReader(`{}`))
		rejectBody(t, request)
	})
	t.Run("wrong content type", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodPost, routeDefinitionValidations, strings.NewReader(`{}`))
		request.Header.Set("Content-Type", "text/plain")
		rejectBody(t, request)
	})
	t.Run("content type with parameters", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodPost, routeDefinitionValidations, strings.NewReader(`{"a":1}`))
		request.Header.Set("Content-Type", "application/json; charset=utf-8")
		if _, fault := readBoundedBody(request, config.RequestBodyMaxBytes); fault != nil {
			t.Fatalf("parameters on the media type are accepted, got %v", fault)
		}
	})
}

func TestRequireExactMembersNamesTheFirstProblem(t *testing.T) {
	required := []string{"definition", "descriptorDigest"}

	if fault := requireExactMembers([]string{"definition", "descriptorDigest"}, required); fault != nil {
		t.Fatalf("expected the exact member set to be accepted, got %v", fault)
	}
	if fault := requireExactMembers([]string{"definition"}, required); fault == nil ||
		!strings.Contains(fault.message, "omits the required member descriptorDigest") {
		t.Fatalf("expected the missing member to be named, got %v", fault)
	}
	if fault := requireExactMembers([]string{"definition", "descriptorDigest", "commandId"}, required); fault == nil ||
		!strings.Contains(fault.message, "commandId") {
		t.Fatalf("expected the undeclared member to be named, got %v", fault)
	}
}

func TestSafeMemberNameBoundsWhatIsEchoed(t *testing.T) {
	if got := safeMemberName(strings.Repeat("k", 200)); len(got) != 64 {
		t.Fatalf("expected an echoed member name to be truncated, got %d characters", len(got))
	}
	if got := safeMemberName("a\x00b\nc"); got != "abc" {
		t.Fatalf("expected control characters to be dropped, got %q", got)
	}
	if got := safeMemberName("\x00\x01"); got != "(unprintable)" {
		t.Fatalf("expected an unprintable name to be replaced, got %q", got)
	}
}

// jsonRequest builds a POST carrying body as application/json.
func jsonRequest(body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, routeDefinitionValidations, strings.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	return request
}

// rejectBody requires readBoundedBody to refuse a request and returns why.
func rejectBody(t *testing.T, request *http.Request) *clientFault {
	t.Helper()
	_, fault := readBoundedBody(request, config.RequestBodyMaxBytes)
	if fault == nil {
		t.Fatal("expected the request to be rejected")
	}
	return fault
}
