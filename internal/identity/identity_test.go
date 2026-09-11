package identity

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	validToken   = "controlled-developer-token-0001"
	requiredTest = "definition.validate"
)

func writeProfile(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "identities.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("writing the profile: %v", err)
	}
	return path
}

func loadValidProfile(t *testing.T) *Profile {
	t.Helper()
	profile, err := LoadProfile(writeProfile(t, `{
      "identities": [
        {
          "token": "`+validToken+`",
          "actorId": "developer-fixture-1",
          "tenantId": "tenant-fixture-1",
          "grantedActions": ["definition.validate"]
        },
        {
          "token": "controlled-reader-token-000001",
          "actorId": "developer-fixture-2",
          "tenantId": "tenant-fixture-2",
          "grantedActions": ["operation.read"]
        }
      ]
    }`))
	if err != nil {
		t.Fatalf("loading a valid profile: %v", err)
	}
	return profile
}

func TestAuthorizeResolvesMappedActorHoldingTheAction(t *testing.T) {
	actor, err := loadValidProfile(t).Authorize("Bearer "+validToken, requiredTest)
	if err != nil {
		t.Fatalf("expected authorization, got %v", err)
	}
	if actor.ActorID != "developer-fixture-1" || actor.TenantID != "tenant-fixture-1" {
		t.Fatalf("resolved the wrong actor: %+v", actor)
	}
}

func TestAuthorizeSeparatesAuthenticationFromPermission(t *testing.T) {
	profile := loadValidProfile(t)

	cases := []struct {
		name   string
		header string
		action string
		want   error
	}{
		{"no header", "", requiredTest, ErrUnauthenticated},
		{"wrong scheme", "Basic " + validToken, requiredTest, ErrUnauthenticated},
		{"empty credential", "Bearer ", requiredTest, ErrUnauthenticated},
		{"unmapped credential", "Bearer unmapped-token-000000000001", requiredTest, ErrUnauthenticated},
		{"credential case differs", "Bearer " + strings.ToUpper(validToken), requiredTest, ErrUnauthenticated},
		{"mapped actor without the action", "Bearer controlled-reader-token-000001", requiredTest, ErrPermissionDenied},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if _, err := profile.Authorize(testCase.header, testCase.action); !errors.Is(err, testCase.want) {
				t.Fatalf("expected %v, got %v", testCase.want, err)
			}
		})
	}
}

func TestSchemeNameIsCaseInsensitive(t *testing.T) {
	if _, err := loadValidProfile(t).Authorize("bearer "+validToken, requiredTest); err != nil {
		t.Fatalf("expected a case-insensitive scheme name to authorize, got %v", err)
	}
}

func TestUnconfiguredProfileAuthorizesNothing(t *testing.T) {
	if _, err := EmptyProfile().Authorize("Bearer "+validToken, requiredTest); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("an empty profile must deny, got %v", err)
	}
	var absent *Profile
	if _, err := absent.Authorize("Bearer "+validToken, requiredTest); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("a nil profile must deny, got %v", err)
	}
}

func TestLoadProfileRejectsUnusableDocuments(t *testing.T) {
	cases := map[string]string{
		"unknown member":      `{"identities": [], "roleMapping": {}}`,
		"no identities":       `{"identities": []}`,
		"short token":         `{"identities":[{"token":"short","actorId":"a","tenantId":"t","grantedActions":["definition.validate"]}]}`,
		"no actions":          `{"identities":[{"token":"controlled-developer-token-0001","actorId":"a","tenantId":"t","grantedActions":[]}]}`,
		"bad action shape":    `{"identities":[{"token":"controlled-developer-token-0001","actorId":"a","tenantId":"t","grantedActions":["Definition.Validate"]}]}`,
		"repeated action":     `{"identities":[{"token":"controlled-developer-token-0001","actorId":"a","tenantId":"t","grantedActions":["definition.validate","definition.validate"]}]}`,
		"bad actor id":        `{"identities":[{"token":"controlled-developer-token-0001","actorId":".bad","tenantId":"t","grantedActions":["definition.validate"]}]}`,
		"trailing document":   `{"identities":[{"token":"controlled-developer-token-0001","actorId":"a","tenantId":"t","grantedActions":["definition.validate"]}]}{}`,
		"repeated credential": `{"identities":[{"token":"controlled-developer-token-0001","actorId":"a","tenantId":"t","grantedActions":["definition.validate"]},{"token":"controlled-developer-token-0001","actorId":"b","tenantId":"t","grantedActions":["operation.read"]}]}`,
	}
	for name, document := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadProfile(writeProfile(t, document)); err == nil {
				t.Fatal("expected the profile to be rejected as a whole")
			}
		})
	}
}

func TestLoadProfileReportsAMissingFile(t *testing.T) {
	if _, err := LoadProfile(filepath.Join(t.TempDir(), "absent.json")); err == nil {
		t.Fatal("expected a missing profile to be reported")
	}
}

func TestControlCredentialComesOnlyFromTheResolvedIdentity(t *testing.T) {
	profile := loadValidProfile(t)
	reader := "controlled-reader-token-000001"
	actor, err := profile.Authorize("bearer "+reader, "operation.read")
	if err != nil || actor.ControlCredential() != reader {
		t.Fatal("the private call did not retain its exact mapped fixture credential")
	}
	actor, err = profile.Authorize("Bearer "+validToken, requiredTest)
	if err != nil || actor.ControlCredential() != "" {
		t.Fatal("an identity without a Control action retained a private credential")
	}
	actor, err = profile.Authorize("Bearer untrusted-request-input", "operation.read")
	if err == nil || actor.ControlCredential() != "" {
		t.Fatal("unmapped input became a private credential")
	}
}
