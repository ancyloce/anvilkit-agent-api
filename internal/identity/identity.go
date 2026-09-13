// Package identity establishes the trusted caller of a request and decides
// whether that caller holds the action a route requires.
//
// The Agent OpenAPI declares a bearer SSO scheme but records the role-to-action
// mapping as an activation input: no issuer, verification key or production
// role mapping exists yet, and the architecture forbids inventing one. The
// first stage therefore resolves callers through a controlled profile file that
// binds each accepted credential to a fixed actor, tenant and action set, in
// the same spirit as the controlled authorization profile DD-02 approves for
// disclosure. Two rules hold regardless of how identity is later established:
// presenting a bearer token never confers permission by itself, and an
// unconfigured profile denies every protected route.
package identity

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// Authorize reports these two failures separately so the HTTP edge can map them
// to UNAUTHENTICATED and PERMISSION_DENIED without inspecting the cause.
var (
	// ErrUnauthenticated means no trusted actor is bound to the credential.
	ErrUnauthenticated = errors.New("identity: the presented credential resolves to no trusted actor")
	// ErrPermissionDenied means a trusted actor lacks the required action.
	ErrPermissionDenied = errors.New("identity: the trusted actor does not hold the required action")
)

const (
	// minimumTokenLength keeps a controlled profile from accepting a credential
	// short enough to guess. It is a safety floor on the profile file, not a
	// claim about credential strength.
	minimumTokenLength = 24
	maximumIdentities  = 64
)

var (
	// identifierPattern is urn:anvilkit:values:v1#/$defs/id.
	identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)
	// actionPattern accepts the x-anvilkit-required-action values the Agent
	// OpenAPI declares, without pinning a copy of that list here.
	actionPattern = regexp.MustCompile(`^[a-z][a-z0-9-]*\.[a-z][a-z0-9-]*$`)
)

// Actor is the trusted caller of one request.
type Actor struct {
	ActorID           string
	TenantID          string
	actions           map[string]struct{}
	controlCredential string
}

// ControlCredential is the credential selected by the fixed local profile,
// never an unvalidated forwarded header. Control independently checks this
// fixture mapping, the matching actor/tenant and the API's mTLS identity.
func (a Actor) ControlCredential() string { return a.controlCredential }

// Holds reports whether the actor was granted action.
func (a Actor) Holds(action string) bool {
	_, granted := a.actions[action]
	return granted
}

// Profile is a loaded controlled identity mapping. The zero value and the
// result of EmptyProfile both deny every request.
type Profile struct {
	byCredentialDigest map[[sha256.Size]byte]Actor
}

// EmptyProfile returns a profile that authorizes nothing. It is what an
// unconfigured deployment runs with, so a missing profile fails closed instead
// of falling back to an implicit developer identity.
func EmptyProfile() *Profile {
	return &Profile{byCredentialDigest: map[[sha256.Size]byte]Actor{}}
}

// Size reports how many identities the profile carries. Startup logs the count
// so an operator can see that a profile was actually loaded.
func (p *Profile) Size() int {
	if p == nil {
		return 0
	}
	return len(p.byCredentialDigest)
}

// profileDocument is the on-disk shape of the controlled profile.
type profileDocument struct {
	Comment    string            `json:"$comment"`
	Identities []profileIdentity `json:"identities"`
}

type profileIdentity struct {
	Token          string   `json:"token"`
	ActorID        string   `json:"actorId"`
	TenantID       string   `json:"tenantId"`
	GrantedActions []string `json:"grantedActions"`
}

// LoadProfile reads and validates a controlled identity profile. It rejects the
// whole file on any problem: a partially applied authorization mapping is worse
// than none, because it would silently grant a subset nobody reviewed.
func LoadProfile(path string) (*Profile, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("identity: reading the controlled profile: %w", err)
	}

	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	var document profileDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("identity: decoding the controlled profile: %w", err)
	}
	if decoder.More() {
		return nil, errors.New("identity: the controlled profile must contain exactly one JSON document")
	}
	if len(document.Identities) == 0 {
		return nil, errors.New("identity: the controlled profile declares no identities")
	}
	if len(document.Identities) > maximumIdentities {
		return nil, fmt.Errorf("identity: the controlled profile declares more than %d identities", maximumIdentities)
	}

	profile := EmptyProfile()
	for index, declared := range document.Identities {
		actor, digest, err := parseIdentity(declared)
		if err != nil {
			return nil, fmt.Errorf("identity: identities[%d]: %w", index, err)
		}
		if _, duplicate := profile.byCredentialDigest[digest]; duplicate {
			return nil, fmt.Errorf("identity: identities[%d]: the credential is already bound to another actor", index)
		}
		profile.byCredentialDigest[digest] = actor
	}
	return profile, nil
}

func parseIdentity(declared profileIdentity) (Actor, [sha256.Size]byte, error) {
	var digest [sha256.Size]byte

	if len(declared.Token) < minimumTokenLength {
		return Actor{}, digest, fmt.Errorf("token must be at least %d characters", minimumTokenLength)
	}
	if !isIdentifier(declared.ActorID) {
		return Actor{}, digest, errors.New("actorId must be a values-v1 identifier")
	}
	if !isIdentifier(declared.TenantID) {
		return Actor{}, digest, errors.New("tenantId must be a values-v1 identifier")
	}
	if len(declared.GrantedActions) == 0 {
		return Actor{}, digest, errors.New("grantedActions must list at least one action")
	}

	actions := make(map[string]struct{}, len(declared.GrantedActions))
	for _, action := range declared.GrantedActions {
		if !actionPattern.MatchString(action) || len(action) > 128 {
			return Actor{}, digest, fmt.Errorf("grantedActions contains an unrecognized action shape")
		}
		if _, repeated := actions[action]; repeated {
			return Actor{}, digest, fmt.Errorf("grantedActions repeats %q", action)
		}
		actions[action] = struct{}{}
	}

	actor := Actor{
		ActorID:  declared.ActorID,
		TenantID: declared.TenantID,
		actions:  actions,
	}
	if actor.Holds("operation.read") || actor.Holds("local-check.create") || actor.Holds("operation.cancel") || actor.Holds("component.prepare") {
		actor.controlCredential = declared.Token
	}
	return actor, sha256.Sum256([]byte(declared.Token)), nil
}

func isIdentifier(value string) bool {
	return value != "" && len(value) <= 128 && identifierPattern.MatchString(value)
}

// Authorize resolves the Authorization header to a trusted actor and confirms
// that the actor holds requiredAction. The credential is matched by digest and
// compared in constant time, so neither the profile nor the comparison exposes
// the accepted tokens.
func (p *Profile) Authorize(authorizationHeader, requiredAction string) (Actor, error) {
	token, ok := bearerToken(authorizationHeader)
	if !ok {
		return Actor{}, ErrUnauthenticated
	}
	if p == nil || len(p.byCredentialDigest) == 0 {
		return Actor{}, ErrUnauthenticated
	}

	presented := sha256.Sum256([]byte(token))
	var (
		resolved Actor
		found    bool
	)
	for digest, actor := range p.byCredentialDigest {
		if subtle.ConstantTimeCompare(digest[:], presented[:]) == 1 {
			resolved, found = actor, true
		}
	}
	if !found {
		return Actor{}, ErrUnauthenticated
	}
	if !resolved.Holds(requiredAction) {
		return Actor{}, ErrPermissionDenied
	}
	return resolved, nil
}

// bearerToken extracts the credential from an Authorization header. The scheme
// name is case-insensitive per RFC 7235; the credential itself is not.
func bearerToken(header string) (string, bool) {
	scheme, credential, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	credential = strings.TrimSpace(credential)
	if credential == "" {
		return "", false
	}
	return credential, true
}
