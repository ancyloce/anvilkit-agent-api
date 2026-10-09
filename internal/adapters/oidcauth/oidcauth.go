// Package oidcauth is the API's identity verifier for an OpenID Connect
// issuer (A12: go-oidc + x/oauth2; ENV-07 supplies the production issuer,
// audience and claim schema). It verifies a bearer JWT against the issuer's
// discovered JWKS — issuer, audience, an explicit signing-algorithm
// allowlist, signature (an unknown kid refetches the key set once) and
// exp/nbf under a bounded clock skew — and maps configured claim paths to
// the Principal's tenant, project, actor and roles. A token that fails any
// check is ErrUnauthenticated; the token itself is never logged.
package oidcauth

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

// Claims names where each Principal field is read in the token: a claim
// name, or a dot-separated path into nested objects. A configured path is
// required in every token (an absent project would read as the
// tenant-wide scope of a platform workload); an empty Project or Roles
// path reads none.
type Claims struct {
	Tenant  string
	Project string
	Actor   string
	Roles   string
}

// Options configure one issuer.
type Options struct {
	Issuer     string
	Audience   string
	Algorithms []string
	ClockSkew  time.Duration
	Claims     Claims
	// HTTPClient fetches the discovery document and the key set.
	HTTPClient *http.Client
	// Now is the clock of the exp/nbf checks (tests); time.Now when nil.
	Now func() time.Time
}

// Verifier verifies bearer tokens of one issuer.
type Verifier struct {
	verifier *oidc.IDTokenVerifier
	claims   Claims
	skew     time.Duration
	now      func() time.Time
}

var role = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`)

// New discovers the issuer (its metadata must name exactly this issuer)
// and builds the verifier; ctx bounds the discovery only.
func New(ctx context.Context, o Options) (*Verifier, error) {
	if o.Issuer == "" || o.Audience == "" || len(o.Algorithms) == 0 || o.Claims.Tenant == "" || o.Claims.Actor == "" {
		return nil, errors.New("oidcauth: issuer, audience, algorithms and the tenant and actor claims are required")
	}
	client := o.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	provider, err := oidc.NewProvider(oidc.ClientContext(ctx, client), o.Issuer)
	if err != nil {
		return nil, fmt.Errorf("oidcauth: discovery of %s: %w", o.Issuer, err)
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	// The key set outlives the discovery context; it carries only the client.
	verifier := provider.VerifierContext(oidc.ClientContext(context.Background(), client), &oidc.Config{
		ClientID:             o.Audience,
		SupportedSigningAlgs: o.Algorithms,
		// exp and nbf are checked below under the configured skew: go-oidc
		// allows no leeway on exp and a fixed five minutes on nbf.
		SkipExpiryCheck: true,
	})
	return &Verifier{verifier: verifier, claims: o.Claims, skew: o.ClockSkew, now: now}, nil
}

// Verify maps a verified token to its Principal.
func (v *Verifier) Verify(ctx context.Context, bearer string) (application.Principal, error) {
	tok, err := v.verifier.Verify(ctx, bearer)
	if err != nil {
		return application.Principal{}, fmt.Errorf("%w: %v", application.ErrUnauthenticated, err)
	}
	var claims map[string]any
	if err := tok.Claims(&claims); err != nil {
		return application.Principal{}, fmt.Errorf("%w: claims: %v", application.ErrUnauthenticated, err)
	}
	now := v.now()
	exp, ok := claims["exp"].(float64)
	if !ok {
		return application.Principal{}, fmt.Errorf("%w: the token has no exp", application.ErrUnauthenticated)
	}
	if !now.Before(time.Unix(int64(exp), 0).Add(v.skew)) {
		return application.Principal{}, fmt.Errorf("%w: the token expired", application.ErrUnauthenticated)
	}
	if nbf, ok := claims["nbf"].(float64); ok && now.Add(v.skew).Before(time.Unix(int64(nbf), 0)) {
		return application.Principal{}, fmt.Errorf("%w: the token is not valid yet", application.ErrUnauthenticated)
	}
	p := application.Principal{}
	for _, f := range []struct {
		path     string
		dst      *string
		required bool
	}{{v.claims.Tenant, &p.TenantID, true}, {v.claims.Actor, &p.ActorID, true}, {v.claims.Project, &p.ProjectID, true}} {
		if f.path == "" {
			continue
		}
		s, _ := lookup(claims, f.path).(string)
		if (f.required && s == "") || len(s) > 128 {
			return application.Principal{}, fmt.Errorf("%w: claim %q is missing or invalid", application.ErrUnauthenticated, f.path)
		}
		*f.dst = s
	}
	if v.claims.Roles != "" {
		roles, err := rolesOf(lookup(claims, v.claims.Roles))
		if err != nil {
			return application.Principal{}, fmt.Errorf("%w: claim %q: %v", application.ErrUnauthenticated, v.claims.Roles, err)
		}
		p.Roles = roles
	}
	return p, nil
}

func lookup(claims map[string]any, path string) any {
	var cur any = claims
	for _, part := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[part]
	}
	return cur
}

// rolesOf reads an absent claim as no roles and refuses anything but a
// bounded list of role names.
func rolesOf(v any) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	list, ok := v.([]any)
	if !ok || len(list) > 32 {
		return nil, errors.New("roles are a list of at most 32 role names")
	}
	out := make([]string, 0, len(list))
	seen := map[string]bool{}
	for _, item := range list {
		s, ok := item.(string)
		if !ok || !role.MatchString(s) {
			return nil, errors.New("a role name is not ^[a-z][a-z0-9_-]{0,63}$")
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out, nil
}
