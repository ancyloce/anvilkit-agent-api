package oidcauth_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/oidcauth"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
)

// issuer is a local OpenID provider: discovery and a key set with one
// RS256 key.
type issuer struct {
	srv  *httptest.Server
	key  *rsa.PrivateKey
	kid  string
	jwks int
}

func newIssuer(t *testing.T) *issuer {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	is := &issuer{key: key, kid: "k1"}
	mux := http.NewServeMux()
	is.srv = httptest.NewServer(mux)
	t.Cleanup(is.srv.Close)
	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"issuer": is.srv.URL, "jwks_uri": is.srv.URL + "/jwks", "authorization_endpoint": is.srv.URL + "/authorize",
			"token_endpoint": is.srv.URL + "/token", "id_token_signing_alg_values_supported": []string{"RS256"},
		})
	})
	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		is.jwks++
		_ = json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &key.PublicKey, KeyID: is.kid, Algorithm: "RS256", Use: "sig"}}})
	})
	return is
}

func sign(t *testing.T, key any, alg jose.SignatureAlgorithm, kid string, claims map[string]any) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: alg, Key: key}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", kid))
	require.NoError(t, err)
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	jws, err := signer.Sign(payload)
	require.NoError(t, err)
	out, err := jws.CompactSerialize()
	require.NoError(t, err)
	return out
}

// P0.3 AC1: every malformed, foreign or stale token is refused; a valid
// one yields the mapped principal including its roles.
func TestVerifier(t *testing.T) {
	is := newIssuer(t)
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	v, err := oidcauth.New(context.Background(), oidcauth.Options{
		Issuer: is.srv.URL, Audience: "anvilkit-agent-api", Algorithms: []string{"RS256"}, ClockSkew: 30 * time.Second,
		Claims: oidcauth.Claims{Tenant: "tenant", Project: "ext.project", Actor: "sub", Roles: "roles"}, Now: func() time.Time { return now },
	})
	require.NoError(t, err)
	claims := func(change func(map[string]any)) map[string]any {
		c := map[string]any{
			"iss": is.srv.URL, "aud": "anvilkit-agent-api", "sub": "user_a", "tenant": "tenant_a", "ext": map[string]any{"project": "proj_a"},
			"roles": []string{"author", "operator"}, "iat": now.Unix(), "nbf": now.Unix(), "exp": now.Add(time.Hour).Unix(),
		}
		if change != nil {
			change(c)
		}
		return c
	}
	valid := sign(t, is.key, jose.RS256, is.kid, claims(nil))
	p, err := v.Verify(context.Background(), valid)
	require.NoError(t, err)
	require.Equal(t, application.Principal{TenantID: "tenant_a", ProjectID: "proj_a", ActorID: "user_a", Roles: []string{"author", "operator"}}, p)
	within, err := v.Verify(context.Background(), sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { c["exp"] = now.Add(-10 * time.Second).Unix() })))
	require.NoError(t, err, "an exp inside the bounded skew")
	require.Equal(t, "tenant_a", within.TenantID)

	other, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT","kid":"k1"}`))
	body, _ := json.Marshal(claims(nil))
	parts := strings.Split(valid, ".")
	tamperedBody, _ := json.Marshal(claims(func(c map[string]any) { c["tenant"] = "tenant_b" }))
	for name, token := range map[string]string{
		"expired":              sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { c["exp"] = now.Add(-time.Minute).Unix() })),
		"no exp":               sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { delete(c, "exp") })),
		"not valid yet":        sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { c["nbf"] = now.Add(time.Minute).Unix() })),
		"wrong audience":       sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { c["aud"] = "another-service" })),
		"wrong issuer":         sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { c["iss"] = "https://idp.example.invalid" })),
		"unsigned (alg none)":  header + "." + base64.RawURLEncoding.EncodeToString(body) + ".",
		"HS256 with a secret":  sign(t, []byte("a shared secret of thirty-two bytes!"), jose.HS256, is.kid, claims(nil)),
		"tampered signature":   parts[0] + "." + base64.RawURLEncoding.EncodeToString(tamperedBody) + "." + parts[2],
		"unknown kid":          sign(t, other, jose.RS256, "k2", claims(nil)),
		"foreign key, own kid": sign(t, other, jose.RS256, is.kid, claims(nil)),
		"no tenant claim":      sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { delete(c, "tenant") })),
		"no project claim":     sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { delete(c, "ext") })),
		"roles not names":      sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { c["roles"] = []string{"Operator!"} })),
		"roles not a list":     sign(t, is.key, jose.RS256, is.kid, claims(func(c map[string]any) { c["roles"] = "operator" })),
		"not a token":          "dev-token-tenant-a-0123456789abcdef",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := v.Verify(context.Background(), token)
			require.True(t, errors.Is(err, application.ErrUnauthenticated), "%v", err)
		})
	}
	require.GreaterOrEqual(t, is.jwks, 2, "an unknown kid refetched the key set")
}

// Discovery must name exactly the configured issuer.
func TestDiscoveryNamesTheIssuer(t *testing.T) {
	is := newIssuer(t)
	_, err := oidcauth.New(context.Background(), oidcauth.Options{
		Issuer: is.srv.URL + "/", Audience: "anvilkit-agent-api", Algorithms: []string{"RS256"},
		Claims: oidcauth.Claims{Tenant: "tenant", Actor: "sub"},
	})
	require.Error(t, err)
}
