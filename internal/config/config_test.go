package config_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
)

func write(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0o600))
	return path
}

const minimal = "control:\n  address: 127.0.0.1:9101\nauth:\n  mode: fixture\n"

var env = []string{"ANVILKIT_API_PRINCIPALS_FILE=/run/principals.json", "HOME=/root", "ANVILKIT_CONTROL_LISTEN=ignored-other-service",
	"ANVILKIT_API_IDENTITY_CERT_FILE=/etc/anvilkit/identity/tls.crt", "ANVILKIT_API_IDENTITY_KEY_FILE=/etc/anvilkit/identity/tls.key", "ANVILKIT_API_IDENTITY_CA_FILE=/etc/anvilkit/identity/ca.crt"}

// Defaults, then the file, then the allowed environment overrides.
func TestPrecedenceDefaultsFileEnvironment(t *testing.T) {
	c, err := config.LoadFrom(write(t, minimal+"http:\n  listen: 0.0.0.0:8080\nsse:\n  frame_buffer: 8\n"), append(env, "ANVILKIT_API_LISTEN=127.0.0.1:9999"))
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1:9999", c.HTTP.Listen, "environment overrides the file")
	require.Equal(t, 8, c.SSE.FrameBuffer, "file overrides the default")
	require.Equal(t, 15*time.Second, c.SSE.HeartbeatInterval, "default kept")
	require.Equal(t, int64(256<<10), c.HTTP.BodyLimitBytes)
	require.Equal(t, "/run/principals.json", c.Auth.PrincipalsFile)
	require.Equal(t, "127.0.0.1:9101", c.Control.Address)
}

// The checked-in service file is a valid candidate once the deployment
// placement is supplied.
func TestCheckedInFileLoads(t *testing.T) {
	c, err := config.LoadFrom(filepath.Join("..", "..", "config.yaml"), env)
	require.NoError(t, err)
	require.Equal(t, "fixture", c.Auth.Mode)
}

func TestUnknownKeysAreRejected(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal+"sse:\n  hartbeat_interval: 5s\n"), env)
	require.ErrorContains(t, err, "hartbeat_interval")
	_, err = config.LoadFrom(write(t, minimal+"database:\n  url: postgres://x\n"), env)
	require.ErrorContains(t, err, "database")
	_, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_API_SSE_FRAME_BUFFER=2"))
	require.ErrorContains(t, err, "ANVILKIT_API_SSE_FRAME_BUFFER", "only the allowlisted overrides exist")
}

func TestMissingFileFails(t *testing.T) {
	_, err := config.LoadFrom(filepath.Join(t.TempDir(), "absent.yaml"), env)
	require.Error(t, err)
}

func TestRequiredValues(t *testing.T) {
	_, err := config.LoadFrom(write(t, "auth:\n  mode: fixture\n"), env)
	require.ErrorContains(t, err, "control.address is required")
	_, err = config.LoadFrom(write(t, "control:\n  address: 127.0.0.1:9101\n"), env)
	require.ErrorContains(t, err, "auth.mode is required")
	_, err = config.LoadFrom(write(t, minimal), []string{})
	require.ErrorContains(t, err, "auth.principals_file is required")
	_, err = config.LoadFrom(write(t, "control:\n  address: 127.0.0.1:9101\nauth:\n  mode: oidc\n"), env)
	require.ErrorContains(t, err, "not implemented")
}

func TestRangesAndCrossFieldRules(t *testing.T) {
	_, err := config.LoadFrom(write(t, minimal+"sse:\n  frame_buffer: 0\n"), env)
	require.ErrorContains(t, err, "sse.frame_buffer")
	_, err = config.LoadFrom(write(t, minimal+"http:\n  body_limit_bytes: 10\n"), env)
	require.ErrorContains(t, err, "http.body_limit_bytes")
	_, err = config.LoadFrom(write(t, minimal+"sse:\n  write_timeout: 30s\n  slow_consumer_grace: 1s\n  heartbeat_interval: 1s\n"), env)
	require.ErrorContains(t, err, "sse.write_timeout 30s must not exceed")
	_, err = config.LoadFrom(write(t, minimal+"http:\n  shutdown_timeout: 0s\n"), env)
	require.ErrorContains(t, err, "http.shutdown_timeout")
	_, err = config.LoadFrom(write(t, minimal+"sse:\n  frame_buffer: many\n"), env)
	require.Error(t, err, "a value of the wrong type is rejected")
}

func TestTelemetryPlacement(t *testing.T) {
	c, err := config.LoadFrom(write(t, minimal+"telemetry:\n  otlp_tls:\n    ca_file: /etc/anvilkit/otlp-ca/ca.crt\n"), append(env, "ANVILKIT_API_TELEMETRY_OTLP_ENDPOINT=collector:4317", "ANVILKIT_API_TELEMETRY_METRICS_LISTEN=0.0.0.0:9190"))
	require.NoError(t, err)
	require.Equal(t, "collector:4317", c.Telemetry.OTLPEndpoint)
	require.Equal(t, "0.0.0.0:9190", c.Telemetry.MetricsListen)
	require.Equal(t, 1.0, c.Telemetry.SampleRatio)
	_, err = config.LoadFrom(write(t, minimal+"telemetry:\n  sample_ratio: 1.5\n"), env)
	require.ErrorContains(t, err, "telemetry.sample_ratio")
	_, err = config.LoadFrom(write(t, minimal+"http:\n  listen: 0.0.0.0:9100\ntelemetry:\n  metrics_listen: 0.0.0.0:9100\n"), env)
	require.ErrorContains(t, err, "must not be the public http.listen")
}

// TestClientIdentityAndDevelopmentGuard (P0.1): every gRPC client is mtls
// by default and resolves its files from the process identity; plaintext
// needs the connection's development mode and the top-level guard; the
// guard downgrades nothing by itself.
func TestClientIdentityAndDevelopmentGuard(t *testing.T) {
	c, err := config.LoadFrom(write(t, minimal), env)
	require.NoError(t, err)
	require.Equal(t, "mtls", c.Control.Identity.Mode)
	require.Equal(t, "/etc/anvilkit/identity/tls.key", c.Resolved(c.Control.Identity).KeyFile, "the client falls back to the process identity")
	require.False(t, c.Development.Enabled)

	_, err = config.LoadFrom(write(t, minimal), env[:3])
	require.ErrorContains(t, err, "control.identity.mtls needs cert_file, key_file and ca_file")
	c, err = config.LoadFrom(write(t, "control:\n  address: 127.0.0.1:9101\n  identity:\n    mtls:\n      cert_file: /c\n      key_file: /k\n      ca_file: /ca\nauth:\n  mode: fixture\n"), env[:3])
	require.NoError(t, err, "a client may name its own files")
	require.Equal(t, "/k", c.Resolved(c.Control.Identity).KeyFile)

	dev := "control:\n  address: 127.0.0.1:9101\n  identity:\n    mode: development\nauth:\n  mode: fixture\n"
	_, err = config.LoadFrom(write(t, dev), env)
	require.ErrorContains(t, err, "control.identity.mode development (plaintext) requires development.enabled")
	c, err = config.LoadFrom(write(t, dev+"development:\n  enabled: true\n"), append(env, "ANVILKIT_API_KNOWLEDGE_ADDRESS=127.0.0.1:9105"))
	require.NoError(t, err)
	require.Equal(t, "development", c.Control.Identity.Mode)
	require.Equal(t, "mtls", c.Knowledge.Identity.Mode, "the guard does not downgrade other connections")
	_, err = config.LoadFrom(write(t, "control:\n  address: 127.0.0.1:9101\n  identity:\n    mode: plaintext\nauth:\n  mode: fixture\n"), env)
	require.ErrorContains(t, err, "control.identity.mode")
	_, err = config.LoadFrom(write(t, minimal+"development:\n  enabled: true\n"), append(env, "ANVILKIT_API_DEVELOPMENT_ENABLED=true"))
	require.ErrorContains(t, err, "not allowed overrides", "the guard is file-only")

	_, err = config.LoadFrom(write(t, minimal), append(env, "ANVILKIT_API_TELEMETRY_OTLP_ENDPOINT=collector:4317"))
	require.ErrorContains(t, err, "telemetry.otlp_tls.ca_file is required")
	_, err = config.LoadFrom(write(t, minimal+"telemetry:\n  otlp_tls:\n    mode: development\n"), append(env, "ANVILKIT_API_TELEMETRY_OTLP_ENDPOINT=collector:4317"))
	require.ErrorContains(t, err, "telemetry.otlp_tls.mode development (plaintext) requires development.enabled")
	_, err = config.LoadFrom(write(t, minimal+"telemetry:\n  otlp_tls:\n    ca_file: /ca\n"), append(env, "ANVILKIT_API_TELEMETRY_OTLP_ENDPOINT=collector:4317"))
	require.NoError(t, err)
}
