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

var env = []string{"ANVILKIT_API_PRINCIPALS_FILE=/run/principals.json", "HOME=/root", "ANVILKIT_CONTROL_LISTEN=ignored-other-service"}

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
