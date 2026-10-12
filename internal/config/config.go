// Package config builds the API's immutable configuration snapshot with
// koanf (A09, DD-09 §4). Precedence is defaults < the service's reviewed
// configuration file < the allowed ANVILKIT_API_* environment overrides. The
// candidate is validated (unknown keys, required values, ranges, cross-field
// rules) before anything else starts; business code receives the validated
// value and never reads the environment or a mutable global.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/confmap"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

const (
	envPrefix = "ANVILKIT_API_"
	// EnvConfigFile names the configuration file; it is the only variable
	// read before the snapshot exists.
	EnvConfigFile = "ANVILKIT_API_CONFIG"
	// DefaultConfigFile is the file next to the service (config.yaml in the
	// working directory) when EnvConfigFile is unset.
	DefaultConfigFile = "config.yaml"
)

// HTTP is the public listener.
type HTTP struct {
	Listen            string        `koanf:"listen"`
	ReadHeaderTimeout time.Duration `koanf:"read_header_timeout"`
	BodyLimitBytes    int64         `koanf:"body_limit_bytes"`
	ShutdownTimeout   time.Duration `koanf:"shutdown_timeout"`
}

// Control is the gRPC dependency.
type Control struct {
	Address  string         `koanf:"address"`
	Identity ClientIdentity `koanf:"identity"`
}

// Owner is the placement of an owning service the API gateways to (P20:
// Knowledge, MCP); an empty address leaves its endpoints answering
// DEPENDENCY_UNAVAILABLE.
type Owner struct {
	Address  string         `koanf:"address"`
	Timeout  time.Duration  `koanf:"timeout"`
	Identity ClientIdentity `koanf:"identity"`
}

// ClientIdentity is how one outbound gRPC connection authenticates (P0.1):
// mtls presents the workload certificate (the files of MTLS, defaulting to
// the process identity) and verifies the server by server_name;
// development is plaintext and is admitted only with development.enabled.
type ClientIdentity struct {
	Mode string    `koanf:"mode"`
	MTLS MTLSFiles `koanf:"mtls"`
}

// MTLSFiles names certificate, key, CA bundle and the expected server name
// (a DNS SAN of the server certificate).
type MTLSFiles struct {
	CertFile   string `koanf:"cert_file"`
	KeyFile    string `koanf:"key_file"`
	CAFile     string `koanf:"ca_file"`
	ServerName string `koanf:"server_name"`
}

// Identity is the process's own workload identity material (P0.1):
// placements (ANVILKIT_API_IDENTITY_{CERT,KEY,CA}_FILE) every mtls client
// block falls back to; watched and reloaded as a whole.
type Identity struct {
	CertFile       string        `koanf:"cert_file"`
	KeyFile        string        `koanf:"key_file"`
	CAFile         string        `koanf:"ca_file"`
	ReloadInterval time.Duration `koanf:"reload_interval"`
}

// Development is the top-level DEVELOPMENT_ONLY guard: every plaintext
// connection of this process requires both its own development mode and
// Enabled; Enabled alone downgrades nothing. File-only.
type Development struct {
	Enabled bool `koanf:"enabled"`
}

// ClientTLS is the transport of an outbound connection whose server is not
// an AnvilKit workload (the OTLP collector): development is plaintext
// (admitted only with development.enabled), tls verifies the server
// against ca_file and server_name, mtls additionally presents cert_file
// and key_file.
type ClientTLS struct {
	Mode       string `koanf:"mode"`
	CAFile     string `koanf:"ca_file"`
	CertFile   string `koanf:"cert_file"`
	KeyFile    string `koanf:"key_file"`
	ServerName string `koanf:"server_name"`
}

// Auth selects the identity protocol (P0.3): oidc verifies bearer JWTs of
// one OpenID Connect issuer (ENV-07 supplies the production issuer,
// audience and claim schema); fixture is the DEVELOPMENT_ONLY principals
// file, admitted only with development.enabled.
type Auth struct {
	Mode           string `koanf:"mode"`
	PrincipalsFile string `koanf:"principals_file"`
	OIDC           OIDC   `koanf:"oidc"`
}

// OIDC is one issuer: discovery and its key set, the audience, the signing
// algorithm allowlist, the bounded clock skew of exp/nbf and the claim
// paths of the principal. An http:// issuer is DEVELOPMENT_ONLY.
type OIDC struct {
	Issuer     string        `koanf:"issuer"`
	Audience   string        `koanf:"audience"`
	Algorithms []string      `koanf:"algorithms"`
	ClockSkew  time.Duration `koanf:"clock_skew"`
	// CAFile verifies the issuer's TLS server (empty: the system roots).
	CAFile  string        `koanf:"ca_file"`
	Timeout time.Duration `koanf:"timeout"`
	Claims  OIDCClaims    `koanf:"claims"`
}

// OIDCClaims names the claim (or dot-separated path) of each principal
// field; tenant and actor are required, an empty project or roles path
// reads none.
type OIDCClaims struct {
	Tenant  string `koanf:"tenant"`
	Project string `koanf:"project"`
	Actor   string `koanf:"actor"`
	Roles   string `koanf:"roles"`
}

// signingAlgorithms are the asymmetric JWS algorithms a configuration may
// allow; "none" and the HMAC family never verify an issuer's token.
var signingAlgorithms = map[string]bool{"RS256": true, "RS384": true, "RS512": true, "ES256": true, "ES384": true, "ES512": true, "PS256": true, "PS384": true, "PS512": true, "EdDSA": true}

// SSE bounds the event stream transport (contracts.md §6): they are
// transport limits, never business clocks.
type SSE struct {
	HeartbeatInterval time.Duration `koanf:"heartbeat_interval"`
	FrameBuffer       int           `koanf:"frame_buffer"`
	SlowConsumerGrace time.Duration `koanf:"slow_consumer_grace"`
	WriteTimeout      time.Duration `koanf:"write_timeout"`
	// MaxStreams bounds the open event streams of the process and
	// MaxStreamsPerPrincipal those of one principal (P1.4, API-03); the
	// stream advertises RetryBase (jittered) as the EventSource reconnect
	// interval.
	MaxStreams             int           `koanf:"max_streams"`
	MaxStreamsPerPrincipal int           `koanf:"max_streams_per_principal"`
	RetryBase              time.Duration `koanf:"retry_base"`
}

// Artifacts bounds the API's side of API-12: the deadline it sets on a
// transfer it begins. Control bounds it further.
type Artifacts struct {
	TransferWindow time.Duration `koanf:"transfer_window"`
}

// Preparations names the reviewed operation profile API-01 accepts a
// prompt into (Control decides whether it is qualified).
type Preparations struct {
	Profile string `koanf:"profile"`
}

// Telemetry places the redacted signals (security.md "data classification,
// logging and deletion"): spans over OTLP to the collector when an endpoint
// is placed (no exporter otherwise), sampled at SampleRatio, and the
// Prometheus metrics on their own listener, never on the public one.
type Telemetry struct {
	OTLPEndpoint  string  `koanf:"otlp_endpoint"`
	SampleRatio   float64 `koanf:"sample_ratio"`
	MetricsListen string  `koanf:"metrics_listen"`
	// OTLPTLS is validated only while an endpoint is placed.
	OTLPTLS ClientTLS `koanf:"otlp_tls"`
}

type Config struct {
	Development  Development  `koanf:"development"`
	Identity     Identity     `koanf:"identity"`
	Telemetry    Telemetry    `koanf:"telemetry"`
	HTTP         HTTP         `koanf:"http"`
	Control      Control      `koanf:"control"`
	Auth         Auth         `koanf:"auth"`
	SSE          SSE          `koanf:"sse"`
	Artifacts    Artifacts    `koanf:"artifacts"`
	Preparations Preparations `koanf:"preparations"`
	Knowledge    Owner        `koanf:"knowledge"`
	MCP          Owner        `koanf:"mcp"`
}

// defaults are the reviewed baseline values; the file and the allowed
// overrides refine them.
var defaults = map[string]any{
	"http.listen":                   "127.0.0.1:9100",
	"http.read_header_timeout":      "10s",
	"http.body_limit_bytes":         256 << 10,
	"http.shutdown_timeout":         "20s",
	"sse.heartbeat_interval":        "15s",
	"sse.frame_buffer":              64,
	"sse.slow_consumer_grace":       "5s",
	"sse.write_timeout":             "10s",
	"sse.max_streams":               512,
	"sse.max_streams_per_principal": 32,
	"sse.retry_base":                "3s",
	"artifacts.transfer_window":     "15m",
	"knowledge.timeout":             "30s",
	"mcp.timeout":                   "60s",
	"preparations.profile":          "preparation-v1",
	"telemetry.sample_ratio":        1.0,
	"development.enabled":           false,
	"identity.reload_interval":      "5s",
	"control.identity.mode":         "mtls",
	"knowledge.identity.mode":       "mtls",
	"mcp.identity.mode":             "mtls",
	"telemetry.otlp_tls.mode":       "tls",
	"auth.mode":                     "oidc",
	"auth.oidc.audience":            "anvilkit-agent-api",
	"auth.oidc.algorithms":          []string{"RS256", "ES256"},
	"auth.oidc.clock_skew":          "30s",
	"auth.oidc.timeout":             "10s",
	"auth.oidc.claims.tenant":       "tenant",
	"auth.oidc.claims.project":      "project",
	"auth.oidc.claims.actor":        "sub",
	"auth.oidc.claims.roles":        "roles",
}

// envOverrides is the complete set of environment variables the service
// accepts: deployment placement only. Any other ANVILKIT_API_* variable is an
// unknown key and rejects the candidate.
var envOverrides = map[string]string{
	"ANVILKIT_API_LISTEN":                   "http.listen",
	"ANVILKIT_API_CONTROL_ADDRESS":          "control.address",
	"ANVILKIT_API_AUTH_MODE":                "auth.mode",
	"ANVILKIT_API_PRINCIPALS_FILE":          "auth.principals_file",
	"ANVILKIT_API_AUTH_OIDC_ISSUER":         "auth.oidc.issuer",
	"ANVILKIT_API_AUTH_OIDC_AUDIENCE":       "auth.oidc.audience",
	"ANVILKIT_API_AUTH_OIDC_CA_FILE":        "auth.oidc.ca_file",
	"ANVILKIT_API_KNOWLEDGE_ADDRESS":        "knowledge.address",
	"ANVILKIT_API_MCP_ADDRESS":              "mcp.address",
	"ANVILKIT_API_TELEMETRY_OTLP_ENDPOINT":  "telemetry.otlp_endpoint",
	"ANVILKIT_API_TELEMETRY_METRICS_LISTEN": "telemetry.metrics_listen",
	"ANVILKIT_API_IDENTITY_CERT_FILE":       "identity.cert_file",
	"ANVILKIT_API_IDENTITY_KEY_FILE":        "identity.key_file",
	"ANVILKIT_API_IDENTITY_CA_FILE":         "identity.ca_file",
}

// Load builds the snapshot from the file named by EnvConfigFile (or
// DefaultConfigFile) and the process environment.
func Load() (Config, error) {
	path := os.Getenv(EnvConfigFile)
	if path == "" {
		path = DefaultConfigFile
	}
	return LoadFrom(path, os.Environ())
}

// LoadFrom is Load with explicit inputs (tests).
func LoadFrom(path string, environ []string) (Config, error) {
	k := koanf.New(".")
	if err := k.Load(confmap.Provider(defaults, "."), nil); err != nil {
		return Config{}, err
	}
	if err := k.Load(file.Provider(path), yaml.Parser()); err != nil {
		return Config{}, fmt.Errorf("config file %s: %w", path, err)
	}
	if err := applyEnv(k, environ); err != nil {
		return Config{}, err
	}
	var c Config
	if err := k.UnmarshalWithConf("", &c, koanf.UnmarshalConf{DecoderConfig: &mapstructure.DecoderConfig{
		DecodeHook:       mapstructure.StringToTimeDurationHookFunc(),
		ErrorUnused:      true, // unknown keys in the file or the overrides reject the candidate
		WeaklyTypedInput: true,
		Result:           &c,
	}}); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return c, c.validate()
}

func applyEnv(k *koanf.Koanf, environ []string) error {
	var unknown []string
	for _, kv := range environ {
		name, value, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(name, envPrefix) || name == EnvConfigFile {
			continue
		}
		key, ok := envOverrides[name]
		if !ok {
			unknown = append(unknown, name)
			continue
		}
		if err := k.Set(key, value); err != nil {
			return err
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return fmt.Errorf("config: environment variables are not allowed overrides: %s", strings.Join(unknown, ", "))
	}
	return nil
}

func (c Config) validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	req("http.listen", c.HTTP.Listen)
	req("control.address", c.Control.Address)
	if c.Telemetry.SampleRatio < 0 || c.Telemetry.SampleRatio > 1 {
		errs = append(errs, fmt.Errorf("telemetry.sample_ratio %v outside [0, 1]", c.Telemetry.SampleRatio))
	}
	if c.Telemetry.MetricsListen != "" && c.Telemetry.MetricsListen == c.HTTP.Listen {
		errs = append(errs, errors.New("telemetry.metrics_listen must not be the public http.listen"))
	}
	for name, o := range map[string]Owner{"knowledge": c.Knowledge, "mcp": c.MCP} {
		if o.Timeout < time.Second || o.Timeout > 10*time.Minute {
			errs = append(errs, fmt.Errorf("%s.timeout %s outside [1s, 10m]", name, o.Timeout))
		}
	}
	if c.HTTP.ReadHeaderTimeout < time.Second || c.HTTP.ReadHeaderTimeout > time.Minute {
		errs = append(errs, fmt.Errorf("http.read_header_timeout %s outside [1s, 1m]", c.HTTP.ReadHeaderTimeout))
	}
	if c.HTTP.BodyLimitBytes < 1<<10 || c.HTTP.BodyLimitBytes > 16<<20 {
		errs = append(errs, fmt.Errorf("http.body_limit_bytes %d outside [1KiB, 16MiB]", c.HTTP.BodyLimitBytes))
	}
	if c.HTTP.ShutdownTimeout < time.Second || c.HTTP.ShutdownTimeout > MaxShutdownTimeout {
		errs = append(errs, fmt.Errorf("http.shutdown_timeout %s outside [1s, 5m]", c.HTTP.ShutdownTimeout))
	}
	switch c.Auth.Mode {
	case "oidc":
		errs = append(errs, c.Auth.OIDC.validate(c.Development.Enabled)...)
	case "fixture":
		if !c.Development.Enabled {
			errs = append(errs, errors.New("auth.mode fixture (the principals file) requires development.enabled: true (DEVELOPMENT_ONLY); production verifies an OIDC issuer"))
		}
		req("auth.principals_file", c.Auth.PrincipalsFile)
	default:
		errs = append(errs, fmt.Errorf("auth.mode %q is not one of oidc, fixture", c.Auth.Mode))
	}
	if c.SSE.HeartbeatInterval < time.Second || c.SSE.HeartbeatInterval > 5*time.Minute {
		errs = append(errs, fmt.Errorf("sse.heartbeat_interval %s outside [1s, 5m]", c.SSE.HeartbeatInterval))
	}
	if c.SSE.FrameBuffer < 1 || c.SSE.FrameBuffer > 4096 {
		errs = append(errs, fmt.Errorf("sse.frame_buffer %d outside [1, 4096]", c.SSE.FrameBuffer))
	}
	if c.SSE.SlowConsumerGrace < 100*time.Millisecond || c.SSE.SlowConsumerGrace > time.Minute {
		errs = append(errs, fmt.Errorf("sse.slow_consumer_grace %s outside [100ms, 1m]", c.SSE.SlowConsumerGrace))
	}
	if c.SSE.WriteTimeout < 100*time.Millisecond || c.SSE.WriteTimeout > time.Minute {
		errs = append(errs, fmt.Errorf("sse.write_timeout %s outside [100ms, 1m]", c.SSE.WriteTimeout))
	}
	// A stalled write must be given up before the producer's grace expires,
	// otherwise the handler could block past the bound it promises.
	if c.SSE.WriteTimeout > c.SSE.SlowConsumerGrace+c.SSE.HeartbeatInterval {
		errs = append(errs, fmt.Errorf("sse.write_timeout %s must not exceed slow_consumer_grace + heartbeat_interval", c.SSE.WriteTimeout))
	}
	if c.SSE.MaxStreams < 1 || c.SSE.MaxStreams > 65536 {
		errs = append(errs, fmt.Errorf("sse.max_streams %d outside [1, 65536]", c.SSE.MaxStreams))
	}
	if c.SSE.MaxStreamsPerPrincipal < 1 || c.SSE.MaxStreamsPerPrincipal > c.SSE.MaxStreams {
		errs = append(errs, fmt.Errorf("sse.max_streams_per_principal %d outside [1, sse.max_streams]", c.SSE.MaxStreamsPerPrincipal))
	}
	if c.SSE.RetryBase < time.Second || c.SSE.RetryBase > time.Minute {
		errs = append(errs, fmt.Errorf("sse.retry_base %s outside [1s, 1m]", c.SSE.RetryBase))
	}
	if c.Preparations.Profile == "" {
		errs = append(errs, errors.New("preparations.profile is required"))
	}
	if c.Artifacts.TransferWindow < time.Minute || c.Artifacts.TransferWindow > 24*time.Hour {
		errs = append(errs, fmt.Errorf("artifacts.transfer_window %s outside [1m, 24h]", c.Artifacts.TransferWindow))
	}
	errs = append(errs, c.validateIdentity()...)
	return errors.Join(errs...)
}

// validateIdentity checks the client identities, the top-level guard and
// the collector transport (P0.1). A client under mtls resolves its files
// through Resolved; an unresolved file is reported here.
func (c Config) validateIdentity() []error {
	var errs []error
	if c.Identity.ReloadInterval < 100*time.Millisecond || c.Identity.ReloadInterval > time.Hour {
		errs = append(errs, fmt.Errorf("identity.reload_interval %s outside [100ms, 1h]", c.Identity.ReloadInterval))
	}
	clients := map[string]ClientIdentity{"control": c.Control.Identity}
	if c.Knowledge.Address != "" {
		clients["knowledge"] = c.Knowledge.Identity
	}
	if c.MCP.Address != "" {
		clients["mcp"] = c.MCP.Identity
	}
	for name, ci := range clients {
		switch ci.Mode {
		case "mtls":
			m := c.Resolved(ci)
			if m.CertFile == "" || m.KeyFile == "" || m.CAFile == "" {
				errs = append(errs, fmt.Errorf("%s.identity.mtls needs cert_file, key_file and ca_file, or the process identity (ANVILKIT_API_IDENTITY_{CERT,KEY,CA}_FILE)", name))
			}
		case "development":
			if !c.Development.Enabled {
				errs = append(errs, fmt.Errorf("%s.identity.mode development (plaintext) requires development.enabled: true (DEVELOPMENT_ONLY)", name))
			}
		default:
			errs = append(errs, fmt.Errorf("%s.identity.mode %q is not one of mtls, development", name, ci.Mode))
		}
	}
	if c.Telemetry.OTLPEndpoint != "" {
		errs = append(errs, c.Telemetry.OTLPTLS.validate("telemetry.otlp_tls", c.Development.Enabled)...)
	}
	return errs
}

// Resolved fills a client's mtls files from the process identity and its
// server name from nothing (the caller supplies the target's name).
func (c Config) Resolved(ci ClientIdentity) MTLSFiles {
	m := ci.MTLS
	if m.CertFile == "" {
		m.CertFile = c.Identity.CertFile
	}
	if m.KeyFile == "" {
		m.KeyFile = c.Identity.KeyFile
	}
	if m.CAFile == "" {
		m.CAFile = c.Identity.CAFile
	}
	return m
}

func (t ClientTLS) validate(name string, development bool) []error {
	var errs []error
	switch t.Mode {
	case "development":
		if !development {
			errs = append(errs, fmt.Errorf("%s.mode development (plaintext) requires development.enabled: true (DEVELOPMENT_ONLY)", name))
		}
	case "tls":
		if t.CAFile == "" {
			errs = append(errs, fmt.Errorf("%s.ca_file is required under %s.mode tls", name, name))
		}
	case "mtls":
		if t.CAFile == "" || t.CertFile == "" || t.KeyFile == "" {
			errs = append(errs, fmt.Errorf("%s.ca_file, cert_file and key_file are required under %s.mode mtls", name, name))
		}
	default:
		errs = append(errs, fmt.Errorf("%s.mode %q is not one of development, tls, mtls", name, t.Mode))
	}
	return errs
}

func (o OIDC) validate(development bool) []error {
	var errs []error
	u, err := url.Parse(o.Issuer)
	switch {
	case o.Issuer == "":
		errs = append(errs, errors.New("auth.oidc.issuer is required under auth.mode oidc (ENV-07)"))
	case err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && u.Scheme != "http"):
		errs = append(errs, fmt.Errorf("auth.oidc.issuer %q is not an absolute https URL", o.Issuer))
	case u.Scheme == "http" && !development:
		errs = append(errs, errors.New("auth.oidc.issuer over http:// requires development.enabled: true (DEVELOPMENT_ONLY)"))
	}
	if o.Audience == "" {
		errs = append(errs, errors.New("auth.oidc.audience is required"))
	}
	if len(o.Algorithms) == 0 {
		errs = append(errs, errors.New("auth.oidc.algorithms names at least one signing algorithm"))
	}
	for _, a := range o.Algorithms {
		if !signingAlgorithms[a] {
			errs = append(errs, fmt.Errorf("auth.oidc.algorithms: %q is not an asymmetric JWS algorithm", a))
		}
	}
	if o.ClockSkew < 0 || o.ClockSkew > 5*time.Minute {
		errs = append(errs, fmt.Errorf("auth.oidc.clock_skew %s outside [0, 5m]", o.ClockSkew))
	}
	if o.Timeout < time.Second || o.Timeout > time.Minute {
		errs = append(errs, fmt.Errorf("auth.oidc.timeout %s outside [1s, 1m]", o.Timeout))
	}
	if o.Claims.Tenant == "" || o.Claims.Actor == "" {
		errs = append(errs, errors.New("auth.oidc.claims.tenant and auth.oidc.claims.actor are required"))
	}
	return errs
}

// MaxShutdownTimeout is the longest drain http.shutdown_timeout may set; the
// process's own stop bound lies above it (P1.7).
const MaxShutdownTimeout = 5 * time.Minute
