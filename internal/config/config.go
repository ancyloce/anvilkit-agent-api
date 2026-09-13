// Package config resolves the anvilkit-agent-api runtime configuration from the
// process environment.
//
// The architecture supplies no ports, endpoints or credentials, so the listener
// and dependency addresses are required deployment inputs with no built-in
// default. Values owned by the parent pilot limit profile
// (contracts/profiles/pilot-limits-v1.json) are compiled in as named constants
// instead, so a deployment cannot quietly widen a contract ceiling.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"time"
)

// Limits mirrored from contracts/profiles/pilot-limits-v1.json. These are pilot
// and development defaults recorded by the architecture, not measured capacity.
const (
	// RequestBodyMaxBytes is ingress.commandBodyMaxBytes, which is also the
	// x-anvilkit-max-body-bytes ceiling of POST /v1/definitions/validations.
	RequestBodyMaxBytes = 65536

	// LocalCheckBodyMaxBytes is localCheck.commandBodyMaxBytes: the ceiling of
	// POST /v1/local-checks, whose body carries only a command identifier and a
	// retained fixture identifier.
	LocalCheckBodyMaxBytes = 1024

	// PreparationBodyMaxBytes is preparation.commandBodyMaxBytes: the ceiling of
	// POST /v1/operations/preparations and /preparation-answers, whose bodies
	// carry the inline input record or answer set (S2, 2026-09-13).
	PreparationBodyMaxBytes = 16384

	// ValidationMaxIssues is definition.validationMaxIssues.
	ValidationMaxIssues = 100

	// DrainGrace is api.drainGraceSeconds: readiness turns false, new requests
	// are refused and in-flight requests finish inside this window.
	DrainGrace = 30 * time.Second

	// ReadinessReadRoleWindow is api.readinessReadRoleWindowSeconds: readiness
	// requires the projection read role to have answered inside this window.
	ReadinessReadRoleWindow = 10 * time.Second

	// ClockMaxInterServiceError is clock.maxInterServiceErrorSeconds. A
	// disclosure grant within this margin of its expiry counts as expired, so
	// the API can only ever be early about revocation, never late.
	ClockMaxInterServiceError = 2 * time.Second

	// AuthorizationFreshness is authorization.freshnessSeconds: the longest a
	// stream may hold one disclosure grant before renewing it.
	AuthorizationFreshness = 30 * time.Second

	// ReplayPageEvents is sse.replayPageEvents.
	ReplayPageEvents = 200

	// SubscriberBufferEvents and SubscriberBufferBytes are
	// sse.subscriberBufferEvents and sse.subscriberBufferBytes: the per-
	// subscriber backlog this replica holds before it stops advancing that
	// subscriber's position.
	SubscriberBufferEvents = 256
	SubscriberBufferBytes  = 1048576

	// SlowConsumerClose is sse.slowConsumerCloseSeconds: how long a subscriber
	// may fail to drain before its connection is closed.
	SlowConsumerClose = 30 * time.Second

	// Heartbeat is sse.heartbeatSeconds. A heartbeat is a comment line with no
	// id, so it never advances a durable cursor.
	Heartbeat = 15 * time.Second

	// SharedCatchup is sse.sharedCatchupSeconds: the interval of the catch-up
	// read shared by every subscriber of an operation.
	SharedCatchup = time.Second

	// SnapshotProtection is sse.snapshotProtectionSeconds: how long the pages
	// of one snapshot stay claimable before the handshake restarts.
	SnapshotProtection = 60 * time.Second

	// ReconnectHintMin and ReconnectHintMax are sse.reconnectHintMinSeconds and
	// sse.reconnectHintMaxSeconds: the randomized pause a drained replica asks
	// a client to wait, so a drain does not return as a synchronized reconnect.
	ReconnectHintMin = 1 * time.Second
	ReconnectHintMax = 5 * time.Second
)

// Environment variable names. Every input this service reads is listed here so
// the README and the deployment record describe the same surface.
const (
	EnvPublicListen        = "ANVILKIT_API_PUBLIC_LISTEN"
	EnvPrivateListen       = "ANVILKIT_API_PRIVATE_LISTEN"
	EnvControlEndpoint     = "ANVILKIT_API_CONTROL_ENDPOINT"
	EnvControlCA           = "ANVILKIT_API_CONTROL_CA"
	EnvControlCert         = "ANVILKIT_API_CONTROL_CERT"
	EnvControlKey          = "ANVILKIT_API_CONTROL_KEY"
	EnvValidationToken     = "ANVILKIT_API_CONTROL_VALIDATION_TOKEN"
	EnvReadDSN             = "ANVILKIT_API_READ_DSN"
	EnvControlCallTimeout  = "ANVILKIT_API_CONTROL_TIMEOUT_SECONDS"
	EnvIdentityProfilePath = "ANVILKIT_API_IDENTITY_PROFILE"
	EnvProfile             = "ANVILKIT_API_PROFILE"
	EnvEnvironment         = "ANVILKIT_API_ENVIRONMENT"
	EnvServiceVersion      = "ANVILKIT_API_SERVICE_VERSION"
	EnvServiceInstanceID   = "ANVILKIT_API_SERVICE_INSTANCE_ID"
)

// Serving profiles. The profile decides which public operations exist at all,
// so a route reserved for controlled local verification cannot be reached in a
// standard deployment by any request, header or body an attacker can send.
const (
	// ProfileStandard serves the business surface only.
	ProfileStandard = "standard"
	// ProfileControlledLocal additionally registers POST /v1/local-checks for
	// the fixed local-check profile (DD-02 #local-check-admission). Control
	// enforces the same restriction independently; neither side relies on the
	// other to keep the route out of a deployment.
	ProfileControlledLocal = "controlled-local"
)

const (
	defaultControlCallTimeout = 10 * time.Second
	maxControlCallTimeout     = 120 * time.Second
	defaultEnvironment        = "local"
	defaultServiceVersion     = "0.0.0-development"
)

// identifierPattern is urn:anvilkit:values:v1#/$defs/id.
var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]*$`)

// Config is the resolved runtime configuration of one API process.
type Config struct {
	// PublicListenAddress serves the tenant-facing HTTPS surface.
	PublicListenAddress string
	// PrivateListenAddress serves /healthz and /readyz only. The architecture
	// keeps health off the public listener.
	PrivateListenAddress string
	// ControlEndpoint is the authenticated base URL of anvilkit-agent-control.
	ControlEndpoint string
	// Control TLS inputs authenticate this service; the validation credential
	// is the existing controlled method grant, separate from public tokens.
	ControlCA, ControlCert, ControlKey, ValidationToken string
	// ReadDSN connects the restricted anvilkit_api_ro role to the Agent
	// database. Pool sizing travels in the DSN because the architecture records
	// connection-pool values as a per-environment input.
	ReadDSN string
	// ControlCallTimeout bounds one Control call attempt.
	ControlCallTimeout time.Duration
	// IdentityProfilePath points at the controlled identity profile. An empty
	// path leaves the profile empty, which denies every protected route.
	IdentityProfilePath string
	// Profile is the serving profile: ProfileStandard or ProfileControlledLocal.
	// It defaults to ProfileStandard, so the local surface is opt-in.
	Profile string
	// Environment, ServiceVersion and ServiceInstanceID are required by
	// contracts/telemetry/log-record-v1.schema.json.
	Environment       string
	ServiceVersion    string
	ServiceInstanceID string
}

// Load reads the configuration from the process environment. It reports every
// problem it finds rather than the first, so a misconfigured deployment needs
// one startup attempt to diagnose.
func Load(instanceIDFallback string) (Config, error) {
	var problems []error

	cfg := Config{
		PublicListenAddress:  os.Getenv(EnvPublicListen),
		PrivateListenAddress: os.Getenv(EnvPrivateListen),
		ControlEndpoint:      os.Getenv(EnvControlEndpoint),
		ControlCA:            os.Getenv(EnvControlCA),
		ControlCert:          os.Getenv(EnvControlCert),
		ControlKey:           os.Getenv(EnvControlKey),
		ValidationToken:      os.Getenv(EnvValidationToken),
		ReadDSN:              os.Getenv(EnvReadDSN),
		IdentityProfilePath:  os.Getenv(EnvIdentityProfilePath),
		Profile:              valueOrDefault(EnvProfile, ProfileStandard),
		Environment:          valueOrDefault(EnvEnvironment, defaultEnvironment),
		ServiceVersion:       valueOrDefault(EnvServiceVersion, defaultServiceVersion),
		ServiceInstanceID:    valueOrDefault(EnvServiceInstanceID, instanceIDFallback),
	}

	required := []struct {
		name  string
		value string
	}{
		{EnvPublicListen, cfg.PublicListenAddress},
		{EnvPrivateListen, cfg.PrivateListenAddress},
		{EnvControlEndpoint, cfg.ControlEndpoint},
		{EnvValidationToken, cfg.ValidationToken},
		{EnvReadDSN, cfg.ReadDSN},
	}
	for _, input := range required {
		if input.value == "" {
			problems = append(problems, fmt.Errorf("%s is required and has no default", input.name))
		}
	}

	if cfg.PublicListenAddress != "" && cfg.PublicListenAddress == cfg.PrivateListenAddress {
		problems = append(problems, fmt.Errorf(
			"%s and %s must differ so health endpoints stay off the public listener",
			EnvPublicListen, EnvPrivateListen))
	}

	timeout, err := controlCallTimeout()
	if err != nil {
		problems = append(problems, err)
	}
	cfg.ControlCallTimeout = timeout

	if cfg.Profile != ProfileStandard && cfg.Profile != ProfileControlledLocal {
		problems = append(problems, fmt.Errorf(
			"%s must be %q or %q", EnvProfile, ProfileStandard, ProfileControlledLocal))
	}

	if !identifierPattern.MatchString(cfg.Environment) || len(cfg.Environment) > 128 {
		problems = append(problems, fmt.Errorf("%s must be a values-v1 identifier", EnvEnvironment))
	}
	if cfg.ServiceVersion == "" || len(cfg.ServiceVersion) > 128 {
		problems = append(problems, fmt.Errorf("%s must be 1 to 128 characters", EnvServiceVersion))
	}
	if cfg.ServiceInstanceID == "" || len(cfg.ServiceInstanceID) > 128 {
		problems = append(problems, fmt.Errorf("%s must be 1 to 128 characters", EnvServiceInstanceID))
	}

	if len(problems) > 0 {
		return Config{}, errors.Join(problems...)
	}
	return cfg, nil
}

// ServesLocalChecks reports whether this process registers the local-check
// route. Only the resolved profile decides it.
func (c Config) ServesLocalChecks() bool { return c.Profile == ProfileControlledLocal }

func valueOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func controlCallTimeout() (time.Duration, error) {
	raw := os.Getenv(EnvControlCallTimeout)
	if raw == "" {
		return defaultControlCallTimeout, nil
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil {
		return defaultControlCallTimeout, fmt.Errorf("%s must be an integer number of seconds", EnvControlCallTimeout)
	}
	timeout := time.Duration(seconds) * time.Second
	if timeout <= 0 || timeout > maxControlCallTimeout {
		return defaultControlCallTimeout, fmt.Errorf(
			"%s must be between 1 and %d seconds", EnvControlCallTimeout, int(maxControlCallTimeout.Seconds()))
	}
	return timeout, nil
}
