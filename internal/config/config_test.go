package config

import (
	"strings"
	"testing"
	"time"
)

func setMinimum(t *testing.T) {
	t.Helper()
	t.Setenv(EnvPublicListen, "127.0.0.1:8443")
	t.Setenv(EnvPrivateListen, "127.0.0.1:8444")
	t.Setenv(EnvControlEndpoint, "https://control.internal:8443")
	t.Setenv(EnvValidationToken, "controlled-private-validation-token-0001")
	t.Setenv(EnvReadDSN, "postgres://anvilkit_api_ro@127.0.0.1:5432/anvilkit")
}

func TestLoadRequiresEveryDeploymentAddress(t *testing.T) {
	t.Setenv(EnvPublicListen, "")
	t.Setenv(EnvPrivateListen, "")
	t.Setenv(EnvControlEndpoint, "")
	t.Setenv(EnvReadDSN, "")

	_, err := Load("instance")
	if err == nil {
		t.Fatal("expected the missing addresses to be reported")
	}
	for _, name := range []string{EnvPublicListen, EnvPrivateListen, EnvControlEndpoint, EnvReadDSN} {
		if !strings.Contains(err.Error(), name) {
			t.Fatalf("expected %s to be named in the startup error", name)
		}
	}
}

func TestLoadAppliesDocumentedDefaults(t *testing.T) {
	setMinimum(t)
	cfg, err := Load("fallback-instance")
	if err != nil {
		t.Fatalf("expected the minimum configuration to load, got %v", err)
	}
	if cfg.ControlCallTimeout != defaultControlCallTimeout {
		t.Fatalf("expected the default Control timeout, got %v", cfg.ControlCallTimeout)
	}
	if cfg.Environment != defaultEnvironment || cfg.ServiceVersion != defaultServiceVersion {
		t.Fatalf("expected the documented telemetry defaults, got %+v", cfg)
	}
	if cfg.ServiceInstanceID != "fallback-instance" {
		t.Fatalf("expected the supplied instance fallback, got %q", cfg.ServiceInstanceID)
	}
	if cfg.IdentityProfilePath != "" {
		t.Fatal("no identity profile is configured by default")
	}
}

func TestLoadRejectsAnUnusableControlTimeout(t *testing.T) {
	for _, value := range []string{"not-a-number", "0", "-1", "3600"} {
		t.Run(value, func(t *testing.T) {
			setMinimum(t)
			t.Setenv(EnvControlCallTimeout, value)
			if _, err := Load("instance"); err == nil {
				t.Fatalf("expected %q to be refused", value)
			}
		})
	}
}

func TestLoadAcceptsABoundedControlTimeout(t *testing.T) {
	setMinimum(t)
	t.Setenv(EnvControlCallTimeout, "30")
	cfg, err := Load("instance")
	if err != nil {
		t.Fatalf("expected a bounded timeout to load, got %v", err)
	}
	if cfg.ControlCallTimeout != 30*time.Second {
		t.Fatalf("expected 30s, got %v", cfg.ControlCallTimeout)
	}
}

func TestLoadKeepsHealthOffThePublicListener(t *testing.T) {
	setMinimum(t)
	t.Setenv(EnvPrivateListen, "127.0.0.1:8443")
	if _, err := Load("instance"); err == nil {
		t.Fatal("expected a shared listener address to be refused")
	}
}

func TestLoadValidatesTelemetryIdentity(t *testing.T) {
	setMinimum(t)
	t.Setenv(EnvEnvironment, "not a valid id")
	if _, err := Load("instance"); err == nil {
		t.Fatal("expected an invalid environment identifier to be refused")
	}
}

func TestContractLimitsMatchThePilotProfile(t *testing.T) {
	// These mirror contracts/profiles/pilot-limits-v1.json. A drift here would
	// silently widen or narrow a contract ceiling.
	if RequestBodyMaxBytes != 65536 {
		t.Fatalf("ingress.commandBodyMaxBytes is 65536, got %d", RequestBodyMaxBytes)
	}
	if ValidationMaxIssues != 100 {
		t.Fatalf("definition.validationMaxIssues is 100, got %d", ValidationMaxIssues)
	}
	if DrainGrace != 30*time.Second {
		t.Fatalf("api.drainGraceSeconds is 30, got %v", DrainGrace)
	}
}

// TestServingProfileIsOptInAndClosed proves that the local surface is never
// reached by accident: the default profile does not serve it, an unrecognized
// value refuses to start rather than falling back, and only the explicit
// controlled-local profile turns it on.
func TestServingProfileIsOptInAndClosed(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		setMinimum(t)
		cfg, err := Load("instance")
		if err != nil {
			t.Fatalf("expected the minimum configuration to load, got %v", err)
		}
		if cfg.Profile != ProfileStandard || cfg.ServesLocalChecks() {
			t.Fatalf("the default profile serves local checks: %+v", cfg.Profile)
		}
	})

	t.Run("controlled local", func(t *testing.T) {
		setMinimum(t)
		t.Setenv(EnvProfile, ProfileControlledLocal)
		cfg, err := Load("instance")
		if err != nil {
			t.Fatalf("expected the controlled local profile to load, got %v", err)
		}
		if !cfg.ServesLocalChecks() {
			t.Fatal("the controlled local profile does not serve local checks")
		}
	})

	for _, value := range []string{"local", "production", "CONTROLLED-LOCAL", "controlled local", "true"} {
		t.Run("refused "+value, func(t *testing.T) {
			setMinimum(t)
			t.Setenv(EnvProfile, value)
			if _, err := Load("instance"); err == nil {
				t.Fatalf("expected the profile %q to be refused", value)
			}
		})
	}

	// The telemetry environment must not double as a capability switch.
	t.Run("environment does not enable the route", func(t *testing.T) {
		setMinimum(t)
		t.Setenv(EnvEnvironment, "local")
		cfg, err := Load("instance")
		if err != nil {
			t.Fatalf("expected the configuration to load, got %v", err)
		}
		if cfg.ServesLocalChecks() {
			t.Fatal("a local telemetry environment enabled the local-check route")
		}
	})
}
