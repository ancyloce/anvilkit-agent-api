// Package bootstrap assembles the API with Fx (A08). The validated
// configuration snapshot is the only source of addresses, bounds and
// modes; constructors receive the sections they need.
package bootstrap

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/fixtureauth"
	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/knowledge"
	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/mcp"
	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/oidcauth"
	"github.com/ancyloce/anvilkit-agent-api/internal/application"
	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	httptransport "github.com/ancyloce/anvilkit-agent-api/internal/transport/http"
)

// ServerOptions maps the configuration snapshot onto the transport's
// injected options.
func ServerOptions(cfg config.Config) httptransport.Options {
	return httptransport.Options{
		Listen: cfg.HTTP.Listen, ReadHeaderTimeout: cfg.HTTP.ReadHeaderTimeout, BodyLimitBytes: cfg.HTTP.BodyLimitBytes, TransferWindow: cfg.Artifacts.TransferWindow,
		PreparationProfile: cfg.Preparations.Profile,
		Stream: httptransport.StreamBounds{
			HeartbeatInterval: cfg.SSE.HeartbeatInterval, FrameBuffer: cfg.SSE.FrameBuffer,
			SlowConsumerGrace: cfg.SSE.SlowConsumerGrace, WriteTimeout: cfg.SSE.WriteTimeout,
			MaxStreams: cfg.SSE.MaxStreams, MaxStreamsPerPrincipal: cfg.SSE.MaxStreamsPerPrincipal, RetryBase: cfg.SSE.RetryBase,
		},
	}
}

func Module() fx.Option {
	return fx.Options(
		// The process's stop bound lies above the longest HTTP drain the
		// configuration allows (P1.7): Fx's default of 15s would end a
		// longer drain as a failed stop with a non-zero exit.
		fx.StopTimeout(config.MaxShutdownTimeout+30*time.Second),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: log} }),
		fx.Provide(
			config.Load,
			func() *slog.Logger {
				return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
			},
			newVerifier,
			newIdentities,
			func(cfg config.Config, ids *identities) (*control.Client, error) {
				transport, err := ids.client("control", cfg.Control.Identity, "anvilkit-agent-control")
				if err != nil {
					return nil, err
				}
				return control.Dial(cfg.Control.Address, transport)
			},
			func(c *control.Client) application.Control { return c },
			newGateways,
			newTracer,
			newMetrics,
			newReadiness,
			func(cfg config.Config, v application.Verifier, ctl application.Control, gw httptransport.Gateways, tracer trace.Tracer, metrics *httptransport.Metrics, ready *readiness, log *slog.Logger) (*httptransport.Server, error) {
				opts := ServerOptions(cfg)
				opts.Tracer, opts.Metrics, opts.Logger = tracer, metrics, log
				return httptransport.NewServer(opts, v, ctl, gw, ready.check)
			},
		),
		fx.Invoke(run),
	)
}

// newGateways dials the placed owners under their client identities and
// closes them on stop; an unplaced owner stays unavailable.
func newGateways(lc fx.Lifecycle, cfg config.Config, ids *identities, log *slog.Logger) (httptransport.Gateways, error) {
	var gw httptransport.Gateways
	if cfg.Knowledge.Address != "" {
		transport, err := ids.client("knowledge", cfg.Knowledge.Identity, "anvilkit-agent-knowledge")
		if err != nil {
			return gw, err
		}
		k, err := knowledge.Dial(cfg.Knowledge.Address, cfg.Knowledge.Timeout, transport)
		if err != nil {
			return gw, err
		}
		lc.Append(fx.Hook{OnStop: func(context.Context) error { return k.Close() }})
		gw.Knowledge = k
	} else {
		log.Warn("no knowledge placement: API-07/08/09/13/14 answer DEPENDENCY_UNAVAILABLE (knowledge.address unset)")
	}
	if cfg.MCP.Address != "" {
		transport, err := ids.client("mcp", cfg.MCP.Identity, "anvilkit-agent-mcp")
		if err != nil {
			return gw, err
		}
		m, err := mcp.Dial(cfg.MCP.Address, cfg.MCP.Timeout, transport)
		if err != nil {
			return gw, err
		}
		lc.Append(fx.Hook{OnStop: func(context.Context) error { return m.Close() }})
		gw.MCP = m
	} else {
		log.Warn("no mcp placement: API-10/11/15/16 answer DEPENDENCY_UNAVAILABLE (mcp.address unset)")
	}
	return gw, nil
}

// run binds the listener inside OnStart, so Fx reports a failed bind as a
// failed start, and turns a listener that stops on its own into a shutdown
// with a non-zero exit instead of a process that keeps running unreachable.
func run(lc fx.Lifecycle, sd fx.Shutdowner, cfg config.Config, srv *httptransport.Server, ctl *control.Client, ready *readiness, log *slog.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			addr, err := srv.Start()
			if err != nil {
				return err
			}
			go func() {
				if err := <-srv.Served(); err != nil {
					log.Error("api listener stopped", "listen", addr.String(), "error", err)
					_ = sd.Shutdown(fx.ExitCode(1))
				}
			}()
			log.Info("api serving", "listen", addr.String(), "control", cfg.Control.Address, "authMode", cfg.Auth.Mode)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			// Readiness goes first (P1.4): the endpoint leaves the Service before
			// the drain starts.
			ready.stop()
			shutdown, cancel := context.WithTimeout(ctx, cfg.HTTP.ShutdownTimeout)
			defer cancel()
			err := srv.Stop(shutdown)
			_ = ctl.Close()
			return err
		},
	})
}

// newVerifier is the identity protocol of the configuration (P0.3): the
// issuer's discovery runs once at startup and a refusal stops the process,
// so the API never serves without a verifier. fixture exists only behind
// development.enabled (the loader refuses it otherwise).
func newVerifier(cfg config.Config, log *slog.Logger) (application.Verifier, error) {
	if cfg.Auth.Mode == "fixture" {
		log.Warn("DEVELOPMENT_ONLY identity: bearer principals file; qualifies no production identity")
		return fixtureauth.Load(cfg.Auth.PrincipalsFile)
	}
	o := cfg.Auth.OIDC
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if o.CAFile != "" {
		pem, err := os.ReadFile(o.CAFile)
		if err != nil {
			return nil, fmt.Errorf("auth.oidc.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("auth.oidc.ca_file %s holds no certificate", o.CAFile)
		}
		transport.TLSClientConfig.RootCAs = pool
	}
	if strings.HasPrefix(o.Issuer, "http://") {
		log.Warn("DEVELOPMENT_ONLY identity: the OIDC issuer is reached over plaintext http", "issuer", o.Issuer)
	}
	ctx, cancel := context.WithTimeout(context.Background(), o.Timeout)
	defer cancel()
	v, err := oidcauth.New(ctx, oidcauth.Options{
		Issuer: o.Issuer, Audience: o.Audience, Algorithms: o.Algorithms, ClockSkew: o.ClockSkew,
		Claims:     oidcauth.Claims{Tenant: o.Claims.Tenant, Project: o.Claims.Project, Actor: o.Claims.Actor, Roles: o.Claims.Roles},
		HTTPClient: &http.Client{Timeout: o.Timeout, Transport: transport},
	})
	if err != nil {
		return nil, err
	}
	log.Info("identity verifier", "mode", "oidc", "issuer", o.Issuer, "audience", o.Audience, "algorithms", o.Algorithms)
	return v, nil
}
