// Command anvilkit-agent-api serves the public AnvilKit Agent HTTP boundary.
//
// This stage serves definition validation, the authorized operation read
// surface (status, snapshot and durable events), the reserved cancellation lane
// and, in the controlled local profile only, local-check intake. The process
// holds a restricted projection read role only, and no Agent write role,
// Temporal client, provider credential or Pagix connection; every business
// decision, including whether a caller may see an operation at all, belongs to
// anvilkit-agent-control.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ancyloce/anvilkit-agent-api/internal/config"
	"github.com/ancyloce/anvilkit-agent-api/internal/httpapi"
	"github.com/ancyloce/anvilkit-agent-api/internal/identity"
	"github.com/ancyloce/anvilkit-agent-api/internal/logging"
	"github.com/ancyloce/anvilkit-agent-api/internal/readmodel"
)

func main() {
	if err := run(); err != nil {
		// Startup problems precede the configured logger, so they are reported
		// on stderr in plain text rather than as a contract log record.
		fmt.Fprintf(os.Stderr, "anvilkit-agent-api: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(defaultInstanceID())
	if err != nil {
		return err
	}

	logger := logging.New(os.Stdout, logging.Identity{
		ServiceVersion:    cfg.ServiceVersion,
		ServiceInstanceID: cfg.ServiceInstanceID,
		Environment:       cfg.Environment,
	}, slog.LevelInfo)

	profile, err := loadIdentityProfile(cfg, logger)
	if err != nil {
		return err
	}

	control, err := httpapi.NewControlClient(httpapi.ControlClientConfig{
		Endpoint: cfg.ControlEndpoint, DialTimeout: cfg.ControlCallTimeout,
		CAFile: cfg.ControlCA, CertificateFile: cfg.ControlCert, KeyFile: cfg.ControlKey,
		ValidationToken: cfg.ValidationToken,
	})
	if err != nil {
		return err
	}

	// The process context bounds the background work that outlives one request:
	// the shared catch-up, the database listener and the readiness probe.
	background, stopBackground := context.WithCancel(context.Background())
	defer stopBackground()

	reader, err := readmodel.Open(background, cfg.ReadDSN)
	if err != nil {
		return err
	}
	defer reader.Close()

	server := httpapi.NewServer(httpapi.Dependencies{
		Logger:             logger,
		Identities:         profile,
		Validator:          control.Validation,
		Disclosure:         control.Disclosure,
		Commands:           control.Commands,
		ReadModel:          reader,
		ServesLocalChecks:  cfg.ServesLocalChecks(),
		ControlCallTimeout: cfg.ControlCallTimeout,
	})

	hub := server.Hub()
	go hub.Run(background)
	go server.WatchReadRole(background)

	// The listener only wakes the shared catch-up sooner. Losing it costs a
	// second of latency, never an event, so it needs no supervision beyond its
	// own reconnect.
	hints := make(chan struct{}, 1)
	go reader.Listen(background, hints)
	go func() {
		for {
			select {
			case <-background.Done():
				return
			case <-hints:
				hub.Wake()
			}
		}
	}()

	public := &http.Server{
		Addr:              cfg.PublicListenAddress,
		Handler:           server.PublicHandler(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	private := &http.Server{
		Addr:              cfg.PrivateListenAddress,
		Handler:           server.PrivateHandler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	logger.LogAttrs(context.Background(), slog.LevelInfo, "",
		slog.String("eventName", logging.EventConfigLoaded),
		slog.Group("attributes",
			slog.Int("identityCount", profile.Size()),
			slog.String("servingProfile", cfg.Profile),
		),
	)

	failures := make(chan error, 2)
	go serve(public, "public", failures)
	go serve(private, "private", failures)

	logger.LogAttrs(context.Background(), slog.LevelInfo, "",
		slog.String("eventName", logging.EventServiceStarted),
	)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-failures:
		return err
	case <-signals:
	}

	logger.LogAttrs(context.Background(), slog.LevelInfo, "",
		slog.String("eventName", logging.EventServiceStopping),
	)

	// Readiness turns false before shutdown starts, so a load balancer stops
	// sending new work while in-flight requests finish inside the grace window.
	// Live subscriptions see the same flag and close with a randomized
	// reconnect hint rather than being cut mid-frame.
	server.BeginDrain()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), config.DrainGrace)
	defer cancel()

	publicErr := public.Shutdown(shutdownCtx)
	privateErr := private.Shutdown(shutdownCtx)
	stopBackground()

	logger.LogAttrs(context.Background(), slog.LevelInfo, "",
		slog.String("eventName", logging.EventServiceDrained),
	)
	return errors.Join(publicErr, privateErr)
}

// loadIdentityProfile resolves the controlled identity mapping. An unconfigured
// profile is allowed to start but authorizes nothing, so a deployment that
// forgets it fails closed on every protected route instead of running open.
func loadIdentityProfile(cfg config.Config, logger *slog.Logger) (*identity.Profile, error) {
	if cfg.IdentityProfilePath == "" {
		logger.LogAttrs(context.Background(), slog.LevelWarn,
			"no controlled identity profile is configured; every protected route will deny",
			slog.String("eventName", logging.EventConfigLoaded),
		)
		return identity.EmptyProfile(), nil
	}
	return identity.LoadProfile(cfg.IdentityProfilePath)
}

func serve(server *http.Server, name string, failures chan<- error) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		failures <- fmt.Errorf("the %s listener stopped: %w", name, err)
	}
}

// defaultInstanceID gives the log contract a required service.instance.id when
// the deployment supplies none.
func defaultInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "unidentified-instance"
	}
	return host
}
