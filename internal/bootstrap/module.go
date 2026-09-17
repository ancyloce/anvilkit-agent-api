// Package bootstrap assembles the API with Fx (A08). The validated
// configuration snapshot is the only source of addresses, bounds and
// modes; constructors receive the sections they need.
package bootstrap

import (
	"context"
	"log/slog"
	"os"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/control"
	"github.com/ancyloce/anvilkit-agent-api/internal/adapters/fixtureauth"
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
		},
	}
}

func Module() fx.Option {
	return fx.Options(
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger { return &fxevent.SlogLogger{Logger: log} }),
		fx.Provide(
			config.Load,
			func() *slog.Logger {
				return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
			},
			func(cfg config.Config) (application.Verifier, error) {
				return fixtureauth.Load(cfg.Auth.PrincipalsFile)
			},
			func(cfg config.Config) (*control.Client, error) { return control.Dial(cfg.Control.Address) },
			func(c *control.Client) application.Control { return c },
			func(cfg config.Config, v application.Verifier, ctl application.Control) (*httptransport.Server, error) {
				return httptransport.NewServer(ServerOptions(cfg), v, ctl, func(context.Context) error { return nil })
			},
		),
		fx.Invoke(run),
	)
}

// run binds the listener inside OnStart, so Fx reports a failed bind as a
// failed start, and turns a listener that stops on its own into a shutdown
// with a non-zero exit instead of a process that keeps running unreachable.
func run(lc fx.Lifecycle, sd fx.Shutdowner, cfg config.Config, srv *httptransport.Server, ctl *control.Client, log *slog.Logger) {
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
			shutdown, cancel := context.WithTimeout(ctx, cfg.HTTP.ShutdownTimeout)
			defer cancel()
			err := srv.Stop(shutdown)
			_ = ctl.Close()
			return err
		},
	})
}
