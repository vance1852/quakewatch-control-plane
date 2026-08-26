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

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/config"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/httpapi"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/service/alertsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/auditsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/authsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/eventsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/idempotencysvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/stationsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/waveformsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/storage/sqlite"
	"github.com/vance1852/quakewatch-control-plane/internal/worker"
)

func main() {
	if err := run(); err != nil {
		slog.Error("quakewatch stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	settings, err := config.Load()
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: settings.LogLevel}))
	slog.SetDefault(logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	database, err := sqlite.Open(ctx, settings.DatabasePath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer database.Close()
	valueClock := clock.Real{}
	ids := idgen.Random{}
	authService := authsvc.New(database, database, valueClock, ids, settings.SessionTTL)
	stationService := stationsvc.New(database, database, valueClock, ids)
	waveformService := waveformsvc.New(database, database, valueClock, ids)
	eventService := eventsvc.New(database, database, valueClock, ids, settings.ReviewLease)
	alertService := alertsvc.New(database, database, valueClock, ids, alertsvc.NewHTTPSender(nil))
	auditService := auditsvc.New(database)
	idempotencyService := idempotencysvc.New(database, database, valueClock, ids, 24*time.Hour)
	if settings.BootstrapEmail != "" {
		user, err := authService.Bootstrap(ctx, settings.BootstrapEmail, settings.BootstrapPassword)
		if err != nil {
			return fmt.Errorf("bootstrap administrator: %w", err)
		}
		logger.Info("bootstrap administrator ready", "user_id", user.ID, "email", user.Email)
	}
	cleanup := worker.CleanupJob(ids, valueClock.Now(), 500)
	if err := database.CreateJob(ctx, cleanup); err != nil && !errors.Is(err, fault.ErrAlreadyExists) {
		return fmt.Errorf("schedule cleanup job: %w", err)
	}
	handlers := worker.NewHandlers(waveformService, database, valueClock)
	runner, err := worker.New(database, valueClock, ids, logger, handlers, alertService, worker.Config{
		PollInterval:  settings.WorkerPoll,
		LeaseDuration: settings.WorkerLease,
		JobTimeout:    settings.WorkerJobTimeout,
		BatchSize:     8,
	})
	if err != nil {
		return fmt.Errorf("configure worker: %w", err)
	}
	api := httpapi.New(httpapi.Services{
		Auth:        authService,
		Stations:    stationService,
		Waveforms:   waveformService,
		Events:      eventService,
		Alerts:      alertService,
		Audit:       auditService,
		Idempotency: idempotencyService,
	}, database, logger, ids, settings.MaxRequestBytes)
	server := &http.Server{
		Addr:              settings.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	workerDone := make(chan error, 1)
	go func() { workerDone <- runner.Run(ctx) }()
	serverDone := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "address", settings.HTTPAddr)
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serverDone <- err
	}()
	select {
	case err := <-workerDone:
		if err != nil {
			stop()
			return fmt.Errorf("worker stopped unexpectedly: %w", err)
		}
	case err := <-serverDone:
		if err != nil {
			stop()
			return fmt.Errorf("http server stopped: %w", err)
		}
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), settings.ShutdownTimeout)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown http server: %w", err)
	}
	select {
	case err := <-workerDone:
		if err != nil {
			return fmt.Errorf("stop worker: %w", err)
		}
	case <-shutdownCtx.Done():
		return fmt.Errorf("wait for worker shutdown: %w", shutdownCtx.Err())
	}
	logger.Info("quakewatch shutdown complete")
	return nil
}
