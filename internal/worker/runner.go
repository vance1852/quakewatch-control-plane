package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type JobHandler interface {
	HandleJob(context.Context, job.Job) error
}

type DeliveryHandler interface {
	Deliver(context.Context, alert.Delivery) error
}

type PermanentError interface {
	Permanent() bool
}

type Config struct {
	PollInterval  time.Duration
	LeaseDuration time.Duration
	JobTimeout    time.Duration
	BatchSize     int
}

type Runner struct {
	store      repository.Store
	clock      clock.Clock
	ids        idgen.Generator
	logger     *slog.Logger
	jobs       JobHandler
	deliveries DeliveryHandler
	config     Config
	owner      string
}

func New(store repository.Store, valueClock clock.Clock, ids idgen.Generator, logger *slog.Logger, jobs JobHandler, deliveries DeliveryHandler, config Config) (*Runner, error) {
	if store == nil || valueClock == nil || ids == nil || logger == nil || jobs == nil || deliveries == nil {
		return nil, errors.New("worker dependencies cannot be nil")
	}
	if config.PollInterval <= 0 || config.LeaseDuration <= 0 || config.JobTimeout <= 0 {
		return nil, errors.New("worker durations must be positive")
	}
	if config.LeaseDuration <= config.JobTimeout {
		return nil, errors.New("worker lease duration must exceed job timeout")
	}
	if config.BatchSize <= 0 {
		config.BatchSize = 8
	}
	if config.BatchSize > 64 {
		config.BatchSize = 64
	}
	return &Runner{
		store:      store,
		clock:      valueClock,
		ids:        ids,
		logger:     logger,
		jobs:       jobs,
		deliveries: deliveries,
		config:     config,
		owner:      ids.New("worker"),
	}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	now := r.clock.Now()
	recovered, err := r.store.RecoverExpiredJobs(ctx, now)
	if err != nil {
		return fmt.Errorf("recover expired jobs at startup: %w", err)
	}
	r.logger.Info("worker started", "owner", r.owner, "recovered_jobs", recovered)
	ticker := time.NewTicker(r.config.PollInterval)
	defer ticker.Stop()
	for {
		if err := r.cycle(ctx); err != nil && !errors.Is(err, context.Canceled) {
			r.logger.Error("worker cycle failed", "error", err)
		}
		select {
		case <-ctx.Done():
			r.logger.Info("worker stopping", "owner", r.owner, "reason", ctx.Err())
			return nil
		case <-ticker.C:
		}
	}
}

func (r *Runner) cycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := r.clock.Now()
	leaseUntil := now.Add(r.config.LeaseDuration)
	jobs, err := r.store.LeaseJobs(ctx, r.owner, now, leaseUntil, r.config.BatchSize)
	if err != nil {
		return fmt.Errorf("lease jobs: %w", err)
	}
	deliveries, err := r.store.LeaseDeliveries(ctx, r.owner, now, leaseUntil, r.config.BatchSize)
	if err != nil {
		return fmt.Errorf("lease deliveries: %w", err)
	}
	var wg sync.WaitGroup
	for _, value := range jobs {
		value := value
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runJob(ctx, value)
		}()
	}
	for _, value := range deliveries {
		value := value
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.runDelivery(ctx, value)
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (r *Runner) runJob(parent context.Context, value job.Job) {
	ctx, cancel := context.WithTimeout(parent, r.config.JobTimeout)
	defer cancel()
	started := r.clock.Now()
	err := r.jobs.HandleJob(ctx, value)
	now := r.clock.Now()
	if err == nil {
		if completeErr := r.store.CompleteJob(parent, value.ID, r.owner, value.Version, now); completeErr != nil {
			r.logger.Error("complete worker job", "job_id", value.ID, "kind", value.Kind, "error", completeErr)
			return
		}
		r.logger.Info("worker job completed", "job_id", value.ID, "kind", value.Kind, "duration", now.Sub(started))
		return
	}
	terminal := value.AttemptCount >= value.MaxAttempts || isPermanent(err)
	retryAt := job.RetryAt(now, value.AttemptCount)
	if failErr := r.store.FailJob(parent, value.ID, r.owner, value.Version, terminal, retryAt, truncate(err.Error(), 1000)); failErr != nil {
		r.logger.Error("record worker job failure", "job_id", value.ID, "error", failErr, "original_error", err)
		return
	}
	r.logger.Warn("worker job failed", "job_id", value.ID, "kind", value.Kind, "attempt", value.AttemptCount, "terminal", terminal, "error", err)
}

func (r *Runner) runDelivery(parent context.Context, value alert.Delivery) {
	ctx, cancel := context.WithTimeout(parent, r.config.JobTimeout)
	defer cancel()
	started := r.clock.Now()
	err := r.deliveries.Deliver(ctx, value)
	now := r.clock.Now()
	if err == nil {
		if completeErr := r.store.CompleteDelivery(parent, value.ID, r.owner, value.Version, now); completeErr != nil {
			r.logger.Error("complete alert delivery", "delivery_id", value.ID, "error", completeErr)
			return
		}
		r.logger.Info("alert delivered", "delivery_id", value.ID, "duration", now.Sub(started))
		return
	}
	plan := alert.PlanFailure(value.AttemptCount, isPermanent(err), now)
	if failErr := r.store.FailDelivery(parent, value.ID, r.owner, value.Version, plan.Terminal, plan.RetryAt, truncate(err.Error(), 1000)); failErr != nil {
		r.logger.Error("record alert delivery failure", "delivery_id", value.ID, "error", failErr, "original_error", err)
		return
	}
	r.logger.Warn("alert delivery failed", "delivery_id", value.ID, "attempt", value.AttemptCount, "terminal", plan.Terminal, "error", err)
}

func isPermanent(err error) bool {
	var value PermanentError
	return errors.As(err, &value) && value.Permanent()
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
