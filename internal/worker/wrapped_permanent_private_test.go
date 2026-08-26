package worker

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type wrappedPermanentStore struct {
	repository.Store
	called   bool
	terminal bool
}

func (s *wrappedPermanentStore) FailJob(_ context.Context, _, _ string, _ int64, terminal bool, _ time.Time, _ string) error {
	s.called = true
	s.terminal = terminal
	return nil
}

type privateJobHandler func(context.Context, job.Job) error

func (f privateJobHandler) HandleJob(ctx context.Context, value job.Job) error {
	return f(ctx, value)
}

type permanentWorkError struct{}

func (permanentWorkError) Error() string   { return "payload is permanently rejected" }
func (permanentWorkError) Permanent() bool { return true }

func TestWrappedPermanentJobFailureIsTerminal(t *testing.T) {
	now := time.Date(2026, 8, 27, 8, 0, 0, 0, time.UTC)
	store := &wrappedPermanentStore{}
	wrapped := fmt.Errorf("provider rejected payload: %w", permanentWorkError{})
	runner := &Runner{
		store:  store,
		clock:  clock.NewFake(now),
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		jobs: privateJobHandler(func(context.Context, job.Job) error {
			return wrapped
		}),
		config: Config{JobTimeout: time.Second},
		owner:  "worker-private",
	}

	runner.runJob(context.Background(), job.Job{
		ID: "job-private", Kind: job.KindProcessWaveform,
		AttemptCount: 1, MaxAttempts: 5, Version: 7,
	})

	if !store.called {
		t.Fatal("FailJob() was not called")
	}
	if !store.terminal {
		t.Fatalf("FailJob() terminal = %v; want true for wrapped permanent error", store.terminal)
	}
}
