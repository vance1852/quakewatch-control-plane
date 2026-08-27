package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type permanentErrorStore struct {
	repository.Store
	failed   bool
	terminal bool
	message  string
}

func (s *permanentErrorStore) FailJob(_ context.Context, _ string, _ string, _ int64, terminal bool, _ time.Time, message string) error {
	s.failed = true
	s.terminal = terminal
	s.message = message
	return nil
}

func TestMalformedWorkerPayloadStopsAfterFirstFailure(t *testing.T) {
	now := time.Date(2026, 8, 26, 15, 0, 0, 0, time.UTC)
	owner := "worker_permanent"
	leaseUntil := now.Add(time.Minute)
	store := &permanentErrorStore{}
	valueClock := clock.NewFake(now)
	runner := &Runner{
		store: store, clock: valueClock, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		jobs: NewHandlers(nil, store, valueClock), owner: owner,
		config: Config{JobTimeout: time.Second},
	}
	value := job.Job{
		ID: "job_bad_payload", Kind: job.KindProcessWaveform, AggregateID: "wav_1",
		PayloadJSON: "not-json", Status: job.StatusLeased, AttemptCount: 1, MaxAttempts: 6,
		LeaseOwner: &owner, LeaseUntil: &leaseUntil, Version: 2,
	}

	runner.runJob(context.Background(), value)
	if !store.failed {
		t.Fatal("malformed worker job did not record a failure")
	}
	if !store.terminal {
		t.Fatalf("malformed worker job terminal = %v; want true on first failure", store.terminal)
	}
	if store.message == "" {
		t.Fatal("malformed worker job lost its decoding error")
	}
}
