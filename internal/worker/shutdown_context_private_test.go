package worker

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/storage/sqlite"
)

type cancellationProbe struct {
	started  chan struct{}
	canceled chan struct{}
}

func (p *cancellationProbe) HandleJob(ctx context.Context, _ job.Job) error {
	close(p.started)
	<-ctx.Done()
	close(p.canceled)
	return ctx.Err()
}

type idleDeliveryHandler struct{}

func (idleDeliveryHandler) Deliver(context.Context, alert.Delivery) error { return nil }

func TestWorkerShutdownCancelsLeasedProcessing(t *testing.T) {
	now := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	queued := job.Job{
		ID: "job-shutdown", Kind: job.KindCleanupSession, AggregateID: "expired-sessions",
		PayloadJSON: `{}`, Status: job.StatusPending, MaxAttempts: 6,
		NextAttemptAt: now, Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	if err := database.CreateJob(context.Background(), queued); err != nil {
		t.Fatalf("create job: %v", err)
	}
	probe := &cancellationProbe{started: make(chan struct{}), canceled: make(chan struct{})}
	runner, err := New(database, clock.NewFake(now), &idgen.Sequence{}, slog.New(slog.NewTextHandler(io.Discard, nil)), probe, idleDeliveryHandler{}, Config{
		PollInterval: time.Hour, LeaseDuration: time.Second, JobTimeout: 500 * time.Millisecond, BatchSize: 1,
	})
	if err != nil {
		t.Fatalf("new runner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-probe.started:
	case <-time.After(time.Second):
		t.Fatal("leased processing did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner shutdown error: %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("runner did not stop after cancellation")
	}
	select {
	case <-probe.canceled:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("worker returned while leased processing still lacked cancellation")
	}
}
