package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type workerErrorStore struct {
	repository.Store
	message string
}

func (s *workerErrorStore) FailJob(_ context.Context, _, _ string, _ int64, _ bool, _ time.Time, message string) error {
	s.message = message
	return nil
}

type failingJobHandler struct{ err error }

func (h failingJobHandler) HandleJob(context.Context, job.Job) error { return h.err }

func TestWorkerFailureSummaryPreservesUTF8Boundary(t *testing.T) {
	now := time.Date(2026, 8, 27, 4, 0, 0, 0, time.UTC)
	store := &workerErrorStore{}
	runner := &Runner{
		store: store, clock: clock.NewFake(now), logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		jobs:   failingJobHandler{err: errors.New(strings.Repeat("震", 400))},
		config: Config{JobTimeout: time.Second}, owner: "worker-1",
	}
	runner.runJob(context.Background(), job.Job{ID: "job-1", Kind: job.KindProcessWaveform, AttemptCount: 1, MaxAttempts: 4, Version: 2})

	if len(store.message) > 1000 {
		t.Fatalf("stored failure length = %d; want at most 1000 bytes", len(store.message))
	}
	if !utf8.ValidString(store.message) {
		t.Fatalf("stored failure is not valid UTF-8: trailing bytes % x", []byte(store.message[len(store.message)-4:]))
	}
}
