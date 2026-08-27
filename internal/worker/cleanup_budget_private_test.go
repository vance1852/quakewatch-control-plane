package worker

import (
	"context"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type cleanupBudgetStore struct {
	repository.Store
	idempotencyCalls int
	idempotencyLimit int
}

func (s *cleanupBudgetStore) DeleteExpiredSessions(context.Context, time.Time, int) (int64, error) {
	return 3, nil
}

func (s *cleanupBudgetStore) DeleteExpiredIdempotency(_ context.Context, _ time.Time, limit int) (int64, error) {
	s.idempotencyCalls++
	s.idempotencyLimit = limit
	return 2, nil
}

func TestCleanupLimitAppliesIndependentlyToEachEntity(t *testing.T) {
	now := time.Date(2026, 8, 27, 4, 0, 0, 0, time.UTC)
	owner := "worker_cleanup"
	leaseUntil := now.Add(time.Minute)
	store := &cleanupBudgetStore{}
	handler := NewHandlers(nil, store, clock.NewFake(now))
	value := job.Job{ID: "job_cleanup", Kind: job.KindCleanupSession, AggregateID: "2026-08-27", PayloadJSON: `{"limit":3}`, Status: job.StatusLeased, LeaseOwner: &owner, LeaseUntil: &leaseUntil, Version: 2}

	if err := handler.HandleJob(context.Background(), value); err != nil {
		t.Fatalf("HandleJob() error = %v", err)
	}
	if store.idempotencyCalls != 1 {
		t.Fatalf("idempotency cleanup calls = %d, want 1 after sessions fill their own limit", store.idempotencyCalls)
	}
	if store.idempotencyLimit != 3 {
		t.Fatalf("idempotency cleanup limit = %d, want independent payload limit 3", store.idempotencyLimit)
	}
}
