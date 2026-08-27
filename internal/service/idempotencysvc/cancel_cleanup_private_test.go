package idempotencysvc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type canceledCleanupStore struct {
	repository.Store
	deleteCalled bool
	deleteCtxErr error
}

func (s *canceledCleanupStore) GetIdempotency(context.Context, string, string, string, string) (repository.IdempotencyRecord, error) {
	return repository.IdempotencyRecord{}, fault.ErrNotFound
}

func (s *canceledCleanupStore) CreateIdempotency(context.Context, repository.IdempotencyRecord) error {
	return nil
}

func (s *canceledCleanupStore) DeleteIdempotency(ctx context.Context, _ string) error {
	s.deleteCalled = true
	s.deleteCtxErr = ctx.Err()
	return ctx.Err()
}

func TestCanceledOperationUsesLiveIdempotencyCleanupContext(t *testing.T) {
	store := &canceledCleanupStore{}
	service := New(store, nil, clock.NewFake(time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)), &idgen.Sequence{}, time.Hour)
	ctx, cancel := context.WithCancel(context.Background())
	operationErr := errors.New("waveform persistence interrupted")

	_, err := service.Execute(ctx, auth.Principal{UserID: "operator-1"}, "POST", "/v1/waveforms", "retry-key", []byte(`{"source":"sensor"}`), func(context.Context) (int, any, error) {
		cancel()
		return 0, nil, operationErr
	})

	if !errors.Is(err, operationErr) {
		t.Fatalf("Execute() error = %v; want operation error", err)
	}
	if !store.deleteCalled {
		t.Fatal("DeleteIdempotency() was not called")
	}
	if store.deleteCtxErr != nil {
		t.Fatalf("DeleteIdempotency() context error = %v; want live cleanup context", store.deleteCtxErr)
	}
}
