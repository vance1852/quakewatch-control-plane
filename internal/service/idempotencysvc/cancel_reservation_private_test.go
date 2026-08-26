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

type cancellationReservationStore struct {
	repository.Store
	record      *repository.IdempotencyRecord
	deleteCalls int
}

func (s *cancellationReservationStore) GetIdempotency(context.Context, string, string, string, string) (repository.IdempotencyRecord, error) {
	if s.record == nil {
		return repository.IdempotencyRecord{}, fault.ErrNotFound
	}
	return *s.record, nil
}

func (s *cancellationReservationStore) CreateIdempotency(_ context.Context, value repository.IdempotencyRecord) error {
	if s.record != nil {
		return fault.ErrAlreadyExists
	}
	s.record = &value
	return nil
}

func (s *cancellationReservationStore) DeleteIdempotency(context.Context, string) error {
	s.deleteCalls++
	s.record = nil
	return nil
}

func (s *cancellationReservationStore) CompleteIdempotency(_ context.Context, _ string, code int, response string) error {
	s.record.ResponseCode = &code
	s.record.ResponseJSON = response
	return nil
}

func TestCanceledOperationReleasesReservationBeforeRetry(t *testing.T) {
	now := time.Date(2026, 8, 27, 9, 0, 0, 0, time.UTC)
	store := &cancellationReservationStore{}
	service := New(store, nil, clock.NewFake(now), &idgen.Sequence{}, time.Hour)
	principal := auth.Principal{UserID: "operator_1", Role: auth.RoleOperator}
	ctx, cancel := context.WithCancel(context.Background())
	disconnected := errors.New("client disconnected")
	_, err := service.Execute(ctx, principal, "POST", "/v1/waveforms", "retry-key", []byte(`{"source":"sensor"}`), func(context.Context) (int, any, error) {
		cancel()
		return 0, nil, disconnected
	})
	if !errors.Is(err, disconnected) {
		t.Fatalf("canceled operation error = %v; want original disconnect cause", err)
	}
	if store.deleteCalls != 1 || store.record != nil {
		t.Fatalf("reservation cleanup calls = %d, record present = %v; want one deletion and no record", store.deleteCalls, store.record != nil)
	}

	retryExecuted := false
	result, err := service.Execute(context.Background(), principal, "POST", "/v1/waveforms", "retry-key", []byte(`{"source":"sensor"}`), func(context.Context) (int, any, error) {
		retryExecuted = true
		return 202, map[string]string{"status": "accepted"}, nil
	})
	if err != nil || !retryExecuted || result.Code != 202 {
		t.Fatalf("retry result = %+v, executed = %v, error = %v; want a fresh accepted operation", result, retryExecuted, err)
	}
}
