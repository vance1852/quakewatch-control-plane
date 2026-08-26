package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type retryClockStore struct {
	repository.Store
	retryAt  time.Time
	terminal bool
}

func (s *retryClockStore) FailDelivery(_ context.Context, _, _ string, _ int64, terminal bool, retryAt time.Time, _ string) error {
	s.retryAt = retryAt
	s.terminal = terminal
	return nil
}

type failingDeliveryHandler struct{}

func (failingDeliveryHandler) Deliver(context.Context, alert.Delivery) error {
	return errors.New("temporary webhook failure")
}

func TestDeliveryRetryUsesWorkerClockAsBackoffBase(t *testing.T) {
	now := time.Date(2031, 4, 5, 6, 7, 8, 0, time.UTC)
	store := &retryClockStore{}
	runner := &Runner{
		store: store, clock: clock.NewFake(now), logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		deliveries: failingDeliveryHandler{}, config: Config{JobTimeout: time.Second}, owner: "worker-1",
	}
	runner.runDelivery(context.Background(), alert.Delivery{ID: "delivery-1", AttemptCount: 2, Version: 3})

	want := now.Add(alert.Backoff(2))
	if !store.retryAt.Equal(want) {
		t.Fatalf("retry time = %s; want worker clock based %s", store.retryAt, want)
	}
	if store.terminal {
		t.Fatal("temporary second attempt was marked terminal")
	}
}
