package stationsvc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/station"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type maintenanceCancelStore struct {
	repository.Store
	cancel  context.CancelFunc
	updated bool
}

func (s *maintenanceCancelStore) WithinTx(ctx context.Context, operation func(repository.Store) error) error {
	return operation(s)
}

func (s *maintenanceCancelStore) GetStation(context.Context, string) (station.Station, error) {
	return station.Station{ID: "sta_cancel", Status: station.StatusActive, Version: 4}, nil
}

func (s *maintenanceCancelStore) ListWaveforms(context.Context, repository.WaveformFilter) (repository.Page[waveform.Batch], error) {
	s.cancel()
	return repository.Page[waveform.Batch]{}, nil
}

func (s *maintenanceCancelStore) UpdateStationState(ctx context.Context, id string, status station.Status, from, until *time.Time, version int64, now time.Time) (station.Station, error) {
	if err := ctx.Err(); err != nil {
		return station.Station{}, err
	}
	s.updated = true
	return station.Station{ID: id, Status: status, Version: version + 1, MaintenanceFrom: from, MaintenanceUntil: until, UpdatedAt: now}, nil
}

func (s *maintenanceCancelStore) CreateAudit(ctx context.Context, event audit.Event) error {
	return ctx.Err()
}

func TestCancelledMaintenanceRequestCannotWriteAfterOverlapCheck(t *testing.T) {
	now := time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC)
	ctx, cancel := context.WithCancel(context.Background())
	store := &maintenanceCancelStore{cancel: cancel}
	service := New(store, store, clock.NewFake(now), &idgen.Sequence{})
	principal := auth.Principal{UserID: "operator", Role: auth.RoleOperator}

	_, err := service.ScheduleMaintenance(ctx, principal, "sta_cancel", 4, now.Add(time.Hour), now.Add(2*time.Hour))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ScheduleMaintenance() error = %v; want context canceled", err)
	}
	if store.updated {
		t.Fatal("station state was updated after request cancellation")
	}
}
