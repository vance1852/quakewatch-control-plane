package stationsvc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/station"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type maintenanceOverlapStore struct {
	repository.Store
	station     station.Station
	waveform    waveform.Batch
	wantFrom    time.Time
	wantUntil   time.Time
	updateCalls int
}

func (s *maintenanceOverlapStore) GetStation(context.Context, string) (station.Station, error) {
	return s.station, nil
}

func (s *maintenanceOverlapStore) ListWaveforms(_ context.Context, filter repository.WaveformFilter) (repository.Page[waveform.Batch], error) {
	if filter.From != nil && filter.Until != nil && filter.From.Equal(s.wantFrom) && filter.Until.Equal(s.wantUntil) {
		return repository.Page[waveform.Batch]{Items: []waveform.Batch{s.waveform}}, nil
	}
	return repository.Page[waveform.Batch]{}, nil
}

func (s *maintenanceOverlapStore) UpdateStationState(_ context.Context, _ string, status station.Status, from, until *time.Time, _ int64, now time.Time) (station.Station, error) {
	s.updateCalls++
	s.station.Status = status
	s.station.MaintenanceFrom = from
	s.station.MaintenanceUntil = until
	s.station.Version++
	s.station.UpdatedAt = now
	return s.station, nil
}

func (s *maintenanceOverlapStore) CreateAudit(context.Context, audit.Event) error { return nil }

type maintenanceOverlapTx struct{ store repository.Store }

func (t maintenanceOverlapTx) WithinTx(ctx context.Context, fn func(repository.Store) error) error {
	return fn(t.store)
}

func TestMaintenanceRejectsWaveformInsideRequestedWindow(t *testing.T) {
	now := time.Date(2026, 8, 27, 6, 0, 0, 0, time.UTC)
	from := now.Add(time.Hour)
	until := from.Add(2 * time.Hour)
	store := &maintenanceOverlapStore{
		station:  station.Station{ID: "sta-1", Status: station.StatusActive, Version: 4},
		waveform: waveform.Batch{ID: "wav-1", StationID: "sta-1", StartsAt: from.Add(10 * time.Minute), EndsAt: from.Add(20 * time.Minute)},
		wantFrom: from, wantUntil: until,
	}
	service := New(store, maintenanceOverlapTx{store: store}, clock.NewFake(now), &idgen.Sequence{})

	_, err := service.ScheduleMaintenance(context.Background(), auth.Principal{UserID: "operator-1", Role: auth.RoleOperator}, "sta-1", 4, from, until)
	if !errors.Is(err, fault.ErrConflict) {
		t.Fatalf("ScheduleMaintenance() error = %v; want conflict for overlapping waveform", err)
	}
	if store.updateCalls != 0 {
		t.Fatalf("UpdateStationState() calls = %d; want 0 when waveform overlaps", store.updateCalls)
	}
}
