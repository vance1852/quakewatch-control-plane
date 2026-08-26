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
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type readinessStore struct {
	repository.Store
	station     station.Station
	sensors     []station.Sensor
	updateCalls int
}

func (s *readinessStore) GetStation(context.Context, string) (station.Station, error) {
	return s.station, nil
}

func (s *readinessStore) ListSensors(context.Context, string, bool) ([]station.Sensor, error) {
	return append([]station.Sensor(nil), s.sensors...), nil
}

func (s *readinessStore) CountEnabledSensors(context.Context, string) (int, time.Time, error) {
	count := 0
	latest := time.Time{}
	for _, sensorValue := range s.sensors {
		if sensorValue.DisabledAt == nil {
			count++
			if sensorValue.CalibratedAt.After(latest) {
				latest = sensorValue.CalibratedAt
			}
		}
	}
	return count, latest, nil
}

func (s *readinessStore) UpdateStationState(_ context.Context, _ string, next station.Status, _ *time.Time, _ *time.Time, _ int64, now time.Time) (station.Station, error) {
	s.updateCalls++
	s.station.Status = next
	s.station.Version++
	s.station.UpdatedAt = now
	return s.station, nil
}

func (s *readinessStore) CreateAudit(context.Context, audit.Event) error { return nil }

type readinessTx struct{ store repository.Store }

func (t readinessTx) WithinTx(ctx context.Context, fn func(repository.Store) error) error {
	return fn(t.store)
}

func TestActivateRechecksSensorReadinessInsideStateTransaction(t *testing.T) {
	now := time.Date(2026, 8, 27, 1, 0, 0, 0, time.UTC)
	store := &readinessStore{
		station: station.Station{ID: "sta-1", Status: station.StatusProvisioning, Version: 1},
		sensors: []station.Sensor{{
			ID: "sen-1", StationID: "sta-1", CalibratedAt: now.AddDate(0, -1, 0), Version: 1,
		}},
	}
	service := New(store, readinessTx{store: store}, clock.NewFake(now), &idgen.Sequence{})

	if _, err := service.Get(context.Background(), "sta-1"); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	disabledAt := now.Add(-time.Minute)
	store.sensors[0].DisabledAt = &disabledAt

	_, err := service.Activate(context.Background(), auth.Principal{UserID: "usr-1", Role: auth.RoleAdmin}, "sta-1", 1)
	if !errors.Is(err, fault.ErrInvalidState) {
		t.Fatalf("Activate() error = %v; want invalid state after the only sensor is disabled", err)
	}
	if store.updateCalls != 0 {
		t.Fatalf("UpdateStationState() calls = %d; want 0", store.updateCalls)
	}
}
