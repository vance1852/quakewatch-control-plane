package stationsvc

import (
	"context"
	"errors"
	"sync"
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

type maintenanceInterleavingStore struct {
	repository.Store
	mu          sync.Mutex
	value       station.Station
	waveforms   []waveform.Batch
	txEntered   chan struct{}
	allowTx     chan struct{}
	updateCalls int
}

func (s *maintenanceInterleavingStore) WithinTx(ctx context.Context, fn func(repository.Store) error) error {
	close(s.txEntered)
	select {
	case <-s.allowTx:
		return fn(s)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *maintenanceInterleavingStore) GetStation(context.Context, string) (station.Station, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value, nil
}

func (s *maintenanceInterleavingStore) ListWaveforms(context.Context, repository.WaveformFilter) (repository.Page[waveform.Batch], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := append([]waveform.Batch(nil), s.waveforms...)
	return repository.Page[waveform.Batch]{Items: items}, nil
}

func (s *maintenanceInterleavingStore) UpdateStationState(_ context.Context, _ string, next station.Status, from, until *time.Time, _ int64, now time.Time) (station.Station, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCalls++
	s.value.Status = next
	s.value.MaintenanceFrom = from
	s.value.MaintenanceUntil = until
	s.value.Version++
	s.value.UpdatedAt = now
	return s.value, nil
}

func (s *maintenanceInterleavingStore) CreateAudit(context.Context, audit.Event) error { return nil }

func (s *maintenanceInterleavingStore) addWaveform(value waveform.Batch) {
	s.mu.Lock()
	s.waveforms = append(s.waveforms, value)
	s.mu.Unlock()
}

func TestMaintenanceRejectsWaveformCommittedAtTransactionEntry(t *testing.T) {
	now := time.Date(2026, 8, 27, 8, 0, 0, 0, time.UTC)
	from := now.Add(time.Hour)
	until := from.Add(2 * time.Hour)
	store := &maintenanceInterleavingStore{
		value:     station.Station{ID: "sta_race", Code: "RACE1", Status: station.StatusActive, Version: 4},
		txEntered: make(chan struct{}), allowTx: make(chan struct{}),
	}
	service := New(store, store, clock.NewFake(now), &idgen.Sequence{})
	principal := auth.Principal{UserID: "operator_1", Role: auth.RoleOperator}
	result := make(chan error, 1)
	go func() {
		_, err := service.ScheduleMaintenance(context.Background(), principal, "sta_race", 4, from, until)
		result <- err
	}()

	<-store.txEntered
	store.addWaveform(waveform.Batch{ID: "wav_overlap", StationID: "sta_race", StartsAt: from.Add(15 * time.Minute), EndsAt: from.Add(45 * time.Minute)})
	close(store.allowTx)
	err := <-result
	if !errors.Is(err, fault.ErrConflict) {
		t.Fatalf("maintenance scheduling error = %v; want conflict for waveform committed at transaction entry", err)
	}
	if store.updateCalls != 0 {
		t.Fatalf("maintenance state updates = %d; want 0 after overlapping waveform commit", store.updateCalls)
	}
}
