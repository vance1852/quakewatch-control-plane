package eventsvc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type waveformSnapshotStore struct {
	repository.Store
	mu        sync.Mutex
	batches   map[string]waveform.Batch
	events    int
	picks     int
	txEntered chan struct{}
	allowTx   chan struct{}
}

func (s *waveformSnapshotStore) WithinTx(ctx context.Context, fn func(repository.Store) error) error {
	close(s.txEntered)
	select {
	case <-s.allowTx:
	case <-ctx.Done():
		return ctx.Err()
	}
	s.mu.Lock()
	eventsBefore, picksBefore := s.events, s.picks
	s.mu.Unlock()
	err := fn(s)
	if err != nil {
		s.mu.Lock()
		s.events, s.picks = eventsBefore, picksBefore
		s.mu.Unlock()
	}
	return err
}

func (s *waveformSnapshotStore) GetWaveform(_ context.Context, id string) (waveform.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.batches[id], nil
}

func (s *waveformSnapshotStore) CreateEvent(context.Context, event.Candidate) error {
	s.mu.Lock()
	s.events++
	s.mu.Unlock()
	return nil
}

func (s *waveformSnapshotStore) CreatePick(context.Context, event.Pick) error {
	s.mu.Lock()
	s.picks++
	s.mu.Unlock()
	return nil
}

func (s *waveformSnapshotStore) CreateAudit(context.Context, audit.Event) error { return nil }

func (s *waveformSnapshotStore) reject(id string) {
	s.mu.Lock()
	batch := s.batches[id]
	batch.Status = waveform.StatusRejected
	s.batches[id] = batch
	s.mu.Unlock()
}

func TestDetectionRejectsWaveformChangedAtTransactionEntry(t *testing.T) {
	now := time.Date(2026, 8, 27, 10, 0, 0, 0, time.UTC)
	store := &waveformSnapshotStore{
		batches: map[string]waveform.Batch{
			"wav_1": {ID: "wav_1", StationID: "sta_1", Status: waveform.StatusProcessed},
			"wav_2": {ID: "wav_2", StationID: "sta_2", Status: waveform.StatusProcessed},
			"wav_3": {ID: "wav_3", StationID: "sta_3", Status: waveform.StatusProcessed},
		},
		txEntered: make(chan struct{}), allowTx: make(chan struct{}),
	}
	service := New(store, store, clock.NewFake(now), &idgen.Sequence{}, time.Minute)
	principal := auth.Principal{UserID: "operator_1", Role: auth.RoleOperator}
	input := event.DetectionInput{PublicID: "evt-public-10", DetectedAt: now.Add(-time.Minute), Latitude: 30, Longitude: 104, DepthKM: 12, Magnitude: 4.2, Picks: []event.Pick{
		{WaveformID: "wav_1", StationID: "sta_1", Phase: event.PhaseP, PickedAt: now.Add(-30 * time.Second), Confidence: 0.9},
		{WaveformID: "wav_2", StationID: "sta_2", Phase: event.PhaseP, PickedAt: now.Add(-25 * time.Second), Confidence: 0.8},
		{WaveformID: "wav_3", StationID: "sta_3", Phase: event.PhaseS, PickedAt: now.Add(-20 * time.Second), Confidence: 0.85},
	}}
	result := make(chan error, 1)
	go func() {
		_, err := service.Detect(context.Background(), principal, input)
		result <- err
	}()
	<-store.txEntered
	store.reject("wav_1")
	close(store.allowTx)
	err := <-result
	if !errors.Is(err, fault.ErrInvalidState) {
		t.Fatalf("detection error = %v; want invalid state for waveform rejected at transaction entry", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.events != 0 || store.picks != 0 {
		t.Fatalf("persisted events = %d, picks = %d; want transaction rollback", store.events, store.picks)
	}
}
