package waveformsvc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

var errWaveformAuditUnavailable = errors.New("audit storage unavailable")

type validationAtomicityStore struct {
	repository.Store
	batch waveform.Batch
}

func (s *validationAtomicityStore) GetWaveform(context.Context, string) (waveform.Batch, error) {
	return s.batch, nil
}

func (s *validationAtomicityStore) AdvanceWaveformValidation(_ context.Context, _ string, _ int64, now time.Time) (waveform.Batch, error) {
	s.batch.Status = waveform.StatusValidated
	s.batch.Version++
	s.batch.UpdatedAt = now
	return s.batch, nil
}

func (s *validationAtomicityStore) UpdateWaveformStatus(_ context.Context, _ string, status waveform.Status, reason string, _ int64, now time.Time) (waveform.Batch, error) {
	s.batch.Status = status
	s.batch.RejectionReason = reason
	s.batch.Version++
	s.batch.UpdatedAt = now
	return s.batch, nil
}

func (s *validationAtomicityStore) CreateAudit(context.Context, audit.Event) error {
	return errWaveformAuditUnavailable
}

func (s *validationAtomicityStore) WithinTx(ctx context.Context, operation func(repository.Store) error) error {
	before := s.batch
	if err := operation(s); err != nil {
		s.batch = before
		return err
	}
	return nil
}

func TestWaveformProcessingRollsBackValidationWhenAuditFails(t *testing.T) {
	now := time.Date(2026, 8, 26, 14, 0, 0, 0, time.UTC)
	store := &validationAtomicityStore{batch: waveform.Batch{
		ID: "wav_atomic", StationID: "sta_1", SensorID: "sen_1", SourceKey: "source-atomic",
		Status: waveform.StatusReceived, Version: 1, CreatedAt: now.Add(-time.Minute), UpdatedAt: now.Add(-time.Minute),
	}}
	service := New(store, store, clock.NewFake(now), &idgen.Sequence{})

	err := service.Process(context.Background(), store.batch.ID)
	if !errors.Is(err, errWaveformAuditUnavailable) {
		t.Fatalf("Process() error = %v; want audit storage failure", err)
	}
	if store.batch.Status != waveform.StatusReceived || store.batch.Version != 1 {
		t.Fatalf("waveform after failed processing = status %s version %d; want received version 1", store.batch.Status, store.batch.Version)
	}
}
