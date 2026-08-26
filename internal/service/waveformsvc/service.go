package waveformsvc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
	"github.com/vance1852/quakewatch-control-plane/internal/service/shared"
)

type Service struct {
	store repository.Store
	tx    repository.Transactor
	clock clock.Clock
	ids   idgen.Generator
}

type IngestResult struct {
	Batch  waveform.Batch `json:"batch"`
	Reused bool           `json:"reused"`
}

func New(store repository.Store, tx repository.Transactor, valueClock clock.Clock, ids idgen.Generator) *Service {
	return &Service{store: store, tx: tx, clock: valueClock, ids: ids}
}

func (s *Service) Ingest(ctx context.Context, principal auth.Principal, input waveform.IngestInput) (IngestResult, error) {
	if err := principal.Require(auth.PermissionIngest); err != nil {
		return IngestResult{}, err
	}
	stationValue, err := s.store.GetStation(ctx, input.StationID)
	if err != nil {
		return IngestResult{}, err
	}
	sensorValue, err := s.store.GetSensor(ctx, input.SensorID)
	if err != nil {
		return IngestResult{}, err
	}
	if sensorValue.StationID != stationValue.ID {
		return IngestResult{}, fmt.Errorf("%w: sensor does not belong to station", fault.ErrConflict)
	}
	if sensorValue.DisabledAt != nil {
		return IngestResult{}, fmt.Errorf("%w: sensor is disabled", fault.ErrInvalidState)
	}
	validated, err := waveform.ValidateIngest(input, sensorValue.SampleRateHz, s.clock.Now())
	if err != nil {
		return IngestResult{}, err
	}
	if err := stationValue.AcceptsWaveform(validated.StartsAt, validated.EndsAt); err != nil {
		return IngestResult{}, err
	}
	existing, err := s.store.GetWaveformBySource(ctx, validated.SensorID, validated.SourceKey)
	if err == nil {
		if sameWaveform(existing, validated) {
			return IngestResult{Batch: existing, Reused: true}, nil
		}
		return IngestResult{}, fmt.Errorf("%w: source key belongs to different waveform metadata", fault.ErrConflict)
	}
	if !errors.Is(err, fault.ErrNotFound) {
		return IngestResult{}, err
	}
	overlap, err := s.store.HasWaveformOverlap(ctx, validated.SensorID, validated.StartsAt, validated.EndsAt)
	if err != nil {
		return IngestResult{}, err
	}
	if overlap {
		return IngestResult{}, fmt.Errorf("%w: waveform interval overlaps an accepted batch", fault.ErrConflict)
	}
	now := s.clock.Now()
	batch := waveform.Batch{
		ID:          s.ids.New("wav"),
		StationID:   validated.StationID,
		SensorID:    validated.SensorID,
		SourceKey:   validated.SourceKey,
		StartsAt:    validated.StartsAt,
		EndsAt:      validated.EndsAt,
		SampleCount: validated.SampleCount,
		Checksum:    validated.Checksum,
		Status:      waveform.StatusReceived,
		Version:     1,
		CreatedAt:   now,
		UpdatedAt:   now,
	}
	payload, _ := json.Marshal(map[string]string{"waveform_id": batch.ID})
	workerJob := job.Job{
		ID:            s.ids.New("job"),
		Kind:          job.KindProcessWaveform,
		AggregateID:   batch.ID,
		PayloadJSON:   string(payload),
		Status:        job.StatusPending,
		MaxAttempts:   6,
		NextAttemptAt: now,
		Version:       1,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	err = s.tx.WithinTx(ctx, func(store repository.Store) error {
		if err := store.CreateWaveform(ctx, batch); err != nil {
			return err
		}
		if err := store.CreateJob(ctx, workerJob); err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "waveform.received", "waveform", batch.ID, "success", map[string]any{
			"station_id":   batch.StationID,
			"sensor_id":    batch.SensorID,
			"starts_at":    batch.StartsAt,
			"ends_at":      batch.EndsAt,
			"sample_count": batch.SampleCount,
			"job_id":       workerJob.ID,
		}, now)
	})
	if err != nil {
		return IngestResult{}, fault.Wrap("ingest waveform transaction", err)
	}
	return IngestResult{Batch: batch}, nil
}

func sameWaveform(existing waveform.Batch, input waveform.IngestInput) bool {
	return existing.StationID == input.StationID && existing.SensorID == input.SensorID &&
		existing.SourceKey == input.SourceKey && existing.StartsAt.Equal(input.StartsAt) &&
		existing.EndsAt.Equal(input.EndsAt) && existing.SampleCount == input.SampleCount &&
		existing.Checksum == input.Checksum
}

func (s *Service) Get(ctx context.Context, id string) (waveform.Batch, error) {
	return s.store.GetWaveform(ctx, id)
}

func (s *Service) List(ctx context.Context, filter repository.WaveformFilter) (repository.Page[waveform.Batch], error) {
	return s.store.ListWaveforms(ctx, filter)
}

func (s *Service) Process(ctx context.Context, waveformID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := s.clock.Now()
	return s.tx.WithinTx(ctx, func(store repository.Store) error {
		batch, err := store.GetWaveform(ctx, waveformID)
		if err != nil {
			return err
		}
		if batch.Status == waveform.StatusProcessed {
			return nil
		}
		if batch.Status == waveform.StatusRejected {
			return fmt.Errorf("%w: rejected waveform cannot be processed", fault.ErrInvalidState)
		}
		if batch.Status == waveform.StatusReceived {
			if err := batch.CanTransition(waveform.StatusValidated, ""); err != nil {
				return err
			}
			batch, err = store.UpdateWaveformStatus(ctx, batch.ID, waveform.StatusValidated, "", batch.Version, now)
			if err != nil {
				return err
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := batch.CanTransition(waveform.StatusProcessed, ""); err != nil {
			return err
		}
		batch, err = store.UpdateWaveformStatus(ctx, batch.ID, waveform.StatusProcessed, "", batch.Version, now)
		if err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, nil, "waveform.processed", "waveform", batch.ID, "success", map[string]any{
			"worker": true,
			"status": batch.Status,
		}, now)
	})
}

func (s *Service) Reject(ctx context.Context, principal auth.Principal, waveformID, reason string, version int64) (waveform.Batch, error) {
	if err := principal.Require(auth.PermissionIngest); err != nil {
		return waveform.Batch{}, err
	}
	now := s.clock.Now()
	var updated waveform.Batch
	err := s.tx.WithinTx(ctx, func(store repository.Store) error {
		current, err := store.GetWaveform(ctx, waveformID)
		if err != nil {
			return err
		}
		if current.Version != version {
			return fault.ErrVersion
		}
		if err := current.CanTransition(waveform.StatusRejected, reason); err != nil {
			return err
		}
		updated, err = store.UpdateWaveformStatus(ctx, waveformID, waveform.StatusRejected, reason, version, now)
		if err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "waveform.rejected", "waveform", waveformID, "success", map[string]any{
			"reason": reason,
		}, now)
	})
	return updated, err
}

func (s *Service) RecoverJob(ctx context.Context, value job.Job) error {
	if value.Kind != job.KindProcessWaveform {
		return fmt.Errorf("%w: unsupported job kind %s", fault.ErrValidation, value.Kind)
	}
	return s.Process(ctx, value.AggregateID)
}
