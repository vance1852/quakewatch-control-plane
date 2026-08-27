package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
	"github.com/vance1852/quakewatch-control-plane/internal/service/waveformsvc"
)

type Handlers struct {
	waveforms *waveformsvc.Service
	store     repository.Store
	clock     clock.Clock
}

func NewHandlers(waveforms *waveformsvc.Service, store repository.Store, valueClock clock.Clock) *Handlers {
	return &Handlers{waveforms: waveforms, store: store, clock: valueClock}
}

func (h *Handlers) HandleJob(ctx context.Context, value job.Job) error {
	if err := value.OwnsLease(pointerValue(value.LeaseOwner), h.clock.Now()); err != nil {
		return err
	}
	switch value.Kind {
	case job.KindProcessWaveform:
		return h.handleWaveform(ctx, value)
	case job.KindCleanupSession:
		return h.handleCleanup(ctx, value)
	case job.KindDeliverAlert:
		return Permanent{Err: fmt.Errorf("%w: alert jobs use delivery queue", fault.ErrInvalidState)}
	default:
		return Permanent{Err: fmt.Errorf("%w: unknown worker kind %s", fault.ErrValidation, value.Kind)}
	}
}

func (h *Handlers) handleWaveform(ctx context.Context, value job.Job) error {
	var payload struct {
		WaveformID string `json:"waveform_id"`
	}
	if err := json.Unmarshal([]byte(value.PayloadJSON), &payload); err != nil {
		return Permanent{Err: fmt.Errorf("decode waveform job: %w", err)}
	}
	if payload.WaveformID == "" || payload.WaveformID != value.AggregateID {
		return Permanent{Err: errorsPayloadMismatch(value.AggregateID, payload.WaveformID)}
	}
	return h.waveforms.Process(ctx, payload.WaveformID)
}

func (h *Handlers) handleCleanup(ctx context.Context, value job.Job) error {
	var payload struct {
		Limit int `json:"limit"`
	}
	if err := json.Unmarshal([]byte(value.PayloadJSON), &payload); err != nil {
		return Permanent{Err: fmt.Errorf("decode cleanup job: %w", err)}
	}
	if payload.Limit <= 0 || payload.Limit > 1000 {
		return Permanent{Err: fault.Validation("limit", "cleanup limit must be between 1 and 1000")}
	}
	now := h.clock.Now()
	budget := job.NewCleanupBudget(payload.Limit)
	deletedSessions, err := h.store.DeleteExpiredSessions(ctx, now, budget.Remaining())
	if err != nil {
		return err
	}
	budget.Consume(deletedSessions)
	if budget.Remaining() == 0 {
		return nil
	}
	if _, err := h.store.DeleteExpiredIdempotency(ctx, now, budget.Remaining()); err != nil {
		return err
	}
	return nil
}

func errorsPayloadMismatch(aggregateID, payloadID string) error {
	return fmt.Errorf("%w: aggregate %q does not match payload %q", fault.ErrValidation, aggregateID, payloadID)
}

func pointerValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

type Permanent struct {
	Err error
}

func (e Permanent) Error() string   { return e.Err.Error() }
func (e Permanent) Unwrap() error   { return e.Err }
func (e Permanent) Permanent() bool { return true }

func CleanupJob(ids interface{ New(string) string }, now time.Time, limit int) job.Job {
	payload, _ := json.Marshal(map[string]int{"limit": limit})
	return job.Job{
		ID:            ids.New("job"),
		Kind:          job.KindCleanupSession,
		AggregateID:   now.UTC().Format("2006-01-02"),
		PayloadJSON:   string(payload),
		Status:        job.StatusPending,
		MaxAttempts:   4,
		NextAttemptAt: now,
		Version:       1,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
}
