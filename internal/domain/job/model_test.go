package job

import (
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

func TestValidateNew(t *testing.T) {
	t.Parallel()
	valid := Job{Kind: KindProcessWaveform, AggregateID: "wav_1", PayloadJSON: `{}`, MaxAttempts: 5}
	if err := ValidateNew(valid); err != nil {
		t.Fatalf("valid job error = %v", err)
	}
	tests := []Job{
		{Kind: "unknown", AggregateID: "wav_1", PayloadJSON: `{}`, MaxAttempts: 5},
		{Kind: KindProcessWaveform, AggregateID: "", PayloadJSON: `{}`, MaxAttempts: 5},
		{Kind: KindProcessWaveform, AggregateID: "wav_1", PayloadJSON: "", MaxAttempts: 5},
		{Kind: KindProcessWaveform, AggregateID: "wav_1", PayloadJSON: `{}`, MaxAttempts: 0},
		{Kind: KindProcessWaveform, AggregateID: "wav_1", PayloadJSON: `{}`, MaxAttempts: 21},
	}
	for _, value := range tests {
		if err := ValidateNew(value); !errors.Is(err, fault.ErrValidation) {
			t.Errorf("ValidateNew(%#v) error = %v", value, err)
		}
	}
}

func TestLeaseOwnership(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)
	pending := Job{Status: StatusPending, NextAttemptAt: now}
	if err := pending.CanLease("worker-a", now); err != nil {
		t.Fatalf("pending lease error = %v", err)
	}
	if err := pending.CanLease("", now); !errors.Is(err, fault.ErrValidation) {
		t.Fatalf("empty owner error = %v", err)
	}
	future := Job{Status: StatusRetryWait, NextAttemptAt: now.Add(time.Minute)}
	if err := future.CanLease("worker-a", now); !errors.Is(err, fault.ErrInvalidState) {
		t.Fatalf("future retry error = %v", err)
	}
	owner := "worker-a"
	until := now.Add(time.Minute)
	leased := Job{Status: StatusLeased, NextAttemptAt: now, LeaseOwner: &owner, LeaseUntil: &until}
	if err := leased.OwnsLease(owner, now); err != nil {
		t.Fatalf("OwnsLease() error = %v", err)
	}
	if err := leased.OwnsLease("worker-b", now); !errors.Is(err, fault.ErrLeaseLost) {
		t.Fatalf("wrong owner error = %v", err)
	}
	if err := leased.OwnsLease(owner, until); !errors.Is(err, fault.ErrLeaseLost) {
		t.Fatalf("expired lease error = %v", err)
	}
}

func TestRetryAt(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)
	for attempt, delay := range map[int]time.Duration{
		0:  time.Second,
		1:  time.Second,
		2:  2 * time.Second,
		3:  4 * time.Second,
		8:  128 * time.Second,
		20: 128 * time.Second,
	} {
		if got := RetryAt(now, attempt); !got.Equal(now.Add(delay)) {
			t.Errorf("RetryAt(%d) = %v, want %v", attempt, got, now.Add(delay))
		}
	}
}
