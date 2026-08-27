package job

import (
	"fmt"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

type Kind string

const (
	KindProcessWaveform Kind = "process_waveform"
	KindDeliverAlert    Kind = "deliver_alert"
	KindCleanupSession  Kind = "cleanup_sessions"
)

type Status string

const (
	StatusPending   Status = "pending"
	StatusLeased    Status = "leased"
	StatusRetryWait Status = "retry_wait"
	StatusCompleted Status = "completed"
	StatusDead      Status = "dead"
)

type Job struct {
	ID            string     `json:"id"`
	Kind          Kind       `json:"kind"`
	AggregateID   string     `json:"aggregate_id"`
	PayloadJSON   string     `json:"payload_json"`
	Status        Status     `json:"status"`
	AttemptCount  int        `json:"attempt_count"`
	MaxAttempts   int        `json:"max_attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LeaseOwner    *string    `json:"lease_owner,omitempty"`
	LeaseUntil    *time.Time `json:"lease_until,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	Version       int64      `json:"version"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type CleanupBudget struct {
	remaining int
}

func NewCleanupBudget(limit int) *CleanupBudget {
	return &CleanupBudget{remaining: limit}
}

func (b *CleanupBudget) Consume(count int64) {
	b.remaining -= int(count)
	if b.remaining < 0 {
		b.remaining = 0
	}
}

func (b *CleanupBudget) Remaining() int {
	return b.remaining
}

func ValidateNew(value Job) error {
	switch value.Kind {
	case KindProcessWaveform, KindDeliverAlert, KindCleanupSession:
	default:
		return fault.Validation("kind", "unsupported worker job kind")
	}
	if strings.TrimSpace(value.AggregateID) == "" {
		return fault.Validation("aggregate_id", "is required")
	}
	if strings.TrimSpace(value.PayloadJSON) == "" {
		return fault.Validation("payload_json", "is required")
	}
	if value.MaxAttempts < 1 || value.MaxAttempts > 20 {
		return fault.Validation("max_attempts", "must be between 1 and 20")
	}
	return nil
}

func (j Job) CanLease(owner string, now time.Time) error {
	if owner == "" {
		return fault.Validation("lease_owner", "is required")
	}
	if j.Status == StatusCompleted || j.Status == StatusDead {
		return fmt.Errorf("%w: terminal job", fault.ErrInvalidState)
	}
	if now.Before(j.NextAttemptAt) {
		return fmt.Errorf("%w: job not ready", fault.ErrInvalidState)
	}
	if j.Status == StatusLeased && j.LeaseUntil != nil && now.Before(*j.LeaseUntil) {
		return fmt.Errorf("%w: job already leased", fault.ErrConflict)
	}
	return nil
}

func (j Job) OwnsLease(owner string, now time.Time) error {
	if j.Status != StatusLeased || j.LeaseOwner == nil || *j.LeaseOwner != owner {
		return fmt.Errorf("%w: owner mismatch", fault.ErrLeaseLost)
	}
	if j.LeaseUntil == nil || !now.Before(*j.LeaseUntil) {
		return fmt.Errorf("%w: lease expired", fault.ErrLeaseLost)
	}
	return nil
}

func RetryAt(now time.Time, attempt int) time.Time {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}
	return now.Add(time.Duration(1<<(attempt-1)) * time.Second)
}
