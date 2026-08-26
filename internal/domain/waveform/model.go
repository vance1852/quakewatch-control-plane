package waveform

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

type Status string

const (
	StatusReceived  Status = "received"
	StatusValidated Status = "validated"
	StatusProcessed Status = "processed"
	StatusRejected  Status = "rejected"
)

type Batch struct {
	ID              string    `json:"id"`
	StationID       string    `json:"station_id"`
	SensorID        string    `json:"sensor_id"`
	SourceKey       string    `json:"source_key"`
	StartsAt        time.Time `json:"starts_at"`
	EndsAt          time.Time `json:"ends_at"`
	SampleCount     int64     `json:"sample_count"`
	Checksum        string    `json:"checksum"`
	Status          Status    `json:"status"`
	RejectionReason string    `json:"rejection_reason,omitempty"`
	Version         int64     `json:"version"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

type IngestInput struct {
	StationID   string
	SensorID    string
	SourceKey   string
	StartsAt    time.Time
	EndsAt      time.Time
	SampleCount int64
	Checksum    string
}

func ValidateIngest(input IngestInput, sampleRate float64, now time.Time) (IngestInput, error) {
	input.StationID = strings.TrimSpace(input.StationID)
	input.SensorID = strings.TrimSpace(input.SensorID)
	input.SourceKey = strings.TrimSpace(input.SourceKey)
	input.Checksum = strings.ToLower(strings.TrimSpace(input.Checksum))
	if input.StationID == "" || input.SensorID == "" {
		return input, fault.Validation("station_id", "station_id and sensor_id are required")
	}
	if len(input.SourceKey) < 4 || len(input.SourceKey) > 160 {
		return input, fault.Validation("source_key", "must contain 4 to 160 characters")
	}
	if input.StartsAt.IsZero() || input.EndsAt.IsZero() || !input.EndsAt.After(input.StartsAt) {
		return input, fault.Validation("ends_at", "must be after starts_at")
	}
	if input.EndsAt.Sub(input.StartsAt) > 24*time.Hour {
		return input, fault.Validation("ends_at", "waveform batch cannot exceed 24 hours")
	}
	if input.EndsAt.After(now.Add(5 * time.Minute)) {
		return input, fault.Validation("ends_at", "cannot be more than five minutes in the future")
	}
	if input.SampleCount <= 0 {
		return input, fault.Validation("sample_count", "must be positive")
	}
	expected := input.EndsAt.Sub(input.StartsAt).Seconds() * sampleRate
	tolerance := expected * 0.02
	if tolerance < 2 {
		tolerance = 2
	}
	delta := float64(input.SampleCount) - expected
	if delta < 0 {
		delta = -delta
	}
	if delta > tolerance {
		return input, fault.Validation("sample_count", "does not match interval and sensor sample rate")
	}
	decoded, err := hex.DecodeString(input.Checksum)
	if err != nil || len(decoded) != 32 {
		return input, fault.Validation("checksum", "must be a 64-character SHA-256 checksum")
	}
	input.StartsAt = input.StartsAt.UTC()
	input.EndsAt = input.EndsAt.UTC()
	return input, nil
}

func (b Batch) CanTransition(next Status, reason string) error {
	if b.Status == next {
		return fmt.Errorf("%w: waveform already %s", fault.ErrInvalidState, next)
	}
	switch {
	case b.Status == StatusReceived && next == StatusValidated:
		if reason != "" {
			return fault.Validation("rejection_reason", "must be empty when validating")
		}
		return nil
	case (b.Status == StatusReceived || b.Status == StatusValidated) && next == StatusRejected:
		if strings.TrimSpace(reason) == "" {
			return fault.Validation("rejection_reason", "is required when rejecting")
		}
		return nil
	case b.Status == StatusValidated && next == StatusProcessed:
		return nil
	default:
		return fmt.Errorf("%w: waveform %s to %s", fault.ErrInvalidState, b.Status, next)
	}
}
