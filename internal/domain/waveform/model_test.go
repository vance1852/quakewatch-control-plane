package waveform

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

func validIngest(now time.Time) IngestInput {
	return IngestInput{
		StationID: "sta_1", SensorID: "sen_1", SourceKey: "segment-001",
		StartsAt: now.Add(-time.Minute), EndsAt: now,
		SampleCount: 6000, Checksum: strings.Repeat("a", 64),
	}
}

func TestValidateIngest(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	if _, err := ValidateIngest(validIngest(now), 100, now); err != nil {
		t.Fatalf("valid ingest error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*IngestInput)
	}{
		{name: "missing station", mutate: func(value *IngestInput) { value.StationID = "" }},
		{name: "short source", mutate: func(value *IngestInput) { value.SourceKey = "x" }},
		{name: "reversed interval", mutate: func(value *IngestInput) { value.EndsAt = value.StartsAt }},
		{name: "future interval", mutate: func(value *IngestInput) { value.EndsAt = now.Add(6 * time.Minute) }},
		{name: "too long", mutate: func(value *IngestInput) { value.StartsAt = now.Add(-25 * time.Hour) }},
		{name: "zero samples", mutate: func(value *IngestInput) { value.SampleCount = 0 }},
		{name: "sample mismatch", mutate: func(value *IngestInput) { value.SampleCount = 1 }},
		{name: "bad checksum", mutate: func(value *IngestInput) { value.Checksum = "not-sha256" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validIngest(now)
			test.mutate(&input)
			if _, err := ValidateIngest(input, 100, now); !errors.Is(err, fault.ErrValidation) {
				t.Fatalf("ValidateIngest() error = %v, want validation", err)
			}
		})
	}
}

func TestBatchTransitions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		current Status
		next    Status
		reason  string
		valid   bool
	}{
		{name: "received validates", current: StatusReceived, next: StatusValidated, valid: true},
		{name: "validated processes", current: StatusValidated, next: StatusProcessed, valid: true},
		{name: "received rejects", current: StatusReceived, next: StatusRejected, reason: "checksum mismatch", valid: true},
		{name: "validated rejects", current: StatusValidated, next: StatusRejected, reason: "quality threshold", valid: true},
		{name: "reject needs reason", current: StatusReceived, next: StatusRejected},
		{name: "validate cannot have rejection", current: StatusReceived, next: StatusValidated, reason: "unexpected"},
		{name: "processed terminal", current: StatusProcessed, next: StatusRejected, reason: "late"},
		{name: "same rejected", current: StatusRejected, next: StatusRejected, reason: "again"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := (Batch{Status: test.current}).CanTransition(test.next, test.reason)
			if test.valid && err != nil {
				t.Fatalf("CanTransition() error = %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("CanTransition() unexpectedly succeeded")
			}
		})
	}
}
