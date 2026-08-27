package event

import (
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

func validDetection(now time.Time) DetectionInput {
	detected := now.Add(-time.Minute)
	return DetectionInput{
		PublicID: "EQ-2026-0001", DetectedAt: detected, Latitude: 31.2,
		Longitude: 103.4, DepthKM: 12.5, Magnitude: 4.2,
		Picks: []Pick{
			{WaveformID: "wav_1", StationID: "sta_1", Phase: PhaseP, PickedAt: detected.Add(5 * time.Second), Confidence: .95},
			{WaveformID: "wav_2", StationID: "sta_2", Phase: PhaseP, PickedAt: detected.Add(6 * time.Second), Confidence: .91},
			{WaveformID: "wav_3", StationID: "sta_3", Phase: PhaseP, PickedAt: detected.Add(7 * time.Second), Confidence: .88},
		},
	}
}

func TestValidateDetection(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)
	validated, err := ValidateDetection(validDetection(now), now)
	if err != nil {
		t.Fatalf("ValidateDetection() error = %v", err)
	}
	if len(validated.Picks) != 3 {
		t.Fatalf("pick count = %d", len(validated.Picks))
	}
	tests := []struct {
		name   string
		mutate func(*DetectionInput)
	}{
		{name: "short public id", mutate: func(value *DetectionInput) { value.PublicID = "EQ" }},
		{name: "future detection", mutate: func(value *DetectionInput) { value.DetectedAt = now.Add(2 * time.Minute) }},
		{name: "latitude", mutate: func(value *DetectionInput) { value.Latitude = 100 }},
		{name: "longitude", mutate: func(value *DetectionInput) { value.Longitude = -200 }},
		{name: "depth", mutate: func(value *DetectionInput) { value.DepthKM = 900 }},
		{name: "magnitude", mutate: func(value *DetectionInput) { value.Magnitude = 11 }},
		{name: "too few stations", mutate: func(value *DetectionInput) { value.Picks[2].StationID = "sta_2" }},
		{name: "duplicate waveform phase", mutate: func(value *DetectionInput) { value.Picks[2].WaveformID = "wav_2" }},
		{name: "invalid confidence", mutate: func(value *DetectionInput) { value.Picks[0].Confidence = 2 }},
		{name: "invalid phase", mutate: func(value *DetectionInput) { value.Picks[0].Phase = "X" }},
		{name: "pick outside association", mutate: func(value *DetectionInput) { value.Picks[0].PickedAt = value.DetectedAt.Add(time.Hour) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validDetection(now)
			test.mutate(&input)
			if _, err := ValidateDetection(input, now); !errors.Is(err, fault.ErrValidation) {
				t.Fatalf("ValidateDetection() error = %v, want validation", err)
			}
		})
	}
}

func TestStationEvidenceDeduplicatesByStation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)
	detected := now.Add(-time.Minute)
	picks := []Pick{
		{WaveformID: "wav_1", StationID: "sta_1", Phase: PhaseP, PickedAt: detected.Add(5 * time.Second)},
		{WaveformID: "wav_1", StationID: "sta_1", Phase: PhaseS, PickedAt: detected.Add(20 * time.Second)},
		{WaveformID: "wav_2", StationID: "sta_1", Phase: PhaseS, PickedAt: detected.Add(25 * time.Second)},
		{WaveformID: "wav_3", StationID: "sta_2", Phase: PhaseP, PickedAt: detected.Add(6 * time.Second)},
	}
	evidence := NewStationEvidence()
	for _, pick := range picks {
		evidence.Add(pick)
	}
	if evidence.Count() != 2 {
		t.Fatalf("station count = %d, want 2 (P/S and waveform differences must not multiply a single station)", evidence.Count())
	}

	input := DetectionInput{
		PublicID: "EQ-2026-0001", DetectedAt: detected, Latitude: 31.2,
		Longitude: 103.4, DepthKM: 12.5, Magnitude: 4.2, Picks: picks,
	}
	if _, err := ValidateDetection(input, now); !errors.Is(err, fault.ErrValidation) {
		t.Fatalf("ValidateDetection() error = %v, want validation for two physical stations", err)
	}
}

func TestCandidateClaim(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)
	detected := Candidate{Status: StatusDetected}
	if err := detected.CanClaim("usr_a", now, 3); err != nil {
		t.Fatalf("detected claim error = %v", err)
	}
	if err := detected.CanClaim("usr_a", now, 2); !errors.Is(err, fault.ErrInvalidState) {
		t.Fatalf("few stations error = %v", err)
	}
	owner := "usr_b"
	lease := now.Add(time.Minute)
	leased := Candidate{Status: StatusUnderReview, ReviewOwnerID: &owner, ReviewLeaseUntil: &lease}
	if err := leased.CanClaim("usr_a", now, 3); !errors.Is(err, fault.ErrConflict) {
		t.Fatalf("foreign lease error = %v", err)
	}
	if err := leased.CanClaim("usr_b", now, 3); err != nil {
		t.Fatalf("same owner renew error = %v", err)
	}
	if err := leased.CanClaim("usr_a", lease, 3); err != nil {
		t.Fatalf("expired lease claim error = %v", err)
	}
}

func TestCandidateDecision(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)
	owner := "usr_a"
	lease := now.Add(time.Minute)
	candidate := Candidate{Status: StatusUnderReview, ReviewOwnerID: &owner, ReviewLeaseUntil: &lease}
	if err := candidate.CanDecide(owner, now, DecisionConfirm, "signals agree"); err != nil {
		t.Fatalf("valid confirm error = %v", err)
	}
	if err := candidate.CanDecide("usr_b", now, DecisionConfirm, "signals agree"); !errors.Is(err, fault.ErrForbidden) {
		t.Fatalf("foreign owner error = %v", err)
	}
	if err := candidate.CanDecide(owner, lease, DecisionConfirm, "signals agree"); !errors.Is(err, fault.ErrLeaseLost) {
		t.Fatalf("expired lease error = %v", err)
	}
	if err := candidate.CanDecide(owner, now, "maybe", "signals agree"); !errors.Is(err, fault.ErrValidation) {
		t.Fatalf("invalid decision error = %v", err)
	}
	if err := candidate.CanDecide(owner, now, DecisionDismiss, "x"); !errors.Is(err, fault.ErrValidation) {
		t.Fatalf("short notes error = %v", err)
	}
}

func TestCandidatePublish(t *testing.T) {
	t.Parallel()
	if err := (Candidate{Status: StatusConfirmed}).CanPublish(); err != nil {
		t.Fatalf("confirmed publish error = %v", err)
	}
	for _, status := range []Status{StatusDetected, StatusUnderReview, StatusDismissed, StatusPublished} {
		if err := (Candidate{Status: status}).CanPublish(); !errors.Is(err, fault.ErrInvalidState) {
			t.Errorf("status %s publish error = %v", status, err)
		}
	}
}
