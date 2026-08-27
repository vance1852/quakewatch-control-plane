package event

import (
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

func TestDetectionRequiresThreePhysicalStations(t *testing.T) {
	detectedAt := time.Date(2026, 8, 27, 7, 0, 0, 0, time.UTC)
	input := DetectionInput{
		PublicID: "quake-physical-stations", DetectedAt: detectedAt,
		Latitude: 30, Longitude: 104, DepthKM: 12, Magnitude: 4.5,
		Picks: []Pick{
			{WaveformID: "wav-1", StationID: "sta-only", Phase: PhaseP, PickedAt: detectedAt.Add(time.Second), Confidence: 0.9},
			{WaveformID: "wav-2", StationID: "sta-only", Phase: PhaseS, PickedAt: detectedAt.Add(2 * time.Second), Confidence: 0.8},
			{WaveformID: "wav-3", StationID: "sta-only", Phase: PhaseS, PickedAt: detectedAt.Add(3 * time.Second), Confidence: 0.7},
		},
	}

	_, err := ValidateDetection(input, detectedAt.Add(time.Minute))
	if !errors.Is(err, fault.ErrValidation) {
		t.Fatalf("ValidateDetection() error = %v; want validation failure for one physical station", err)
	}
}
