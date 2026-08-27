package event

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

type Status string

const (
	StatusDetected    Status = "detected"
	StatusUnderReview Status = "under_review"
	StatusConfirmed   Status = "confirmed"
	StatusDismissed   Status = "dismissed"
	StatusPublished   Status = "published"
)

type Candidate struct {
	ID               string     `json:"id"`
	PublicID         string     `json:"public_id"`
	DetectedAt       time.Time  `json:"detected_at"`
	Latitude         float64    `json:"latitude"`
	Longitude        float64    `json:"longitude"`
	DepthKM          float64    `json:"depth_km"`
	Magnitude        float64    `json:"magnitude"`
	Status           Status     `json:"status"`
	ReviewOwnerID    *string    `json:"review_owner_id,omitempty"`
	ReviewLeaseUntil *time.Time `json:"review_lease_until,omitempty"`
	Version          int64      `json:"version"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

type Phase string

const (
	PhaseP Phase = "P"
	PhaseS Phase = "S"
)

type Pick struct {
	ID         string    `json:"id"`
	EventID    string    `json:"event_id"`
	WaveformID string    `json:"waveform_id"`
	StationID  string    `json:"station_id"`
	Phase      Phase     `json:"phase"`
	PickedAt   time.Time `json:"picked_at"`
	Confidence float64   `json:"confidence"`
	CreatedAt  time.Time `json:"created_at"`
}

type Decision string

const (
	DecisionConfirm Decision = "confirm"
	DecisionDismiss Decision = "dismiss"
)

type ReviewDecision struct {
	ID           string    `json:"id"`
	EventID      string    `json:"event_id"`
	AnalystID    string    `json:"analyst_id"`
	Decision     Decision  `json:"decision"`
	Notes        string    `json:"notes"`
	EventVersion int64     `json:"event_version"`
	CreatedAt    time.Time `json:"created_at"`
}

type DetectionInput struct {
	PublicID   string
	DetectedAt time.Time
	Latitude   float64
	Longitude  float64
	DepthKM    float64
	Magnitude  float64
	Picks      []Pick
}

type StationEvidence struct {
	keys map[string]struct{}
}

func NewStationEvidence() *StationEvidence {
	return &StationEvidence{keys: make(map[string]struct{})}
}

// Add records the physical station that produced a pick. Evidence is deduplicated
// strictly by station: phase (P/S) and waveform differences must not let a single
// station contribute more than once toward the minimum-station threshold.
func (s *StationEvidence) Add(pick Pick) {
	s.keys[pick.StationID] = struct{}{}
}

func (s *StationEvidence) Count() int {
	return len(s.keys)
}

func ValidateDetection(input DetectionInput, now time.Time) (DetectionInput, error) {
	input.PublicID = strings.TrimSpace(input.PublicID)
	if len(input.PublicID) < 6 || len(input.PublicID) > 80 {
		return input, fault.Validation("public_id", "must contain 6 to 80 characters")
	}
	if input.DetectedAt.IsZero() || input.DetectedAt.After(now.Add(time.Minute)) {
		return input, fault.Validation("detected_at", "must not be in the future")
	}
	if input.Latitude < -90 || input.Latitude > 90 || math.IsNaN(input.Latitude) {
		return input, fault.Validation("latitude", "must be between -90 and 90")
	}
	if input.Longitude < -180 || input.Longitude > 180 || math.IsNaN(input.Longitude) {
		return input, fault.Validation("longitude", "must be between -180 and 180")
	}
	if input.DepthKM < -5 || input.DepthKM > 800 || math.IsNaN(input.DepthKM) {
		return input, fault.Validation("depth_km", "must be between -5 and 800")
	}
	if input.Magnitude < -2 || input.Magnitude > 10 || math.IsNaN(input.Magnitude) {
		return input, fault.Validation("magnitude", "must be between -2 and 10")
	}
	seen := make(map[string]bool)
	stations := NewStationEvidence()
	for index := range input.Picks {
		pick, err := ValidatePick(input.Picks[index], input.DetectedAt)
		if err != nil {
			return input, fmt.Errorf("pick %d: %w", index, err)
		}
		key := pick.WaveformID + "/" + string(pick.Phase)
		if seen[key] {
			return input, fault.Validation("picks", "contains duplicate waveform phase")
		}
		seen[key] = true
		stations.Add(pick)
		input.Picks[index] = pick
	}
	if stations.Count() < 3 {
		return input, fault.Validation("picks", "must include at least three distinct stations")
	}
	input.DetectedAt = input.DetectedAt.UTC()
	return input, nil
}

func ValidatePick(pick Pick, detectedAt time.Time) (Pick, error) {
	if pick.WaveformID == "" || pick.StationID == "" {
		return pick, fault.Validation("pick", "waveform_id and station_id are required")
	}
	if pick.Phase != PhaseP && pick.Phase != PhaseS {
		return pick, fault.Validation("phase", "must be P or S")
	}
	if pick.Confidence < 0 || pick.Confidence > 1 || math.IsNaN(pick.Confidence) {
		return pick, fault.Validation("confidence", "must be between 0 and 1")
	}
	if pick.PickedAt.Before(detectedAt.Add(-2*time.Minute)) || pick.PickedAt.After(detectedAt.Add(20*time.Minute)) {
		return pick, fault.Validation("picked_at", "falls outside the event association window")
	}
	pick.PickedAt = pick.PickedAt.UTC()
	return pick, nil
}

func (c Candidate) CanClaim(actorID string, now time.Time, distinctStations int) error {
	if c.Status != StatusDetected && c.Status != StatusUnderReview {
		return fmt.Errorf("%w: event %s cannot be claimed", fault.ErrInvalidState, c.Status)
	}
	if distinctStations < 3 {
		return fmt.Errorf("%w: event needs picks from three stations", fault.ErrInvalidState)
	}
	if c.ReviewOwnerID != nil && c.ReviewLeaseUntil != nil && now.Before(*c.ReviewLeaseUntil) && *c.ReviewOwnerID != actorID {
		return fmt.Errorf("%w: event review is leased", fault.ErrConflict)
	}
	return nil
}

func (c Candidate) CanDecide(actorID string, now time.Time, decision Decision, notes string) error {
	if c.Status != StatusUnderReview {
		return fmt.Errorf("%w: event is not under review", fault.ErrInvalidState)
	}
	if c.ReviewOwnerID == nil || *c.ReviewOwnerID != actorID {
		return fmt.Errorf("%w: analyst does not own review", fault.ErrForbidden)
	}
	if c.ReviewLeaseUntil == nil || !now.Before(*c.ReviewLeaseUntil) {
		return fmt.Errorf("%w: review lease expired", fault.ErrLeaseLost)
	}
	if decision != DecisionConfirm && decision != DecisionDismiss {
		return fault.Validation("decision", "must be confirm or dismiss")
	}
	if len(strings.TrimSpace(notes)) < 3 || len(notes) > 2000 {
		return fault.Validation("notes", "must contain 3 to 2000 characters")
	}
	return nil
}

func (c Candidate) CanPublish() error {
	if c.Status != StatusConfirmed {
		return fmt.Errorf("%w: only confirmed events can be published", fault.ErrInvalidState)
	}
	return nil
}
