package station

import (
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

type Status string

const (
	StatusProvisioning Status = "provisioning"
	StatusActive       Status = "active"
	StatusMaintenance  Status = "maintenance"
	StatusRetired      Status = "retired"
)

var codePattern = regexp.MustCompile(`^[A-Z][A-Z0-9]{2,9}$`)
var channelPattern = regexp.MustCompile(`^[A-Z0-9]{2,5}$`)

type Station struct {
	ID               string     `json:"id"`
	Code             string     `json:"code"`
	Name             string     `json:"name"`
	Latitude         float64    `json:"latitude"`
	Longitude        float64    `json:"longitude"`
	ElevationM       float64    `json:"elevation_m"`
	Timezone         string     `json:"timezone"`
	Status           Status     `json:"status"`
	MaintenanceFrom  *time.Time `json:"maintenance_from,omitempty"`
	MaintenanceUntil *time.Time `json:"maintenance_until,omitempty"`
	Version          int64      `json:"version"`
	CreatedAt        time.Time  `json:"created_at"`
	UpdatedAt        time.Time  `json:"updated_at"`
}

type Sensor struct {
	ID           string     `json:"id"`
	StationID    string     `json:"station_id"`
	SerialNumber string     `json:"serial_number"`
	Channel      string     `json:"channel"`
	SampleRateHz float64    `json:"sample_rate_hz"`
	InstalledAt  time.Time  `json:"installed_at"`
	CalibratedAt time.Time  `json:"calibrated_at"`
	DisabledAt   *time.Time `json:"disabled_at,omitempty"`
	Version      int64      `json:"version"`
	CreatedAt    time.Time  `json:"created_at"`
}

type RegisterInput struct {
	Code             string
	Name             string
	Latitude         float64
	Longitude        float64
	ElevationM       float64
	Timezone         string
	SensorSerial     string
	SensorChannel    string
	SensorSampleRate float64
	InstalledAt      time.Time
	CalibratedAt     time.Time
}

type MaintenanceWindow struct {
	From  time.Time
	Until time.Time
}

func NormalizeCode(value string) (string, error) {
	value = strings.ToUpper(strings.TrimSpace(value))
	if !codePattern.MatchString(value) {
		return "", fault.Validation("code", "must contain 3 to 10 uppercase letters or digits")
	}
	return value, nil
}

func NormalizeChannel(value string) (string, error) {
	value = strings.ToUpper(strings.TrimSpace(value))
	if !channelPattern.MatchString(value) {
		return "", fault.Validation("channel", "must contain 2 to 5 uppercase letters or digits")
	}
	return value, nil
}

func ValidateCoordinates(latitude, longitude, elevation float64) error {
	if math.IsNaN(latitude) || math.IsInf(latitude, 0) || latitude < -90 || latitude > 90 {
		return fault.Validation("latitude", "must be between -90 and 90")
	}
	if math.IsNaN(longitude) || math.IsInf(longitude, 0) || longitude < -180 || longitude > 180 {
		return fault.Validation("longitude", "must be between -180 and 180")
	}
	if math.IsNaN(elevation) || math.IsInf(elevation, 0) || elevation < -500 || elevation > 9000 {
		return fault.Validation("elevation_m", "must be between -500 and 9000")
	}
	return nil
}

func ValidateRegister(input RegisterInput) (RegisterInput, error) {
	code, err := NormalizeCode(input.Code)
	if err != nil {
		return input, err
	}
	input.Code = code
	input.Name = strings.TrimSpace(input.Name)
	if len(input.Name) < 2 || len(input.Name) > 120 {
		return input, fault.Validation("name", "must contain 2 to 120 characters")
	}
	if err := ValidateCoordinates(input.Latitude, input.Longitude, input.ElevationM); err != nil {
		return input, err
	}
	if _, err := time.LoadLocation(input.Timezone); err != nil {
		return input, fault.Validation("timezone", "must be a valid IANA timezone")
	}
	input.SensorSerial = strings.ToUpper(strings.TrimSpace(input.SensorSerial))
	if len(input.SensorSerial) < 4 || len(input.SensorSerial) > 80 {
		return input, fault.Validation("sensor_serial", "must contain 4 to 80 characters")
	}
	channel, err := NormalizeChannel(input.SensorChannel)
	if err != nil {
		return input, err
	}
	input.SensorChannel = channel
	if input.SensorSampleRate < 1 || input.SensorSampleRate > 10000 {
		return input, fault.Validation("sample_rate_hz", "must be between 1 and 10000")
	}
	if input.InstalledAt.IsZero() || input.CalibratedAt.IsZero() {
		return input, fault.Validation("installed_at", "installation and calibration times are required")
	}
	if input.CalibratedAt.Before(input.InstalledAt) {
		return input, fault.Validation("calibrated_at", "must not precede installation")
	}
	return input, nil
}

func (s Station) CanTransition(next Status, enabledSensors int, latestCalibration time.Time, now time.Time) error {
	if s.Status == next {
		return fmt.Errorf("%w: station already %s", fault.ErrInvalidState, next)
	}
	if s.Status == StatusRetired {
		return fmt.Errorf("%w: retired station is terminal", fault.ErrInvalidState)
	}
	switch {
	case s.Status == StatusProvisioning && next == StatusActive:
		if enabledSensors == 0 {
			return fmt.Errorf("%w: station requires an enabled sensor", fault.ErrInvalidState)
		}
		if latestCalibration.Before(now.AddDate(0, -6, 0)) {
			return fmt.Errorf("%w: sensor calibration is older than six months", fault.ErrInvalidState)
		}
		return nil
	case s.Status == StatusActive && next == StatusMaintenance:
		return nil
	case s.Status == StatusMaintenance && next == StatusActive:
		return nil
	case (s.Status == StatusProvisioning || s.Status == StatusActive || s.Status == StatusMaintenance) && next == StatusRetired:
		return nil
	default:
		return fmt.Errorf("%w: %s to %s", fault.ErrInvalidState, s.Status, next)
	}
}

func ValidateMaintenance(from, until, now time.Time) (MaintenanceWindow, error) {
	if from.IsZero() || until.IsZero() {
		return MaintenanceWindow{}, fault.Validation("maintenance", "from and until are required")
	}
	if !until.After(from) {
		return MaintenanceWindow{}, fault.Validation("maintenance_until", "must be after maintenance_from")
	}
	if until.Sub(from) > 30*24*time.Hour {
		return MaintenanceWindow{}, fault.Validation("maintenance_until", "window cannot exceed 30 days")
	}
	if until.Before(now) {
		return MaintenanceWindow{}, fault.Validation("maintenance_until", "window has already ended")
	}
	return MaintenanceWindow{From: from.UTC(), Until: until.UTC()}, nil
}

func (s Station) AcceptsWaveform(start, end time.Time) error {
	if s.Status != StatusActive {
		return fmt.Errorf("%w: station %s is %s", fault.ErrInvalidState, s.Code, s.Status)
	}
	if s.MaintenanceFrom != nil && s.MaintenanceUntil != nil && start.Before(*s.MaintenanceUntil) && end.After(*s.MaintenanceFrom) {
		return fmt.Errorf("%w: waveform overlaps station maintenance", fault.ErrConflict)
	}
	return nil
}
