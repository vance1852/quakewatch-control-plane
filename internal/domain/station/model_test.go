package station

import (
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

func validRegisterInput() RegisterInput {
	installed := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return RegisterInput{
		Code: "bj01", Name: "Beijing Ridge", Latitude: 39.9, Longitude: 116.4,
		ElevationM: 42, Timezone: "Asia/Shanghai", SensorSerial: "sn-2026-001",
		SensorChannel: "bhz", SensorSampleRate: 100, InstalledAt: installed,
		CalibratedAt: installed.Add(24 * time.Hour),
	}
}

func TestValidateRegister(t *testing.T) {
	t.Parallel()
	t.Run("normalizes identifiers", func(t *testing.T) {
		input := validRegisterInput()
		got, err := ValidateRegister(input)
		if err != nil {
			t.Fatalf("ValidateRegister() error = %v", err)
		}
		if got.Code != "BJ01" || got.SensorChannel != "BHZ" || got.SensorSerial != "SN-2026-001" {
			t.Fatalf("normalized values = %#v", got)
		}
	})
	tests := []struct {
		name   string
		mutate func(*RegisterInput)
	}{
		{name: "invalid code", mutate: func(input *RegisterInput) { input.Code = "?" }},
		{name: "short name", mutate: func(input *RegisterInput) { input.Name = "x" }},
		{name: "latitude high", mutate: func(input *RegisterInput) { input.Latitude = 91 }},
		{name: "longitude low", mutate: func(input *RegisterInput) { input.Longitude = -181 }},
		{name: "elevation high", mutate: func(input *RegisterInput) { input.ElevationM = 10000 }},
		{name: "invalid timezone", mutate: func(input *RegisterInput) { input.Timezone = "Mars/Olympus" }},
		{name: "short serial", mutate: func(input *RegisterInput) { input.SensorSerial = "abc" }},
		{name: "invalid channel", mutate: func(input *RegisterInput) { input.SensorChannel = "?" }},
		{name: "sample rate low", mutate: func(input *RegisterInput) { input.SensorSampleRate = 0 }},
		{name: "sample rate high", mutate: func(input *RegisterInput) { input.SensorSampleRate = 10001 }},
		{name: "calibration before install", mutate: func(input *RegisterInput) { input.CalibratedAt = input.InstalledAt.Add(-time.Second) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := validRegisterInput()
			test.mutate(&input)
			if _, err := ValidateRegister(input); !errors.Is(err, fault.ErrValidation) {
				t.Fatalf("ValidateRegister() error = %v, want validation", err)
			}
		})
	}
}

func TestStationTransitions(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	recent := now.AddDate(0, -1, 0)
	old := now.AddDate(-1, 0, 0)
	tests := []struct {
		name       string
		current    Status
		next       Status
		sensors    int
		calibrated time.Time
		valid      bool
	}{
		{name: "provisioning activates", current: StatusProvisioning, next: StatusActive, sensors: 1, calibrated: recent, valid: true},
		{name: "activation requires sensor", current: StatusProvisioning, next: StatusActive, calibrated: recent},
		{name: "activation requires recent calibration", current: StatusProvisioning, next: StatusActive, sensors: 1, calibrated: old},
		{name: "active enters maintenance", current: StatusActive, next: StatusMaintenance, valid: true},
		{name: "maintenance returns active", current: StatusMaintenance, next: StatusActive, valid: true},
		{name: "active retires", current: StatusActive, next: StatusRetired, valid: true},
		{name: "retired is terminal", current: StatusRetired, next: StatusActive},
		{name: "same status rejected", current: StatusActive, next: StatusActive},
		{name: "provisioning cannot enter maintenance", current: StatusProvisioning, next: StatusMaintenance},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := Station{Status: test.current, Code: "BJ01"}
			err := value.CanTransition(test.next, test.sensors, test.calibrated, now)
			if test.valid && err != nil {
				t.Fatalf("CanTransition() error = %v", err)
			}
			if !test.valid && !errors.Is(err, fault.ErrInvalidState) {
				t.Fatalf("CanTransition() error = %v, want invalid state", err)
			}
		})
	}
}

func TestMaintenanceWindow(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC)
	window, err := ValidateMaintenance(now.Add(time.Hour), now.Add(2*time.Hour), now)
	if err != nil {
		t.Fatalf("ValidateMaintenance() error = %v", err)
	}
	if window.Until.Sub(window.From) != time.Hour {
		t.Fatalf("window duration = %v", window.Until.Sub(window.From))
	}
	invalid := [][2]time.Time{
		{now, now},
		{now.Add(time.Hour), now},
		{now.Add(-2 * time.Hour), now.Add(-time.Hour)},
		{now.Add(time.Hour), now.Add(31 * 24 * time.Hour)},
	}
	for _, values := range invalid {
		if _, err := ValidateMaintenance(values[0], values[1], now); !errors.Is(err, fault.ErrValidation) {
			t.Errorf("ValidateMaintenance(%v,%v) error = %v", values[0], values[1], err)
		}
	}
}

func TestAcceptsWaveform(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	active := Station{Status: StatusActive, Code: "BJ01"}
	if err := active.AcceptsWaveform(start, end); err != nil {
		t.Fatalf("active station rejected waveform: %v", err)
	}
	maintenanceFrom := start.Add(30 * time.Minute)
	maintenanceUntil := start.Add(90 * time.Minute)
	active.MaintenanceFrom = &maintenanceFrom
	active.MaintenanceUntil = &maintenanceUntil
	if err := active.AcceptsWaveform(start, end); !errors.Is(err, fault.ErrConflict) {
		t.Fatalf("overlap error = %v, want conflict", err)
	}
	retired := Station{Status: StatusRetired, Code: "OLD1"}
	if err := retired.AcceptsWaveform(start, end); !errors.Is(err, fault.ErrInvalidState) {
		t.Fatalf("retired error = %v, want invalid state", err)
	}
}
