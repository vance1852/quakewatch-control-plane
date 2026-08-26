package alert

import (
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

func validRule() Rule {
	return Rule{
		Name: "Regional M4", MinimumMagnitude: 4,
		MinLatitude: 20, MaxLatitude: 50, MinLongitude: 90, MaxLongitude: 130,
		Destination: "https://alerts.example.invalid/earthquakes", Enabled: true,
	}
}

func TestValidateRule(t *testing.T) {
	t.Parallel()
	if _, err := ValidateRule(validRule()); err != nil {
		t.Fatalf("valid rule error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*Rule)
	}{
		{name: "short name", mutate: func(value *Rule) { value.Name = "x" }},
		{name: "magnitude low", mutate: func(value *Rule) { value.MinimumMagnitude = -3 }},
		{name: "latitude reversed", mutate: func(value *Rule) { value.MinLatitude = 60 }},
		{name: "longitude reversed", mutate: func(value *Rule) { value.MinLongitude = 140 }},
		{name: "http destination", mutate: func(value *Rule) { value.Destination = "http://alerts.example.invalid" }},
		{name: "relative destination", mutate: func(value *Rule) { value.Destination = "/alerts" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := validRule()
			test.mutate(&value)
			if _, err := ValidateRule(value); !errors.Is(err, fault.ErrValidation) {
				t.Fatalf("ValidateRule() error = %v, want validation", err)
			}
		})
	}
}

func TestRuleMatches(t *testing.T) {
	t.Parallel()
	rule := validRule()
	tests := []struct {
		name  string
		event EventEnvelope
		want  bool
	}{
		{name: "inside", event: EventEnvelope{Magnitude: 4.5, Latitude: 31, Longitude: 110}, want: true},
		{name: "boundary", event: EventEnvelope{Magnitude: 4, Latitude: 20, Longitude: 130}, want: true},
		{name: "magnitude below", event: EventEnvelope{Magnitude: 3.9, Latitude: 31, Longitude: 110}},
		{name: "latitude outside", event: EventEnvelope{Magnitude: 5, Latitude: 51, Longitude: 110}},
		{name: "longitude outside", event: EventEnvelope{Magnitude: 5, Latitude: 31, Longitude: 140}},
	}
	for _, test := range tests {
		if got := rule.Matches(test.event); got != test.want {
			t.Errorf("%s Matches() = %v, want %v", test.name, got, test.want)
		}
	}
	rule.Enabled = false
	if rule.Matches(EventEnvelope{Magnitude: 8, Latitude: 30, Longitude: 100}) {
		t.Fatal("disabled rule matched")
	}
}

func TestDeliveryLease(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 1, 0, 0, 0, time.UTC)
	pending := Delivery{Status: StatusPending, NextAttemptAt: now}
	if err := pending.CanLease("worker-a", now); err != nil {
		t.Fatalf("pending lease error = %v", err)
	}
	owner := "worker-b"
	lease := now.Add(time.Minute)
	leased := Delivery{Status: StatusLeased, NextAttemptAt: now, LeaseOwner: &owner, LeaseUntil: &lease}
	if err := leased.CanLease("worker-a", now); !errors.Is(err, fault.ErrConflict) {
		t.Fatalf("active lease error = %v", err)
	}
	if err := leased.ValidateOwner(owner, now); err != nil {
		t.Fatalf("ValidateOwner() error = %v", err)
	}
	if err := leased.ValidateOwner("worker-a", now); !errors.Is(err, fault.ErrLeaseLost) {
		t.Fatalf("wrong owner error = %v", err)
	}
	if err := leased.ValidateOwner(owner, lease); !errors.Is(err, fault.ErrLeaseLost) {
		t.Fatalf("expired owner error = %v", err)
	}
}

func TestBackoff(t *testing.T) {
	t.Parallel()
	wants := []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second}
	for index, want := range wants {
		if got := Backoff(index + 1); got != want {
			t.Errorf("Backoff(%d) = %v, want %v", index+1, got, want)
		}
	}
	if got := Backoff(0); got != 5*time.Second {
		t.Errorf("Backoff(0) = %v", got)
	}
	if got := Backoff(100); got != 2560*time.Second {
		t.Errorf("Backoff(100) = %v", got)
	}
}
