package config

import (
	"strings"
	"testing"
	"time"
)

func clearConfigEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"QUAKEWATCH_HTTP_ADDR", "QUAKEWATCH_DATABASE_PATH", "QUAKEWATCH_SESSION_TTL",
		"QUAKEWATCH_WORKER_POLL_INTERVAL", "QUAKEWATCH_WORKER_LEASE_DURATION",
		"QUAKEWATCH_WORKER_JOB_TIMEOUT", "QUAKEWATCH_SHUTDOWN_TIMEOUT",
		"QUAKEWATCH_LOG_LEVEL", "QUAKEWATCH_BOOTSTRAP_EMAIL",
		"QUAKEWATCH_BOOTSTRAP_PASSWORD", "QUAKEWATCH_MAX_REQUEST_BYTES",
		"QUAKEWATCH_REVIEW_LEASE_DURATION",
	} {
		t.Setenv(name, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearConfigEnvironment(t)
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.HTTPAddr != ":8080" || config.DatabasePath != "./data/quakewatch.db" {
		t.Fatalf("default address/path = %q %q", config.HTTPAddr, config.DatabasePath)
	}
	if config.SessionTTL != 12*time.Hour || config.WorkerPoll != 500*time.Millisecond {
		t.Fatalf("default durations = %#v", config)
	}
	if config.WorkerLease <= config.WorkerJobTimeout {
		t.Fatalf("lease %v must exceed timeout %v", config.WorkerLease, config.WorkerJobTimeout)
	}
	if config.MaxRequestBytes != 1<<20 {
		t.Fatalf("MaxRequestBytes = %d", config.MaxRequestBytes)
	}
}

func TestLoadEnvironmentOverrides(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("QUAKEWATCH_HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("QUAKEWATCH_DATABASE_PATH", "./custom.db")
	t.Setenv("QUAKEWATCH_SESSION_TTL", "2h")
	t.Setenv("QUAKEWATCH_WORKER_POLL_INTERVAL", "200ms")
	t.Setenv("QUAKEWATCH_WORKER_LEASE_DURATION", "45s")
	t.Setenv("QUAKEWATCH_WORKER_JOB_TIMEOUT", "20s")
	t.Setenv("QUAKEWATCH_SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("QUAKEWATCH_LOG_LEVEL", "debug")
	t.Setenv("QUAKEWATCH_MAX_REQUEST_BYTES", "2048")
	t.Setenv("QUAKEWATCH_REVIEW_LEASE_DURATION", "20m")
	config, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if config.HTTPAddr != "127.0.0.1:9090" || config.DatabasePath != "./custom.db" {
		t.Fatalf("override address/path = %q %q", config.HTTPAddr, config.DatabasePath)
	}
	if config.SessionTTL != 2*time.Hour || config.WorkerPoll != 200*time.Millisecond {
		t.Fatalf("override durations = %#v", config)
	}
	if config.ReviewLease != 20*time.Minute || config.MaxRequestBytes != 2048 {
		t.Fatalf("review/max overrides = %#v", config)
	}
}

func TestLoadRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		value    string
		contains string
	}{
		{name: "invalid duration", key: "QUAKEWATCH_SESSION_TTL", value: "later", contains: "positive duration"},
		{name: "session too short", key: "QUAKEWATCH_SESSION_TTL", value: "1s", contains: "one minute"},
		{name: "poll too short", key: "QUAKEWATCH_WORKER_POLL_INTERVAL", value: "1ms", contains: "10ms"},
		{name: "unknown log level", key: "QUAKEWATCH_LOG_LEVEL", value: "verbose", contains: "debug"},
		{name: "request too small", key: "QUAKEWATCH_MAX_REQUEST_BYTES", value: "100", contains: "between"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clearConfigEnvironment(t)
			t.Setenv(test.key, test.value)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("Load() error = %v, want containing %q", err, test.contains)
			}
		})
	}
}

func TestBootstrapCredentialsMustBePaired(t *testing.T) {
	clearConfigEnvironment(t)
	t.Setenv("QUAKEWATCH_BOOTSTRAP_EMAIL", "admin@example.invalid")
	if _, err := Load(); err == nil {
		t.Fatal("Load() unexpectedly accepted unpaired bootstrap email")
	}
	clearConfigEnvironment(t)
	t.Setenv("QUAKEWATCH_BOOTSTRAP_PASSWORD", "StrongAdmin123")
	if _, err := Load(); err == nil {
		t.Fatal("Load() unexpectedly accepted unpaired bootstrap password")
	}
}
