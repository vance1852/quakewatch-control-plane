package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

type Config struct {
	HTTPAddr          string
	DatabasePath      string
	SessionTTL        time.Duration
	WorkerPoll        time.Duration
	WorkerLease       time.Duration
	WorkerJobTimeout  time.Duration
	ShutdownTimeout   time.Duration
	LogLevel          slog.Level
	BootstrapEmail    string
	BootstrapPassword string
	MaxRequestBytes   int64
	ReviewLease       time.Duration
}

func Load() (Config, error) {
	config := Config{
		HTTPAddr:          env("QUAKEWATCH_HTTP_ADDR", ":8080"),
		DatabasePath:      env("QUAKEWATCH_DATABASE_PATH", "./data/quakewatch.db"),
		BootstrapEmail:    strings.TrimSpace(os.Getenv("QUAKEWATCH_BOOTSTRAP_EMAIL")),
		BootstrapPassword: os.Getenv("QUAKEWATCH_BOOTSTRAP_PASSWORD"),
		MaxRequestBytes:   1 << 20,
	}
	var err error
	if config.SessionTTL, err = duration("QUAKEWATCH_SESSION_TTL", 12*time.Hour); err != nil {
		return Config{}, err
	}
	if config.WorkerPoll, err = duration("QUAKEWATCH_WORKER_POLL_INTERVAL", 500*time.Millisecond); err != nil {
		return Config{}, err
	}
	if config.WorkerLease, err = duration("QUAKEWATCH_WORKER_LEASE_DURATION", 30*time.Second); err != nil {
		return Config{}, err
	}
	if config.WorkerJobTimeout, err = duration("QUAKEWATCH_WORKER_JOB_TIMEOUT", 20*time.Second); err != nil {
		return Config{}, err
	}
	if config.ShutdownTimeout, err = duration("QUAKEWATCH_SHUTDOWN_TIMEOUT", 10*time.Second); err != nil {
		return Config{}, err
	}
	if config.ReviewLease, err = duration("QUAKEWATCH_REVIEW_LEASE_DURATION", 15*time.Minute); err != nil {
		return Config{}, err
	}
	if value := strings.TrimSpace(os.Getenv("QUAKEWATCH_MAX_REQUEST_BYTES")); value != "" {
		parsed, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil || parsed < 1024 || parsed > 16<<20 {
			return Config{}, fault.Validation("QUAKEWATCH_MAX_REQUEST_BYTES", "must be between 1024 and 16777216")
		}
		config.MaxRequestBytes = parsed
	}
	switch strings.ToLower(env("QUAKEWATCH_LOG_LEVEL", "info")) {
	case "debug":
		config.LogLevel = slog.LevelDebug
	case "info":
		config.LogLevel = slog.LevelInfo
	case "warn":
		config.LogLevel = slog.LevelWarn
	case "error":
		config.LogLevel = slog.LevelError
	default:
		return Config{}, fault.Validation("QUAKEWATCH_LOG_LEVEL", "must be debug, info, warn, or error")
	}
	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.HTTPAddr) == "" {
		return fault.Validation("QUAKEWATCH_HTTP_ADDR", "cannot be empty")
	}
	if strings.TrimSpace(c.DatabasePath) == "" {
		return fault.Validation("QUAKEWATCH_DATABASE_PATH", "cannot be empty")
	}
	if c.SessionTTL < time.Minute || c.SessionTTL > 30*24*time.Hour {
		return fault.Validation("QUAKEWATCH_SESSION_TTL", "must be between one minute and thirty days")
	}
	if c.WorkerPoll < 10*time.Millisecond || c.WorkerPoll > time.Minute {
		return fault.Validation("QUAKEWATCH_WORKER_POLL_INTERVAL", "must be between 10ms and one minute")
	}
	if c.WorkerLease <= c.WorkerJobTimeout {
		return fault.Validation("QUAKEWATCH_WORKER_LEASE_DURATION", "must exceed worker job timeout")
	}
	if c.ShutdownTimeout < time.Second || c.ShutdownTimeout > time.Minute {
		return fault.Validation("QUAKEWATCH_SHUTDOWN_TIMEOUT", "must be between one second and one minute")
	}
	if (c.BootstrapEmail == "") != (c.BootstrapPassword == "") {
		return fault.Validation("bootstrap", "email and password must be provided together")
	}
	return nil
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func duration(name string, fallback time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%w: %s must be a positive duration", fault.ErrValidation, name)
	}
	return parsed, nil
}
