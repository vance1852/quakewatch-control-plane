package repository

import (
	"context"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/station"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
)

type Store interface {
	AuthStore
	StationStore
	WaveformStore
	EventStore
	AlertStore
	JobStore
	AuditStore
	IdempotencyStore
}

type Transactor interface {
	WithinTx(context.Context, func(Store) error) error
}

type Database interface {
	Store
	Transactor
	Ping(context.Context) error
	Close() error
}

type Page[T any] struct {
	Items      []T    `json:"items"`
	NextCursor string `json:"next_cursor,omitempty"`
}

type AuthStore interface {
	CreateUser(context.Context, auth.User) error
	GetUserByID(context.Context, string) (auth.User, error)
	GetUserByEmail(context.Context, string) (auth.User, error)
	UpdateUserRole(context.Context, string, auth.Role, int64, time.Time) (auth.User, error)
	SetUserActive(context.Context, string, bool, int64, time.Time) (auth.User, error)
	CreateSession(context.Context, auth.Session) error
	GetSessionByHash(context.Context, string) (auth.Session, auth.User, error)
	TouchSession(context.Context, string, time.Time) error
	RevokeSession(context.Context, string, time.Time) error
	DeleteExpiredSessions(context.Context, time.Time, int) (int64, error)
}

type StationFilter struct {
	Status station.Status
	Search string
	After  string
	Limit  int
}

type StationStore interface {
	CreateStation(context.Context, station.Station) error
	ReserveStationIdentity(context.Context, station.Station) error
	CreateSensor(context.Context, station.Sensor) error
	GetStation(context.Context, string) (station.Station, error)
	GetStationByCode(context.Context, string) (station.Station, error)
	ListStations(context.Context, StationFilter) (Page[station.Station], error)
	ListSensors(context.Context, string, bool) ([]station.Sensor, error)
	GetSensor(context.Context, string) (station.Sensor, error)
	UpdateStationState(context.Context, string, station.Status, *time.Time, *time.Time, int64, time.Time) (station.Station, error)
	UpdateStationCoordinates(context.Context, string, float64, float64, float64, int64, time.Time) (station.Station, error)
	DisableSensor(context.Context, string, int64, time.Time) (station.Sensor, error)
	CountEnabledSensors(context.Context, string) (int, time.Time, error)
}

type WaveformFilter struct {
	StationID string
	SensorID  string
	Status    waveform.Status
	From      *time.Time
	Until     *time.Time
	After     string
	Limit     int
}

type WaveformStore interface {
	CreateWaveform(context.Context, waveform.Batch) error
	GetWaveform(context.Context, string) (waveform.Batch, error)
	GetWaveformBySource(context.Context, string, string) (waveform.Batch, error)
	ListWaveforms(context.Context, WaveformFilter) (Page[waveform.Batch], error)
	UpdateWaveformStatus(context.Context, string, waveform.Status, string, int64, time.Time) (waveform.Batch, error)
	HasWaveformOverlap(context.Context, string, time.Time, time.Time) (bool, error)
}

type EventFilter struct {
	Status    event.Status
	Magnitude *float64
	From      *time.Time
	Until     *time.Time
	After     string
	Limit     int
}

type EventStore interface {
	CreateEvent(context.Context, event.Candidate) error
	CreatePick(context.Context, event.Pick) error
	GetEvent(context.Context, string) (event.Candidate, error)
	GetEventByPublicID(context.Context, string) (event.Candidate, error)
	ListEvents(context.Context, EventFilter) (Page[event.Candidate], error)
	ListPicks(context.Context, string) ([]event.Pick, error)
	CountDistinctPickStations(context.Context, string) (int, error)
	ClaimEvent(context.Context, string, string, time.Time, int64, time.Time) (event.Candidate, error)
	CreateDecision(context.Context, event.ReviewDecision) error
	DecideEvent(context.Context, string, event.Status, int64, time.Time) (event.Candidate, error)
	PublishEvent(context.Context, string, int64, time.Time) (event.Candidate, error)
}

type AlertFilter struct {
	Enabled *bool
	After   string
	Limit   int
}

type AlertStore interface {
	CreateAlertRule(context.Context, alert.Rule) error
	GetAlertRule(context.Context, string) (alert.Rule, error)
	ListAlertRules(context.Context, AlertFilter) (Page[alert.Rule], error)
	UpdateAlertRule(context.Context, alert.Rule, int64, time.Time) (alert.Rule, error)
	MatchingAlertRules(context.Context, alert.EventEnvelope) ([]alert.Rule, error)
	CreateDelivery(context.Context, alert.Delivery) error
	GetDelivery(context.Context, string) (alert.Delivery, error)
	LeaseDeliveries(context.Context, string, time.Time, time.Time, int) ([]alert.Delivery, error)
	CompleteDelivery(context.Context, string, string, int64, time.Time) error
	FailDelivery(context.Context, string, string, int64, bool, time.Time, string) error
}

type JobStore interface {
	CreateJob(context.Context, job.Job) error
	GetJob(context.Context, string) (job.Job, error)
	LeaseJobs(context.Context, string, time.Time, time.Time, int) ([]job.Job, error)
	CompleteJob(context.Context, string, string, int64, time.Time) error
	FailJob(context.Context, string, string, int64, bool, time.Time, string) error
	RecoverExpiredJobs(context.Context, time.Time) (int64, error)
}

type AuditStore interface {
	CreateAudit(context.Context, audit.Event) error
	ListAudit(context.Context, audit.Query) (Page[audit.Event], error)
}

type IdempotencyRecord struct {
	ID           string
	ActorID      string
	Method       string
	Path         string
	Key          string
	RequestHash  string
	ResponseCode *int
	ResponseJSON string
	ExpiresAt    time.Time
	CreatedAt    time.Time
}

type IdempotencyStore interface {
	CreateIdempotency(context.Context, IdempotencyRecord) error
	GetIdempotency(context.Context, string, string, string, string) (IdempotencyRecord, error)
	CompleteIdempotency(context.Context, string, int, string) error
	DeleteIdempotency(context.Context, string) error
	DeleteExpiredIdempotency(context.Context, time.Time, int) (int64, error)
}
