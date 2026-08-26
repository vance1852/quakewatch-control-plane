package httpapi

import (
	"log/slog"
	"net/http"

	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
	"github.com/vance1852/quakewatch-control-plane/internal/service/alertsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/auditsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/authsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/eventsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/idempotencysvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/stationsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/waveformsvc"
)

type Services struct {
	Auth        *authsvc.Service
	Stations    *stationsvc.Service
	Waveforms   *waveformsvc.Service
	Events      *eventsvc.Service
	Alerts      *alertsvc.Service
	Audit       *auditsvc.Service
	Idempotency *idempotencysvc.Service
}

type API struct {
	services        Services
	database        repository.Database
	logger          *slog.Logger
	ids             idgen.Generator
	maxRequestBytes int64
	mux             *http.ServeMux
}

func New(services Services, database repository.Database, logger *slog.Logger, ids idgen.Generator, maxRequestBytes int64) *API {
	api := &API{
		services:        services,
		database:        database,
		logger:          logger,
		ids:             ids,
		maxRequestBytes: maxRequestBytes,
		mux:             http.NewServeMux(),
	}
	api.routes()
	return api
}

func (a *API) Handler() http.Handler {
	return a.requestID(a.recoverPanic(a.requestLog(a.limitBody(a.mux))))
}

func (a *API) routes() {
	a.mux.HandleFunc("GET /healthz", a.health)
	a.mux.HandleFunc("GET /readyz", a.ready)
	a.mux.HandleFunc("POST /v1/auth/login", a.login)
	a.mux.Handle("POST /v1/auth/logout", a.authenticated(http.HandlerFunc(a.logout)))
	a.mux.Handle("GET /v1/stations", a.authenticated(http.HandlerFunc(a.listStations)))
	a.mux.Handle("POST /v1/stations", a.authenticated(http.HandlerFunc(a.registerStation)))
	a.mux.Handle("GET /v1/stations/{station_id}", a.authenticated(http.HandlerFunc(a.getStation)))
	a.mux.Handle("POST /v1/stations/{station_id}/activate", a.authenticated(http.HandlerFunc(a.activateStation)))
	a.mux.Handle("POST /v1/stations/{station_id}/maintenance", a.authenticated(http.HandlerFunc(a.maintainStation)))
	a.mux.Handle("POST /v1/stations/{station_id}/retire", a.authenticated(http.HandlerFunc(a.retireStation)))
	a.mux.Handle("GET /v1/waveforms", a.authenticated(http.HandlerFunc(a.listWaveforms)))
	a.mux.Handle("POST /v1/waveforms", a.authenticated(http.HandlerFunc(a.ingestWaveform)))
	a.mux.Handle("GET /v1/waveforms/{waveform_id}", a.authenticated(http.HandlerFunc(a.getWaveform)))
	a.mux.Handle("POST /v1/waveforms/{waveform_id}/reject", a.authenticated(http.HandlerFunc(a.rejectWaveform)))
	a.mux.Handle("GET /v1/events", a.authenticated(http.HandlerFunc(a.listEvents)))
	a.mux.Handle("POST /v1/events", a.authenticated(http.HandlerFunc(a.detectEvent)))
	a.mux.Handle("GET /v1/events/{event_id}", a.authenticated(http.HandlerFunc(a.getEvent)))
	a.mux.Handle("POST /v1/events/{event_id}/claim", a.authenticated(http.HandlerFunc(a.claimEvent)))
	a.mux.Handle("POST /v1/events/{event_id}/decision", a.authenticated(http.HandlerFunc(a.decideEvent)))
	a.mux.Handle("POST /v1/events/{event_id}/publish", a.authenticated(http.HandlerFunc(a.publishEvent)))
	a.mux.Handle("GET /v1/alert-rules", a.authenticated(http.HandlerFunc(a.listAlertRules)))
	a.mux.Handle("POST /v1/alert-rules", a.authenticated(http.HandlerFunc(a.createAlertRule)))
	a.mux.Handle("PUT /v1/alert-rules/{rule_id}", a.authenticated(http.HandlerFunc(a.updateAlertRule)))
	a.mux.Handle("GET /v1/audit-events", a.authenticated(http.HandlerFunc(a.listAudit)))
}
