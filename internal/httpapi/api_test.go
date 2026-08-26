package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/service/alertsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/auditsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/authsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/eventsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/idempotencysvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/stationsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/service/waveformsvc"
	"github.com/vance1852/quakewatch-control-plane/internal/storage/sqlite"
)

type apiFixture struct {
	server   *httptest.Server
	database *sqlite.DB
	auth     *authsvc.Service
	clock    *clock.Fake
	admin    auth.User
}

func newAPIFixture(t *testing.T) apiFixture {
	t.Helper()
	now := time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC)
	valueClock := clock.NewFake(now)
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "api.db"))
	if err != nil {
		t.Fatalf("sqlite.Open() error = %v", err)
	}
	ids := &idgen.Sequence{}
	authService := authsvc.New(database, database, valueClock, ids, time.Hour)
	stationService := stationsvc.New(database, database, valueClock, ids)
	waveformService := waveformsvc.New(database, database, valueClock, ids)
	eventService := eventsvc.New(database, database, valueClock, ids, 15*time.Minute)
	alertService := alertsvc.New(database, database, valueClock, ids, alertsvc.NewHTTPSender(nil))
	admin, err := authService.Bootstrap(context.Background(), "admin@example.invalid", "StrongAdmin123")
	if err != nil {
		database.Close()
		t.Fatalf("Bootstrap() error = %v", err)
	}
	api := New(Services{
		Auth:        authService,
		Stations:    stationService,
		Waveforms:   waveformService,
		Events:      eventService,
		Alerts:      alertService,
		Audit:       auditsvc.New(database),
		Idempotency: idempotencysvc.New(database, database, valueClock, ids, time.Hour),
	}, database, slog.New(slog.NewTextHandler(io.Discard, nil)), ids, 1<<20)
	server := httptest.NewServer(api.Handler())
	t.Cleanup(func() {
		server.Close()
		if err := database.Close(); err != nil {
			t.Errorf("database.Close() error = %v", err)
		}
	})
	return apiFixture{server: server, database: database, auth: authService, clock: valueClock, admin: admin}
}

func requestJSON(t *testing.T, client *http.Client, method, url, token string, body any) (*http.Response, []byte) {
	t.Helper()
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("json.Marshal() error = %v", err)
		}
		reader = bytes.NewReader(encoded)
	}
	request, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("http.NewRequest() error = %v", err)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("client.Do() error = %v", err)
	}
	content, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}
	return response, content
}

func loginToken(t *testing.T, fixture apiFixture, email, password string) string {
	t.Helper()
	response, body := requestJSON(t, fixture.server.Client(), http.MethodPost, fixture.server.URL+"/v1/auth/login", "", map[string]string{
		"email":    email,
		"password": password,
	})
	if response.StatusCode != http.StatusOK {
		t.Fatalf("login status = %d, body = %s", response.StatusCode, body)
	}
	var result struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode login: %v", err)
	}
	if result.Token == "" {
		t.Fatal("login token is empty")
	}
	return result.Token
}

func TestHealthAndReadiness(t *testing.T) {
	fixture := newAPIFixture(t)
	for _, path := range []string{"/healthz", "/readyz"} {
		response, body := requestJSON(t, fixture.server.Client(), http.MethodGet, fixture.server.URL+path, "", nil)
		if response.StatusCode != http.StatusOK {
			t.Errorf("GET %s status = %d, body = %s", path, response.StatusCode, body)
		}
		if response.Header.Get("X-Request-ID") == "" {
			t.Errorf("GET %s missing X-Request-ID", path)
		}
		if !json.Valid(body) {
			t.Errorf("GET %s invalid JSON: %s", path, body)
		}
	}
}

func TestAuthenticationRequiredAndStableErrorShape(t *testing.T) {
	fixture := newAPIFixture(t)
	response, body := requestJSON(t, fixture.server.Client(), http.MethodGet, fixture.server.URL+"/v1/stations", "", nil)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d, body = %s", response.StatusCode, body)
	}
	var result errorResponse
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if result.Error.Code != "unauthorized" || result.Error.RequestID == "" {
		t.Fatalf("error response = %#v", result)
	}
	if result.Error.RequestID != response.Header.Get("X-Request-ID") {
		t.Fatalf("request id mismatch: body=%s header=%s", result.Error.RequestID, response.Header.Get("X-Request-ID"))
	}
}

func TestLoginStationLifecycleAndLogoutRevocation(t *testing.T) {
	fixture := newAPIFixture(t)
	token := loginToken(t, fixture, "admin@example.invalid", "StrongAdmin123")
	registeredAt := fixture.clock.Now().AddDate(0, -1, 0)
	response, body := requestJSON(t, fixture.server.Client(), http.MethodPost, fixture.server.URL+"/v1/stations", token, map[string]any{
		"code":                  "sc01",
		"name":                  "Sichuan Ridge",
		"latitude":              30.2,
		"longitude":             103.8,
		"elevation_m":           880,
		"timezone":              "Asia/Shanghai",
		"sensor_serial":         "SN-SC-001",
		"sensor_channel":        "BHZ",
		"sensor_sample_rate_hz": 100,
		"installed_at":          registeredAt.AddDate(-1, 0, 0),
		"calibrated_at":         registeredAt,
	})
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("register status = %d, body = %s", response.StatusCode, body)
	}
	var detail struct {
		Station struct {
			ID      string `json:"id"`
			Code    string `json:"code"`
			Version int64  `json:"version"`
			Status  string `json:"status"`
		} `json:"station"`
		Sensors []struct {
			ID string `json:"id"`
		} `json:"sensors"`
	}
	if err := json.Unmarshal(body, &detail); err != nil {
		t.Fatalf("decode station: %v", err)
	}
	if detail.Station.Code != "SC01" || detail.Station.Status != "provisioning" || len(detail.Sensors) != 1 {
		t.Fatalf("registered detail = %#v", detail)
	}
	response, body = requestJSON(t, fixture.server.Client(), http.MethodPost,
		fixture.server.URL+"/v1/stations/"+detail.Station.ID+"/activate", token,
		map[string]any{"version": detail.Station.Version})
	if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"status":"active"`)) {
		t.Fatalf("activate status = %d, body = %s", response.StatusCode, body)
	}
	response, body = requestJSON(t, fixture.server.Client(), http.MethodGet, fixture.server.URL+"/v1/stations?status=active", token, nil)
	if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"code":"SC01"`)) {
		t.Fatalf("list status = %d, body = %s", response.StatusCode, body)
	}
	response, body = requestJSON(t, fixture.server.Client(), http.MethodPost, fixture.server.URL+"/v1/auth/logout", token, nil)
	if response.StatusCode != http.StatusOK || !bytes.Contains(body, []byte(`"revoked":true`)) {
		t.Fatalf("logout status = %d, body = %s", response.StatusCode, body)
	}
	response, body = requestJSON(t, fixture.server.Client(), http.MethodGet, fixture.server.URL+"/v1/stations", token, nil)
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked token status = %d, body = %s", response.StatusCode, body)
	}
}

func TestRoleAuthorizationRejectsAnalystNetworkMutation(t *testing.T) {
	fixture := newAPIFixture(t)
	adminPrincipal := auth.Principal{UserID: fixture.admin.ID, Role: auth.RoleAdmin}
	_, err := fixture.auth.CreateUser(context.Background(), adminPrincipal, authsvc.CreateUserInput{
		Email: "analyst@example.invalid", DisplayName: "Duty Analyst",
		Password: "AnalystStrong123", Role: auth.RoleAnalyst,
	})
	if err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	token := loginToken(t, fixture, "analyst@example.invalid", "AnalystStrong123")
	response, body := requestJSON(t, fixture.server.Client(), http.MethodPost, fixture.server.URL+"/v1/stations", token, map[string]any{
		"code": "NO01",
	})
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("analyst station status = %d, body = %s", response.StatusCode, body)
	}
	if !bytes.Contains(body, []byte(`"code":"forbidden"`)) {
		t.Fatalf("forbidden body = %s", body)
	}
}

func TestLoginValidationAndUnknownFields(t *testing.T) {
	fixture := newAPIFixture(t)
	response, body := requestJSON(t, fixture.server.Client(), http.MethodPost, fixture.server.URL+"/v1/auth/login", "", map[string]any{
		"email":      "admin@example.invalid",
		"password":   "StrongAdmin123",
		"unexpected": true,
	})
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d, body = %s", response.StatusCode, body)
	}
	if !strings.Contains(string(body), "unknown field") {
		t.Fatalf("unknown field body = %s", body)
	}
	response, body = requestJSON(t, fixture.server.Client(), http.MethodPost, fixture.server.URL+"/v1/auth/login", "", map[string]string{
		"email":    "admin@example.invalid",
		"password": "wrong-password",
	})
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad password status = %d, body = %s", response.StatusCode, body)
	}
}
