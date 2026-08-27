package httpapi

import (
	"bytes"
	"net/http"
	"testing"
)

func TestRejectedStationRegistrationLeavesNoPartialNetworkState(t *testing.T) {
	fixture := newAPIFixture(t)
	token := loginToken(t, fixture, "admin@example.invalid", "StrongAdmin123")
	calibratedAt := fixture.clock.Now().AddDate(0, -1, 0)
	registration := func(code, serial string) map[string]any {
		return map[string]any{
			"code": code, "name": "Atomic Ridge " + code,
			"latitude": 30.2, "longitude": 103.8, "elevation_m": 880,
			"timezone": "Asia/Shanghai", "sensor_serial": serial,
			"sensor_channel": "BHZ", "sensor_sample_rate_hz": 100,
			"installed_at": calibratedAt.AddDate(-1, 0, 0), "calibrated_at": calibratedAt,
		}
	}

	response, body := requestJSON(t, fixture.server.Client(), http.MethodPost,
		fixture.server.URL+"/v1/stations", token, registration("AT01", "SN-ATOMIC-001"))
	if response.StatusCode != http.StatusCreated {
		t.Fatalf("initial registration status = %d, body = %s", response.StatusCode, body)
	}

	response, body = requestJSON(t, fixture.server.Client(), http.MethodPost,
		fixture.server.URL+"/v1/stations", token, registration("AT02", "SN-ATOMIC-001"))
	if response.StatusCode != http.StatusConflict {
		t.Fatalf("conflicting registration status = %d, body = %s", response.StatusCode, body)
	}
	response, body = requestJSON(t, fixture.server.Client(), http.MethodGet,
		fixture.server.URL+"/v1/stations?search=AT02", token, nil)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("list after conflict status = %d, body = %s", response.StatusCode, body)
	}
	if bytes.Contains(body, []byte(`"code":"AT02"`)) {
		t.Fatalf("failed registration leaked a station: %s", body)
	}

	response, body = requestJSON(t, fixture.server.Client(), http.MethodPost,
		fixture.server.URL+"/v1/stations", token, registration("AT02", "SN-ATOMIC-002"))
	if response.StatusCode != http.StatusCreated || !bytes.Contains(body, []byte(`"code":"AT02"`)) {
		t.Fatalf("valid retry status = %d, body = %s", response.StatusCode, body)
	}
}
