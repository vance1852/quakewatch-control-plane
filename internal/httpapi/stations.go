package httpapi

import (
	"net/http"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/station"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

func (a *API) registerStation(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	var body struct {
		Code             string    `json:"code"`
		Name             string    `json:"name"`
		Latitude         float64   `json:"latitude"`
		Longitude        float64   `json:"longitude"`
		ElevationM       float64   `json:"elevation_m"`
		Timezone         string    `json:"timezone"`
		SensorSerial     string    `json:"sensor_serial"`
		SensorChannel    string    `json:"sensor_channel"`
		SensorSampleRate float64   `json:"sensor_sample_rate_hz"`
		InstalledAt      time.Time `json:"installed_at"`
		CalibratedAt     time.Time `json:"calibrated_at"`
	}
	if err := decodeJSON(request, &body); err != nil {
		writeError(writer, request, err)
		return
	}
	result, err := a.services.Stations.Register(request.Context(), value, station.RegisterInput{
		Code: body.Code, Name: body.Name, Latitude: body.Latitude, Longitude: body.Longitude,
		ElevationM: body.ElevationM, Timezone: body.Timezone, SensorSerial: body.SensorSerial,
		SensorChannel: body.SensorChannel, SensorSampleRate: body.SensorSampleRate,
		InstalledAt: body.InstalledAt, CalibratedAt: body.CalibratedAt,
	})
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusCreated, result)
}

func (a *API) getStation(writer http.ResponseWriter, request *http.Request) {
	result, err := a.services.Stations.Get(request.Context(), request.PathValue("station_id"))
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) listStations(writer http.ResponseWriter, request *http.Request) {
	limit, err := queryInt(request, "limit", 50)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	filter := repository.StationFilter{
		Status: station.Status(strings.TrimSpace(request.URL.Query().Get("status"))),
		Search: request.URL.Query().Get("search"),
		After:  request.URL.Query().Get("after"),
		Limit:  limit,
	}
	result, err := a.services.Stations.List(request.Context(), filter)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) activateStation(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body struct {
		Version int64 `json:"version"`
	}
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if err == nil {
		var result station.Station
		result, err = a.services.Stations.Activate(request.Context(), value, request.PathValue("station_id"), body.Version)
		if err == nil {
			writeJSON(writer, http.StatusOK, result)
			return
		}
	}
	writeError(writer, request, err)
}

func (a *API) maintainStation(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body struct {
		Version int64     `json:"version"`
		From    time.Time `json:"from"`
		Until   time.Time `json:"until"`
	}
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if err == nil {
		var result station.Station
		result, err = a.services.Stations.ScheduleMaintenance(request.Context(), value, request.PathValue("station_id"), body.Version, body.From, body.Until)
		if err == nil {
			writeJSON(writer, http.StatusOK, result)
			return
		}
	}
	writeError(writer, request, err)
}

func (a *API) retireStation(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body struct {
		Version int64 `json:"version"`
	}
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if err == nil {
		var result station.Station
		result, err = a.services.Stations.Retire(request.Context(), value, request.PathValue("station_id"), body.Version)
		if err == nil {
			writeJSON(writer, http.StatusOK, result)
			return
		}
	}
	writeError(writer, request, err)
}
