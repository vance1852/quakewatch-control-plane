package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

func (a *API) ingestWaveform(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	raw, err := io.ReadAll(request.Body)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	var body struct {
		StationID   string    `json:"station_id"`
		SensorID    string    `json:"sensor_id"`
		SourceKey   string    `json:"source_key"`
		StartsAt    time.Time `json:"starts_at"`
		EndsAt      time.Time `json:"ends_at"`
		SampleCount int64     `json:"sample_count"`
		Checksum    string    `json:"checksum"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&body); err != nil {
		writeError(writer, request, err)
		return
	}
	result, err := a.services.Idempotency.Execute(request.Context(), value, request.Method, request.URL.Path,
		request.Header.Get("Idempotency-Key"), raw, func(ctx context.Context) (int, any, error) {
			created, err := a.services.Waveforms.Ingest(ctx, value, waveform.IngestInput{
				StationID: body.StationID, SensorID: body.SensorID, SourceKey: body.SourceKey,
				StartsAt: body.StartsAt, EndsAt: body.EndsAt, SampleCount: body.SampleCount, Checksum: body.Checksum,
			})
			if err != nil {
				return 0, nil, err
			}
			status := http.StatusCreated
			if created.Reused {
				status = http.StatusOK
			}
			return status, created, nil
		})
	if err != nil {
		writeError(writer, request, err)
		return
	}
	if result.Reused {
		writer.Header().Set("Idempotency-Replayed", "true")
	}
	writeRawJSON(writer, result.Code, result.Body)
}

func (a *API) getWaveform(writer http.ResponseWriter, request *http.Request) {
	result, err := a.services.Waveforms.Get(request.Context(), request.PathValue("waveform_id"))
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) listWaveforms(writer http.ResponseWriter, request *http.Request) {
	limit, err := queryInt(request, "limit", 50)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	from, err := queryTime(request, "from")
	if err != nil {
		writeError(writer, request, err)
		return
	}
	until, err := queryTime(request, "until")
	if err != nil {
		writeError(writer, request, err)
		return
	}
	result, err := a.services.Waveforms.List(request.Context(), repository.WaveformFilter{
		StationID: request.URL.Query().Get("station_id"), SensorID: request.URL.Query().Get("sensor_id"),
		Status: waveform.Status(request.URL.Query().Get("status")), From: from, Until: until,
		After: request.URL.Query().Get("after"), Limit: limit,
	})
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) rejectWaveform(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body struct {
		Version int64  `json:"version"`
		Reason  string `json:"reason"`
	}
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if err == nil {
		var result waveform.Batch
		result, err = a.services.Waveforms.Reject(request.Context(), value, request.PathValue("waveform_id"), body.Reason, body.Version)
		if err == nil {
			writeJSON(writer, http.StatusOK, result)
			return
		}
	}
	writeError(writer, request, err)
}
