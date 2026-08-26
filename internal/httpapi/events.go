package httpapi

import (
	"fmt"
	"net/http"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

func (a *API) getEvent(writer http.ResponseWriter, request *http.Request) {
	result, err := a.services.Events.Get(request.Context(), request.PathValue("event_id"))
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) detectEvent(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body struct {
		PublicID   string       `json:"public_id"`
		DetectedAt time.Time    `json:"detected_at"`
		Latitude   float64      `json:"latitude"`
		Longitude  float64      `json:"longitude"`
		DepthKM    float64      `json:"depth_km"`
		Magnitude  float64      `json:"magnitude"`
		Picks      []event.Pick `json:"picks"`
	}
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if err == nil {
		result, serviceErr := a.services.Events.Detect(request.Context(), value, event.DetectionInput{
			PublicID:   body.PublicID,
			DetectedAt: body.DetectedAt,
			Latitude:   body.Latitude,
			Longitude:  body.Longitude,
			DepthKM:    body.DepthKM,
			Magnitude:  body.Magnitude,
			Picks:      body.Picks,
		})
		if serviceErr == nil {
			writeJSON(writer, http.StatusCreated, result)
			return
		}
		err = serviceErr
	}
	writeError(writer, request, err)
}

func (a *API) listEvents(writer http.ResponseWriter, request *http.Request) {
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
	var magnitude *float64
	if raw := request.URL.Query().Get("minimum_magnitude"); raw != "" {
		var parsed float64
		if _, err := fmt.Sscan(raw, &parsed); err != nil {
			writeError(writer, request, fault.Validation("minimum_magnitude", "must be numeric"))
			return
		}
		magnitude = &parsed
	}
	result, err := a.services.Events.List(request.Context(), repository.EventFilter{
		Status: event.Status(request.URL.Query().Get("status")), Magnitude: magnitude,
		From: from, Until: until, After: request.URL.Query().Get("after"), Limit: limit,
	})
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) claimEvent(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body struct {
		Version int64 `json:"version"`
	}
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if err == nil {
		var result event.Candidate
		result, err = a.services.Events.Claim(request.Context(), value, request.PathValue("event_id"), body.Version)
		if err == nil {
			writeJSON(writer, http.StatusOK, result)
			return
		}
	}
	writeError(writer, request, err)
}

func (a *API) decideEvent(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body struct {
		Version  int64          `json:"version"`
		Decision event.Decision `json:"decision"`
		Notes    string         `json:"notes"`
	}
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if err == nil {
		var result event.Candidate
		result, err = a.services.Events.Decide(request.Context(), value, request.PathValue("event_id"), body.Decision, body.Notes, body.Version)
		if err == nil {
			writeJSON(writer, http.StatusOK, result)
			return
		}
	}
	writeError(writer, request, err)
}

func (a *API) publishEvent(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body struct {
		Version int64 `json:"version"`
	}
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if err == nil {
		var result event.Candidate
		result, err = a.services.Events.Publish(request.Context(), value, request.PathValue("event_id"), body.Version)
		if err == nil {
			writeJSON(writer, http.StatusOK, result)
			return
		}
	}
	writeError(writer, request, err)
}
