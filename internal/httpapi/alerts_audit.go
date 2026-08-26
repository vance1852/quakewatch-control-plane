package httpapi

import (
	"net/http"
	"strconv"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

func (a *API) createAlertRule(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body alert.Rule
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if err == nil {
		var result alert.Rule
		result, err = a.services.Alerts.CreateRule(request.Context(), value, body)
		if err == nil {
			writeJSON(writer, http.StatusCreated, result)
			return
		}
	}
	writeError(writer, request, err)
}

func (a *API) updateAlertRule(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	var body alert.Rule
	if err == nil {
		err = decodeJSON(request, &body)
	}
	if body.ID != "" && body.ID != request.PathValue("rule_id") {
		err = fault.Validation("id", "must match path rule_id")
	}
	body.ID = request.PathValue("rule_id")
	if err == nil {
		var result alert.Rule
		result, err = a.services.Alerts.UpdateRule(request.Context(), value, body, body.Version)
		if err == nil {
			writeJSON(writer, http.StatusOK, result)
			return
		}
	}
	writeError(writer, request, err)
}

func (a *API) listAlertRules(writer http.ResponseWriter, request *http.Request) {
	limit, err := queryInt(request, "limit", 50)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	var enabled *bool
	if raw := request.URL.Query().Get("enabled"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			writeError(writer, request, fault.Validation("enabled", "must be true or false"))
			return
		}
		enabled = &parsed
	}
	result, err := a.services.Alerts.ListRules(request.Context(), repository.AlertFilter{
		Enabled: enabled, After: request.URL.Query().Get("after"), Limit: limit,
	})
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) listAudit(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	limit, err := queryInt(request, "limit", 50)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	before, err := queryTime(request, "before")
	if err != nil {
		writeError(writer, request, err)
		return
	}
	result, err := a.services.Audit.List(request.Context(), value, audit.Query{
		ActorID: request.URL.Query().Get("actor_id"), RequestID: request.URL.Query().Get("request_id"),
		Object: request.URL.Query().Get("object"), Action: request.URL.Query().Get("action"),
		Before: before, Limit: limit,
	})
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}
