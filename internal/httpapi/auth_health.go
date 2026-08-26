package httpapi

import (
	"net/http"
	"time"
)

func (a *API) health(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{
		"status": "alive",
		"time":   time.Now().UTC(),
	})
}

func (a *API) ready(writer http.ResponseWriter, request *http.Request) {
	ctx, cancel := timeoutContext(request, 2*time.Second)
	defer cancel()
	if err := a.database.Ping(ctx); err != nil {
		writeJSON(writer, http.StatusServiceUnavailable, map[string]any{
			"status":     "not_ready",
			"dependency": "database",
		})
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"status":   "ready",
		"database": "available",
	})
}

func (a *API) login(writer http.ResponseWriter, request *http.Request) {
	var body struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := decodeJSON(request, &body); err != nil {
		writeError(writer, request, err)
		return
	}
	result, err := a.services.Auth.Login(request.Context(), body.Email, body.Password)
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, result)
}

func (a *API) logout(writer http.ResponseWriter, request *http.Request) {
	value, err := principal(request)
	if err == nil {
		err = a.services.Auth.Logout(request.Context(), value)
	}
	if err != nil {
		writeError(writer, request, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]bool{"revoked": true})
}
