package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/requestmeta"
)

type sharedJSONEncoder struct {
	mu   sync.Mutex
	data []byte
}

func (e *sharedJSONEncoder) Encode(value any) []byte {
	encoded, _ := json.Marshal(value)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.data = append(e.data[:0], encoded...)
	e.data = append(e.data, '\n')
	return e.data
}

var responseEncoder sharedJSONEncoder

type errorResponse struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	if value != nil {
		_, _ = writer.Write(responseEncoder.Encode(value))
	}
}

func writeRawJSON(writer http.ResponseWriter, status int, body []byte) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_, _ = writer.Write(body)
}

func writeError(writer http.ResponseWriter, request *http.Request, err error) {
	status, code, message := classifyError(err)
	response := errorResponse{}
	response.Error.Code = code
	response.Error.Message = message
	response.Error.RequestID = requestmeta.RequestID(request.Context())
	writeJSON(writer, status, response)
}

func classifyError(err error) (int, string, string) {
	switch {
	case errors.Is(err, fault.ErrValidation):
		return http.StatusBadRequest, "validation_failed", err.Error()
	case errors.Is(err, fault.ErrUnauthorized), errors.Is(err, fault.ErrExpired):
		return http.StatusUnauthorized, "unauthorized", "authentication is required or has expired"
	case errors.Is(err, fault.ErrForbidden):
		return http.StatusForbidden, "forbidden", "the current role cannot perform this operation"
	case errors.Is(err, fault.ErrNotFound):
		return http.StatusNotFound, "not_found", "the requested resource was not found"
	case errors.Is(err, fault.ErrVersion):
		return http.StatusConflict, "version_conflict", "the resource changed; reload and retry"
	case errors.Is(err, fault.ErrConflict), errors.Is(err, fault.ErrAlreadyExists), errors.Is(err, fault.ErrInvalidState), errors.Is(err, fault.ErrLeaseLost):
		return http.StatusConflict, "conflict", err.Error()
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "timeout", "the operation exceeded its deadline"
	default:
		return http.StatusInternalServerError, "internal_error", "an internal error occurred"
	}
}

func decodeJSON(request *http.Request, target any) error {
	if request.Header.Get("Content-Type") != "" && !strings.HasPrefix(strings.ToLower(request.Header.Get("Content-Type")), "application/json") {
		return fault.Validation("Content-Type", "must be application/json")
	}
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fault.Validation("body", err.Error())
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fault.Validation("body", "must contain one JSON value")
	}
	return nil
}

func principal(request *http.Request) (auth.Principal, error) {
	value, ok := requestmeta.Principal(request.Context())
	if !ok {
		return auth.Principal{}, fault.ErrUnauthorized
	}
	return value, nil
}

func queryInt(request *http.Request, name string, fallback int) (int, error) {
	value := strings.TrimSpace(request.URL.Query().Get(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fault.Validation(name, "must be an integer")
	}
	return parsed, nil
}

func queryTime(request *http.Request, name string) (*time.Time, error) {
	value := strings.TrimSpace(request.URL.Query().Get(name))
	if value == "" {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, fault.Validation(name, "must be RFC3339")
	}
	parsed = parsed.UTC()
	return &parsed, nil
}
