package httpapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/requestmeta"
)

type responseRecorder struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (r *responseRecorder) WriteHeader(status int) {
	if r.status != 0 {
		return
	}
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (r *responseRecorder) Write(body []byte) (int, error) {
	if r.status == 0 {
		r.WriteHeader(http.StatusOK)
	}
	count, err := r.ResponseWriter.Write(body)
	r.bytes += count
	return count, err
}

func (a *API) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requestID := strings.TrimSpace(request.Header.Get("X-Request-ID"))
		if requestID == "" || len(requestID) > 128 {
			requestID = a.ids.New("req")
		}
		writer.Header().Set("X-Request-ID", requestID)
		ctx := requestmeta.WithRequestID(request.Context(), requestID)
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

func (a *API) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		started := time.Now()
		recorder := &responseRecorder{ResponseWriter: writer}
		next.ServeHTTP(recorder, request)
		if recorder.status == 0 {
			recorder.status = http.StatusOK
		}
		a.logger.InfoContext(request.Context(), "http request",
			slog.String("method", request.Method),
			slog.String("path", request.URL.Path),
			slog.Int("status", recorder.status),
			slog.Int("response_bytes", recorder.bytes),
			slog.Duration("duration", time.Since(started)),
			slog.String("request_id", requestmeta.RequestID(request.Context())),
		)
	})
}

func (a *API) recoverPanic(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				a.logger.ErrorContext(request.Context(), "http panic recovered",
					"panic", fmt.Sprint(recovered),
					"request_id", requestmeta.RequestID(request.Context()),
					"stack", string(debug.Stack()),
				)
				writeError(writer, request, fmt.Errorf("panic recovered: %v", recovered))
			}
		}()
		next.ServeHTTP(writer, request)
	})
}

func (a *API) limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Body != nil {
			request.Body = http.MaxBytesReader(writer, request.Body, a.maxRequestBytes)
		}
		next.ServeHTTP(writer, request)
	})
}

func (a *API) authenticated(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		header := strings.TrimSpace(request.Header.Get("Authorization"))
		prefix, token, found := strings.Cut(header, " ")
		if !found || !strings.EqualFold(prefix, "Bearer") || strings.TrimSpace(token) == "" {
			writeError(writer, request, fault.ErrUnauthorized)
			return
		}
		principal, err := a.services.Auth.Authenticate(request.Context(), strings.TrimSpace(token))
		if err != nil {
			writeError(writer, request, err)
			return
		}
		ctx := requestmeta.WithPrincipal(request.Context(), principal)
		next.ServeHTTP(writer, request.WithContext(ctx))
	})
}

func timeoutContext(request *http.Request, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(request.Context(), timeout)
}
