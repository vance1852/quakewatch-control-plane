package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/requestmeta"
)

type delayedResponseWriter struct {
	header  http.Header
	status  int
	entered chan struct{}
	release chan struct{}
	body    []byte
}

func (w *delayedResponseWriter) Header() http.Header    { return w.header }
func (w *delayedResponseWriter) WriteHeader(status int) { w.status = status }
func (w *delayedResponseWriter) Write(value []byte) (int, error) {
	close(w.entered)
	<-w.release
	w.body = append(w.body, value...)
	return len(value), nil
}

func TestConcurrentErrorsKeepRequestResponseBytesIsolated(t *testing.T) {
	first := &delayedResponseWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	firstRequest := httptest.NewRequest(http.MethodGet, "/v1/events/missing", nil)
	firstRequest = firstRequest.WithContext(requestmeta.WithRequestID(firstRequest.Context(), "request-one"))
	firstDone := make(chan struct{})
	go func() {
		writeError(first, firstRequest, fault.ErrNotFound)
		close(firstDone)
	}()
	<-first.entered

	second := httptest.NewRecorder()
	secondRequest := httptest.NewRequest(http.MethodGet, "/v1/stations/missing", nil)
	secondRequest = secondRequest.WithContext(requestmeta.WithRequestID(secondRequest.Context(), "request-two"))
	writeError(second, secondRequest, fault.ErrNotFound)
	close(first.release)
	<-firstDone

	var firstEnvelope errorResponse
	if err := json.Unmarshal(first.body, &firstEnvelope); err != nil {
		t.Fatalf("decode first response: %v, body=%q", err, first.body)
	}
	if firstEnvelope.Error.RequestID != "request-one" {
		t.Fatalf("first response request_id = %q, want request-one", firstEnvelope.Error.RequestID)
	}
	var secondEnvelope errorResponse
	if err := json.Unmarshal(second.Body.Bytes(), &secondEnvelope); err != nil {
		t.Fatalf("decode second response: %v", err)
	}
	if secondEnvelope.Error.RequestID != "request-two" {
		t.Fatalf("second response request_id = %q, want request-two", secondEnvelope.Error.RequestID)
	}
}
