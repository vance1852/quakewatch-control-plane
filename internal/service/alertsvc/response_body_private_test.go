package alertsvc

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
)

type responseCloseTracker struct {
	io.Reader
	closed bool
}

func (b *responseCloseTracker) Close() error {
	b.closed = true
	return nil
}

type responseRoundTripFunc func(*http.Request) (*http.Response, error)

func (f responseRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestFailedAlertResponseAlwaysReleasesBody(t *testing.T) {
	body := &responseCloseTracker{Reader: strings.NewReader("destination unavailable")}
	client := &http.Client{Transport: responseRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: body, Header: make(http.Header), Request: request}, nil
	})}
	sender := NewHTTPSender(client)

	err := sender.Send(context.Background(), alert.Rule{Destination: "https://alerts.example.invalid/hook"}, alert.Delivery{ID: "del_retry"}, map[string]any{"event": "evt_503"})
	var statusErr HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("Send() error = %v; want HTTPStatusError 503", err)
	}
	if !body.closed {
		t.Fatal("failed webhook response body remained open after Send returned")
	}
}
