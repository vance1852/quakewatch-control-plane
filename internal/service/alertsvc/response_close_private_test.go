package alertsvc

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
)

type privateRoundTripper func(*http.Request) (*http.Response, error)

func (f privateRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

type failingResponseBody struct {
	err    error
	closed bool
}

func (b *failingResponseBody) Read([]byte) (int, error) { return 0, b.err }
func (b *failingResponseBody) Close() error {
	b.closed = true
	return nil
}

func TestAlertResponseBodyClosesWhenReadFails(t *testing.T) {
	readErr := errors.New("upstream response reset")
	body := &failingResponseBody{err: readErr}
	client := &http.Client{Transport: privateRoundTripper(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusBadGateway, Body: body, Header: make(http.Header)}, nil
	})}
	sender := NewHTTPSender(client)

	err := sender.Send(context.Background(), alert.Rule{Destination: "https://alerts.example.test/hook"}, alert.Delivery{ID: "delivery-30"}, map[string]string{"event": "quake-30"})
	if !errors.Is(err, readErr) {
		t.Fatalf("Send() error = %v; want response read error", err)
	}
	if !body.closed {
		t.Fatal("response body remained open after read failure")
	}
}
