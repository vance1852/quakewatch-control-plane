package alertsvc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type payloadIsolationStore struct {
	repository.Store
	rules  map[string]alert.Rule
	events map[string]event.Candidate
}

func (s *payloadIsolationStore) GetAlertRule(_ context.Context, id string) (alert.Rule, error) {
	value, ok := s.rules[id]
	if !ok {
		return alert.Rule{}, fmt.Errorf("missing alert rule %s", id)
	}
	return value, nil
}

func (s *payloadIsolationStore) GetEvent(_ context.Context, id string) (event.Candidate, error) {
	value, ok := s.events[id]
	if !ok {
		return event.Candidate{}, fmt.Errorf("missing event %s", id)
	}
	return value, nil
}

type observedWebhook struct {
	deliveryID        string
	payloadDeliveryID string
	eventPublicID     string
	magnitude         float64
}

type synchronizedPayloadSender struct {
	firstEntered chan struct{}
	releaseFirst chan struct{}
	observed     chan observedWebhook
}

func newSynchronizedPayloadSender() *synchronizedPayloadSender {
	return &synchronizedPayloadSender{
		firstEntered: make(chan struct{}),
		releaseFirst: make(chan struct{}),
		observed:     make(chan observedWebhook, 2),
	}
}

func (s *synchronizedPayloadSender) Send(ctx context.Context, _ alert.Rule, delivery alert.Delivery, payload any) error {
	if delivery.ID == "del_alpha" {
		close(s.firstEntered)
		select {
		case <-s.releaseFirst:
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		select {
		case <-s.firstEntered:
		case <-ctx.Done():
			return ctx.Err()
		}
		defer close(s.releaseFirst)
	}
	root, ok := payload.(map[string]any)
	if !ok {
		return fmt.Errorf("webhook payload has type %T", payload)
	}
	eventPayload, ok := root["event"].(map[string]any)
	if !ok {
		return fmt.Errorf("event payload has type %T", root["event"])
	}
	s.observed <- observedWebhook{
		deliveryID:        delivery.ID,
		payloadDeliveryID: stringValue(root["delivery_id"]),
		eventPublicID:     stringValue(eventPayload["id"]),
		magnitude:         floatValue(eventPayload["magnitude"]),
	}
	return nil
}

func stringValue(value any) string {
	converted, _ := value.(string)
	return converted
}

func floatValue(value any) float64 {
	converted, _ := value.(float64)
	return converted
}

func TestConcurrentAlertDeliveriesKeepWebhookPayloadsIsolated(t *testing.T) {
	store := &payloadIsolationStore{
		rules: map[string]alert.Rule{
			"rule_alpha": {ID: "rule_alpha", Destination: "https://alpha.example.invalid/hook"},
			"rule_beta":  {ID: "rule_beta", Destination: "https://beta.example.invalid/hook"},
		},
		events: map[string]event.Candidate{
			"evt_alpha": {ID: "evt_alpha", PublicID: "QUAKE-ALPHA", Magnitude: 4.2, Status: event.StatusPublished},
			"evt_beta":  {ID: "evt_beta", PublicID: "QUAKE-BETA", Magnitude: 6.7, Status: event.StatusPublished},
		},
	}
	sender := newSynchronizedPayloadSender()
	service := New(store, nil, nil, nil, sender)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first := alert.Delivery{ID: "del_alpha", EventID: "evt_alpha", RuleID: "rule_alpha"}
	second := alert.Delivery{ID: "del_beta", EventID: "evt_beta", RuleID: "rule_beta"}
	firstResult := make(chan error, 1)
	go func() {
		firstResult <- service.Deliver(ctx, first)
	}()

	select {
	case <-sender.firstEntered:
	case <-ctx.Done():
		t.Fatalf("first webhook was not started: %v", ctx.Err())
	}
	if err := service.Deliver(ctx, second); err != nil {
		t.Fatalf("second Deliver() error = %v", err)
	}
	if err := <-firstResult; err != nil {
		t.Fatalf("first Deliver() error = %v", err)
	}

	want := map[string]observedWebhook{
		"del_alpha": {deliveryID: "del_alpha", payloadDeliveryID: "del_alpha", eventPublicID: "QUAKE-ALPHA", magnitude: 4.2},
		"del_beta":  {deliveryID: "del_beta", payloadDeliveryID: "del_beta", eventPublicID: "QUAKE-BETA", magnitude: 6.7},
	}
	for range 2 {
		got := <-sender.observed
		if got != want[got.deliveryID] {
			t.Errorf("webhook observed for %s = %#v, want %#v", got.deliveryID, got, want[got.deliveryID])
		}
	}
}
