package alertsvc

import (
	"context"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type ruleCacheStore struct {
	repository.Store
	rule alert.Rule
}

func (s *ruleCacheStore) WithinTx(ctx context.Context, operation func(repository.Store) error) error {
	return operation(s)
}
func (s *ruleCacheStore) GetAlertRule(context.Context, string) (alert.Rule, error) {
	return s.rule, nil
}
func (s *ruleCacheStore) UpdateAlertRule(_ context.Context, value alert.Rule, version int64, now time.Time) (alert.Rule, error) {
	value.Version = version + 1
	value.UpdatedAt = now
	s.rule = value
	return value, nil
}
func (s *ruleCacheStore) GetEvent(context.Context, string) (event.Candidate, error) {
	return event.Candidate{ID: "evt_cache", PublicID: "EQ-CACHE", Status: event.StatusConfirmed, Magnitude: 5.8}, nil
}
func (s *ruleCacheStore) CreateAudit(context.Context, audit.Event) error { return nil }

type destinationRecorder struct{ destinations []string }

func (s *destinationRecorder) Send(_ context.Context, rule alert.Rule, _ alert.Delivery, _ any) error {
	s.destinations = append(s.destinations, rule.Destination)
	return nil
}

func TestRuleUpdateChangesDestinationForNextDelivery(t *testing.T) {
	now := time.Date(2026, 8, 27, 5, 0, 0, 0, time.UTC)
	store := &ruleCacheStore{rule: alert.Rule{ID: "rule_cache", Name: "regional warning", MinimumMagnitude: 4, MinLatitude: -90, MaxLatitude: 90, MinLongitude: -180, MaxLongitude: 180, Destination: "https://old.example.invalid/hook", Enabled: true, Version: 1}}
	sender := &destinationRecorder{}
	service := New(store, store, clock.NewFake(now), &idgen.Sequence{}, sender)
	delivery := alert.Delivery{ID: "del_cache", EventID: "evt_cache", RuleID: "rule_cache"}
	if err := service.Deliver(context.Background(), delivery); err != nil {
		t.Fatalf("first Deliver() error = %v", err)
	}
	updated := store.rule
	updated.Destination = "https://new.example.invalid/hook"
	if _, err := service.UpdateRule(context.Background(), auth.Principal{UserID: "admin", Role: auth.RoleAdmin}, updated, 1); err != nil {
		t.Fatalf("UpdateRule() error = %v", err)
	}
	if err := service.Deliver(context.Background(), delivery); err != nil {
		t.Fatalf("second Deliver() error = %v", err)
	}
	if len(sender.destinations) != 2 || sender.destinations[1] != updated.Destination {
		t.Fatalf("delivery destinations = %v; want next delivery to use %q", sender.destinations, updated.Destination)
	}
}
