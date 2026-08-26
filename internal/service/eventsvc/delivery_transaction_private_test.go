package eventsvc

import (
	"context"
	"errors"
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

var errDecisionAudit = errors.New("decision audit unavailable")

type deliveryOuterStore struct {
	repository.Store
	escapedDeliveries int
}

func (s *deliveryOuterStore) CreateDelivery(context.Context, alert.Delivery) error {
	s.escapedDeliveries++
	return nil
}

type deliveryTransactionStore struct {
	repository.Store
	candidate     event.Candidate
	rules         []alert.Rule
	transactional int
}

func (s *deliveryTransactionStore) GetEvent(context.Context, string) (event.Candidate, error) {
	return s.candidate, nil
}

func (s *deliveryTransactionStore) CreateDecision(context.Context, event.ReviewDecision) error {
	return nil
}

func (s *deliveryTransactionStore) DecideEvent(_ context.Context, _ string, status event.Status, _ int64, now time.Time) (event.Candidate, error) {
	s.candidate.Status = status
	s.candidate.Version++
	s.candidate.UpdatedAt = now
	return s.candidate, nil
}

func (s *deliveryTransactionStore) MatchingAlertRules(context.Context, alert.EventEnvelope) ([]alert.Rule, error) {
	return append([]alert.Rule(nil), s.rules...), nil
}

func (s *deliveryTransactionStore) CreateDelivery(context.Context, alert.Delivery) error {
	s.transactional++
	return nil
}

func (s *deliveryTransactionStore) CreateAudit(context.Context, audit.Event) error {
	return errDecisionAudit
}

type deliveryTransaction struct{ store repository.Store }

func (t deliveryTransaction) WithinTx(ctx context.Context, fn func(repository.Store) error) error {
	return fn(t.store)
}

func TestConfirmedEventDoesNotEscapeDeliveryWhenDecisionRollsBack(t *testing.T) {
	now := time.Date(2026, 8, 27, 2, 0, 0, 0, time.UTC)
	owner := "analyst-1"
	leaseUntil := now.Add(10 * time.Minute)
	outer := &deliveryOuterStore{}
	txStore := &deliveryTransactionStore{
		candidate: event.Candidate{
			ID: "evt-1", Status: event.StatusUnderReview, ReviewOwnerID: &owner,
			ReviewLeaseUntil: &leaseUntil, Version: 1, Magnitude: 5.2,
		},
		rules: []alert.Rule{{ID: "rule-1", Enabled: true}},
	}
	service := New(outer, deliveryTransaction{store: txStore}, clock.NewFake(now), &idgen.Sequence{}, 15*time.Minute)

	_, err := service.Decide(context.Background(), auth.Principal{UserID: owner, Role: auth.RoleAnalyst}, "evt-1", event.DecisionConfirm, "confirmed by analyst", 1)
	if !errors.Is(err, errDecisionAudit) {
		t.Fatalf("Decide() error = %v; want audit failure", err)
	}
	if outer.escapedDeliveries != 0 {
		t.Fatalf("non-transactional deliveries = %d; want 0 after rollback", outer.escapedDeliveries)
	}
}
