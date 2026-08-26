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
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type reviewTimeStore struct {
	repository.Store
	candidate   event.Candidate
	decideCalls int
}

func (s *reviewTimeStore) GetEvent(context.Context, string) (event.Candidate, error) {
	return s.candidate, nil
}

func (s *reviewTimeStore) CountDistinctPickStations(context.Context, string) (int, error) {
	return 3, nil
}

func (s *reviewTimeStore) ClaimEvent(_ context.Context, _, owner string, leaseUntil time.Time, _ int64, now time.Time) (event.Candidate, error) {
	s.candidate.Status = event.StatusUnderReview
	s.candidate.ReviewOwnerID = &owner
	s.candidate.ReviewLeaseUntil = &leaseUntil
	s.candidate.Version++
	s.candidate.UpdatedAt = now
	return s.candidate, nil
}

func (s *reviewTimeStore) CreateDecision(context.Context, event.ReviewDecision) error { return nil }

func (s *reviewTimeStore) DecideEvent(_ context.Context, _ string, status event.Status, _ int64, now time.Time) (event.Candidate, error) {
	s.decideCalls++
	s.candidate.Status = status
	s.candidate.Version++
	s.candidate.UpdatedAt = now
	return s.candidate, nil
}

func (s *reviewTimeStore) MatchingAlertRules(context.Context, alert.EventEnvelope) ([]alert.Rule, error) {
	return nil, nil
}

func (s *reviewTimeStore) CreateAudit(context.Context, audit.Event) error { return nil }

type reviewTimeTx struct{ store repository.Store }

func (t reviewTimeTx) WithinTx(ctx context.Context, fn func(repository.Store) error) error {
	return fn(t.store)
}

func TestDecisionRejectsLeaseExpiredAfterEarlierClaim(t *testing.T) {
	now := time.Date(2026, 8, 27, 5, 0, 0, 0, time.UTC)
	valueClock := clock.NewFake(now)
	store := &reviewTimeStore{candidate: event.Candidate{ID: "evt-1", Status: event.StatusDetected, Version: 1}}
	service := New(store, reviewTimeTx{store: store}, valueClock, &idgen.Sequence{}, 5*time.Minute)
	principal := auth.Principal{UserID: "analyst-1", Role: auth.RoleAnalyst}

	claimed, err := service.Claim(context.Background(), principal, "evt-1", 1)
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	valueClock.Advance(10 * time.Minute)
	_, err = service.Decide(context.Background(), principal, "evt-1", event.DecisionConfirm, "confirmed after review", claimed.Version)
	if !errors.Is(err, fault.ErrLeaseLost) {
		t.Fatalf("Decide() error = %v; want lease lost after clock advances past lease", err)
	}
	if store.decideCalls != 0 {
		t.Fatalf("DecideEvent() calls = %d; want 0 for expired lease", store.decideCalls)
	}
}
