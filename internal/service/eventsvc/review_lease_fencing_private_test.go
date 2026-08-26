package eventsvc

import (
	"context"
	"errors"
	"sync"
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

type reviewLeaseFencingStore struct {
	repository.Store
	mu              sync.Mutex
	current         event.Candidate
	rules           []alert.Rule
	decisions       []event.ReviewDecision
	deliveries      []alert.Delivery
	snapshotRead    chan struct{}
	continueRequest chan struct{}
	readOnce        sync.Once
}

func (s *reviewLeaseFencingStore) GetEvent(ctx context.Context, id string) (event.Candidate, error) {
	s.mu.Lock()
	if id != s.current.ID {
		s.mu.Unlock()
		return event.Candidate{}, fault.ErrNotFound
	}
	snapshot := s.current
	s.mu.Unlock()
	wait := false
	s.readOnce.Do(func() {
		wait = true
		close(s.snapshotRead)
	})
	if wait {
		select {
		case <-s.continueRequest:
		case <-ctx.Done():
			return event.Candidate{}, ctx.Err()
		}
	}
	return snapshot, nil
}

func (s *reviewLeaseFencingStore) CreateDecision(_ context.Context, value event.ReviewDecision) error {
	s.mu.Lock()
	s.decisions = append(s.decisions, value)
	s.mu.Unlock()
	return nil
}

func (s *reviewLeaseFencingStore) DecideEvent(_ context.Context, id string, status event.Status, version int64, now time.Time) (event.Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current.ID != id || s.current.Version != version || s.current.Status != event.StatusUnderReview {
		return event.Candidate{}, fault.ErrVersion
	}
	s.current.Status = status
	s.current.ReviewOwnerID = nil
	s.current.ReviewLeaseUntil = nil
	s.current.Version++
	s.current.UpdatedAt = now
	return s.current, nil
}

func (s *reviewLeaseFencingStore) ApplyDecisionAuthorization(_ context.Context, authorization event.DecisionAuthorization, status event.Status, now time.Time) (event.Candidate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current.ID != authorization.EventID || s.current.Status != event.StatusUnderReview {
		return event.Candidate{}, fault.ErrConflict
	}
	s.current.Status = status
	s.current.ReviewOwnerID = nil
	s.current.ReviewLeaseUntil = nil
	s.current.Version++
	s.current.UpdatedAt = now
	return s.current, nil
}

func (s *reviewLeaseFencingStore) MatchingAlertRules(context.Context, alert.EventEnvelope) ([]alert.Rule, error) {
	return append([]alert.Rule(nil), s.rules...), nil
}

func (s *reviewLeaseFencingStore) CreateDelivery(_ context.Context, value alert.Delivery) error {
	s.mu.Lock()
	s.deliveries = append(s.deliveries, value)
	s.mu.Unlock()
	return nil
}

func (s *reviewLeaseFencingStore) CreateAudit(context.Context, audit.Event) error {
	return nil
}

func (s *reviewLeaseFencingStore) WithinTx(ctx context.Context, operation func(repository.Store) error) error {
	s.mu.Lock()
	beforeEvent := s.current
	beforeDecisions := append([]event.ReviewDecision(nil), s.decisions...)
	beforeDeliveries := append([]alert.Delivery(nil), s.deliveries...)
	s.mu.Unlock()
	if err := operation(s); err != nil {
		s.mu.Lock()
		s.current = beforeEvent
		s.decisions = beforeDecisions
		s.deliveries = beforeDeliveries
		s.mu.Unlock()
		return err
	}
	return nil
}

func TestReviewDecisionRejectsAuthorizationFromExpiredLeaseGeneration(t *testing.T) {
	now := time.Date(2026, 8, 26, 13, 0, 0, 0, time.UTC)
	analystA := "usr_analyst_a"
	analystB := "usr_analyst_b"
	oldLease := now.Add(time.Minute)
	newLease := now.Add(10 * time.Minute)
	store := &reviewLeaseFencingStore{
		current: event.Candidate{
			ID: "evt_review_handoff", PublicID: "QUAKE-REVIEW-HANDOFF", Status: event.StatusUnderReview,
			Magnitude: 6.4, Latitude: 31.2, Longitude: 103.8, ReviewOwnerID: &analystA,
			ReviewLeaseUntil: &oldLease, Version: 2, CreatedAt: now.Add(-time.Hour), UpdatedAt: now,
		},
		rules: []alert.Rule{{
			ID: "rule_critical", Enabled: true, MinimumMagnitude: 5,
			MinLatitude: -90, MaxLatitude: 90, MinLongitude: -180, MaxLongitude: 180,
		}},
		snapshotRead:    make(chan struct{}),
		continueRequest: make(chan struct{}),
	}
	service := New(store, store, clock.NewFake(now), &idgen.Sequence{}, 15*time.Minute)
	principal := auth.Principal{UserID: analystA, SessionID: "ses_a", Role: auth.RoleAnalyst}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := make(chan error, 1)
	go func() {
		_, err := service.Decide(ctx, principal, store.current.ID, event.DecisionConfirm, "confirmed after waveform review", 2)
		result <- err
	}()

	select {
	case <-store.snapshotRead:
	case <-ctx.Done():
		t.Fatalf("decision request did not read the original review lease: %v", ctx.Err())
	}
	store.mu.Lock()
	store.current.ReviewOwnerID = &analystB
	store.current.ReviewLeaseUntil = &newLease
	store.current.Version = 3
	store.current.UpdatedAt = now.Add(2 * time.Minute)
	store.mu.Unlock()
	close(store.continueRequest)

	err := <-result
	if err == nil || (!errors.Is(err, fault.ErrVersion) && !errors.Is(err, fault.ErrConflict) && !errors.Is(err, fault.ErrLeaseLost)) {
		t.Fatalf("stale review authorization error = %v; want version, conflict, or lease-lost rejection", err)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.current.Status != event.StatusUnderReview || store.current.Version != 3 || store.current.ReviewOwnerID == nil || *store.current.ReviewOwnerID != analystB {
		t.Fatalf("event after stale decision = %#v; want analyst B's version 3 review lease unchanged", store.current)
	}
	if len(store.decisions) != 0 || len(store.deliveries) != 0 {
		t.Fatalf("stale decision created %d review decisions and %d alert deliveries", len(store.decisions), len(store.deliveries))
	}
}
