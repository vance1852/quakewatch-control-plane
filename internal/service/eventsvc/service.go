package eventsvc

import (
	"context"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
	"github.com/vance1852/quakewatch-control-plane/internal/service/shared"
)

type Service struct {
	store       repository.Store
	tx          repository.Transactor
	clock       clock.Clock
	ids         idgen.Generator
	reviewLease time.Duration
}

type Detail struct {
	Event event.Candidate `json:"event"`
	Picks []event.Pick    `json:"picks"`
}

func New(store repository.Store, tx repository.Transactor, valueClock clock.Clock, ids idgen.Generator, reviewLease time.Duration) *Service {
	return &Service{store: store, tx: tx, clock: valueClock, ids: ids, reviewLease: reviewLease}
}

func (s *Service) Detect(ctx context.Context, principal auth.Principal, input event.DetectionInput) (Detail, error) {
	if err := principal.Require(auth.PermissionIngest); err != nil {
		return Detail{}, err
	}
	validated, err := event.ValidateDetection(input, s.clock.Now())
	if err != nil {
		return Detail{}, err
	}
	now := s.clock.Now()
	candidate := event.Candidate{
		ID:         s.ids.New("evt"),
		PublicID:   validated.PublicID,
		DetectedAt: validated.DetectedAt,
		Latitude:   validated.Latitude,
		Longitude:  validated.Longitude,
		DepthKM:    validated.DepthKM,
		Magnitude:  validated.Magnitude,
		Status:     event.StatusDetected,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	picks := make([]event.Pick, len(validated.Picks))
	for _, inputPick := range validated.Picks {
		if err := ctx.Err(); err != nil {
			return Detail{}, err
		}
		batch, err := s.store.GetWaveform(ctx, inputPick.WaveformID)
		if err != nil {
			return Detail{}, err
		}
		if err := event.ValidatePickSource(inputPick, batch); err != nil {
			return Detail{}, err
		}
	}
	err = s.tx.WithinTx(ctx, func(store repository.Store) error {
		if err := store.CreateEvent(ctx, candidate); err != nil {
			return err
		}
		for index, inputPick := range validated.Picks {
			if err := ctx.Err(); err != nil {
				return err
			}
			inputPick.ID = s.ids.New("pik")
			inputPick.EventID = candidate.ID
			inputPick.CreatedAt = now
			if err := store.CreatePick(ctx, inputPick); err != nil {
				return err
			}
			picks[index] = inputPick
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "event.detected", "event", candidate.ID, "success", map[string]any{
			"public_id":  candidate.PublicID,
			"pick_count": len(picks),
			"magnitude":  candidate.Magnitude,
		}, now)
	})
	if err != nil {
		return Detail{}, fault.Wrap("detect event transaction", err)
	}
	return Detail{Event: candidate, Picks: picks}, nil
}

func (s *Service) Get(ctx context.Context, id string) (Detail, error) {
	candidate, err := s.store.GetEvent(ctx, id)
	if err != nil {
		return Detail{}, err
	}
	picks, err := s.store.ListPicks(ctx, id)
	if err != nil {
		return Detail{}, err
	}
	return Detail{Event: candidate, Picks: picks}, nil
}

func (s *Service) List(ctx context.Context, filter repository.EventFilter) (repository.Page[event.Candidate], error) {
	return s.store.ListEvents(ctx, filter)
}

func (s *Service) Claim(ctx context.Context, principal auth.Principal, eventID string, version int64) (event.Candidate, error) {
	if err := principal.Require(auth.PermissionReviewEvents); err != nil {
		return event.Candidate{}, err
	}
	now := s.clock.Now()
	var claimed event.Candidate
	err := s.tx.WithinTx(ctx, func(store repository.Store) error {
		current, err := store.GetEvent(ctx, eventID)
		if err != nil {
			return err
		}
		if current.Version != version {
			return fault.ErrVersion
		}
		stations, err := store.CountDistinctPickStations(ctx, eventID)
		if err != nil {
			return err
		}
		if err := current.CanClaim(principal.UserID, now, stations); err != nil {
			return err
		}
		claimed, err = store.ClaimEvent(ctx, eventID, principal.UserID, now.Add(s.reviewLease), version, now)
		if err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "event.review_claimed", "event", eventID, "success", map[string]any{
			"lease_until": claimed.ReviewLeaseUntil,
		}, now)
	})
	return claimed, err
}

func (s *Service) Decide(ctx context.Context, principal auth.Principal, eventID string, decision event.Decision, notes string, version int64) (event.Candidate, error) {
	if err := principal.Require(auth.PermissionReviewEvents); err != nil {
		return event.Candidate{}, err
	}
	now := s.clock.Now()
	var updated event.Candidate
	err := s.tx.WithinTx(ctx, func(store repository.Store) error {
		current, err := store.GetEvent(ctx, eventID)
		if err != nil {
			return err
		}
		if current.Version != version {
			return fault.ErrVersion
		}
		if err := current.CanDecide(principal.UserID, now, decision, notes); err != nil {
			return err
		}
		record := event.ReviewDecision{
			ID:           s.ids.New("rev"),
			EventID:      eventID,
			AnalystID:    principal.UserID,
			Decision:     decision,
			Notes:        notes,
			EventVersion: version,
			CreatedAt:    now,
		}
		if err := store.CreateDecision(ctx, record); err != nil {
			return err
		}
		next := event.StatusDismissed
		if decision == event.DecisionConfirm {
			next = event.StatusConfirmed
		}
		updated, err = store.DecideEvent(ctx, eventID, next, version, now)
		if err != nil {
			return err
		}
		createdDeliveries := 0
		if next == event.StatusConfirmed {
			rules, err := store.MatchingAlertRules(ctx, alert.EventEnvelope{
				EventID: eventID, Magnitude: current.Magnitude, Latitude: current.Latitude, Longitude: current.Longitude,
			})
			if err != nil {
				return err
			}
			for _, rule := range rules {
				delivery := alert.Delivery{
					ID:            s.ids.New("del"),
					EventID:       eventID,
					RuleID:        rule.ID,
					Status:        alert.StatusPending,
					NextAttemptAt: now,
					Version:       1,
					CreatedAt:     now,
					UpdatedAt:     now,
				}
				if err := store.CreateDelivery(ctx, delivery); err != nil {
					return err
				}
				createdDeliveries++
			}
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "event.review_decided", "event", eventID, "success", map[string]any{
			"decision":       decision,
			"delivery_count": createdDeliveries,
			"event_version":  version,
		}, now)
	})
	return updated, fault.Wrap("decide event transaction", err)
}

func (s *Service) Publish(ctx context.Context, principal auth.Principal, eventID string, version int64) (event.Candidate, error) {
	if err := principal.Require(auth.PermissionPublishEvents); err != nil {
		return event.Candidate{}, err
	}
	now := s.clock.Now()
	var published event.Candidate
	err := s.tx.WithinTx(ctx, func(store repository.Store) error {
		current, err := store.GetEvent(ctx, eventID)
		if err != nil {
			return err
		}
		if current.Version != version {
			return fault.ErrVersion
		}
		if err := current.CanPublish(); err != nil {
			return err
		}
		published, err = store.PublishEvent(ctx, eventID, version, now)
		if err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "event.published", "event", eventID, "success", map[string]any{
			"public_id": published.PublicID,
			"magnitude": published.Magnitude,
		}, now)
	})
	return published, err
}
