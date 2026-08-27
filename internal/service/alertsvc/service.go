package alertsvc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
	"github.com/vance1852/quakewatch-control-plane/internal/service/shared"
)

type Sender interface {
	Send(context.Context, alert.Rule, alert.Delivery, any) error
}

type HTTPStatusError struct {
	StatusCode int
	Body       string
}

func (e HTTPStatusError) Error() string {
	return fmt.Sprintf("alert destination returned HTTP %d: %s", e.StatusCode, e.Body)
}

func (e HTTPStatusError) Permanent() bool {
	return e.StatusCode >= 400 && e.StatusCode < 500 && e.StatusCode != http.StatusTooManyRequests
}

type HTTPSender struct {
	client *http.Client
}

func NewHTTPSender(client *http.Client) *HTTPSender {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &HTTPSender{client: client}
}

func (s *HTTPSender) Send(ctx context.Context, rule alert.Rule, delivery alert.Delivery, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("encode alert payload: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, rule.Destination, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create alert request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", delivery.ID)
	response, err := s.client.Do(request)
	if err != nil {
		return fmt.Errorf("send alert request: %w", err)
	}
	defer response.Body.Close()
	limited, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return HTTPStatusError{StatusCode: response.StatusCode, Body: strings.TrimSpace(string(limited))}
	}
	return nil
}

type Service struct {
	store  repository.Store
	tx     repository.Transactor
	clock  clock.Clock
	ids    idgen.Generator
	sender Sender
	rules  *alert.RuleSnapshotCache
}

func New(store repository.Store, tx repository.Transactor, valueClock clock.Clock, ids idgen.Generator, sender Sender) *Service {
	return &Service{store: store, tx: tx, clock: valueClock, ids: ids, sender: sender, rules: alert.NewRuleSnapshotCache()}
}

func (s *Service) CreateRule(ctx context.Context, principal auth.Principal, input alert.Rule) (alert.Rule, error) {
	if err := principal.Require(auth.PermissionManageAlerts); err != nil {
		return alert.Rule{}, err
	}
	validated, err := alert.ValidateRule(input)
	if err != nil {
		return alert.Rule{}, err
	}
	now := s.clock.Now()
	validated.ID = s.ids.New("rul")
	validated.Version = 1
	validated.CreatedAt = now
	validated.UpdatedAt = now
	err = s.tx.WithinTx(ctx, func(store repository.Store) error {
		if err := store.CreateAlertRule(ctx, validated); err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "alert_rule.created", "alert_rule", validated.ID, "success", map[string]any{
			"name":              validated.Name,
			"minimum_magnitude": validated.MinimumMagnitude,
			"enabled":           validated.Enabled,
		}, now)
	})
	return validated, err
}

func (s *Service) UpdateRule(ctx context.Context, principal auth.Principal, input alert.Rule, version int64) (alert.Rule, error) {
	if err := principal.Require(auth.PermissionManageAlerts); err != nil {
		return alert.Rule{}, err
	}
	validated, err := alert.ValidateRule(input)
	if err != nil {
		return alert.Rule{}, err
	}
	now := s.clock.Now()
	var updated alert.Rule
	err = s.tx.WithinTx(ctx, func(store repository.Store) error {
		var txErr error
		updated, txErr = store.UpdateAlertRule(ctx, validated, version, now)
		if txErr != nil {
			return txErr
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "alert_rule.updated", "alert_rule", input.ID, "success", map[string]any{
			"version": updated.Version,
			"enabled": updated.Enabled,
		}, now)
	})
	if err == nil {
		// Invalidate any previously cached snapshot so the next delivery reads
		// the freshly committed destination instead of a stale value.
		s.rules.Put(updated)
	}
	return updated, err
}

func (s *Service) ListRules(ctx context.Context, filter repository.AlertFilter) (repository.Page[alert.Rule], error) {
	return s.store.ListAlertRules(ctx, filter)
}

func (s *Service) Deliver(ctx context.Context, delivery alert.Delivery) error {
	rule, found := s.rules.Get(delivery.RuleID)
	if !found {
		var err error
		rule, err = s.store.GetAlertRule(ctx, delivery.RuleID)
		if err != nil {
			return err
		}
		s.rules.Put(rule)
	}
	eventValue, err := s.store.GetEvent(ctx, delivery.EventID)
	if err != nil {
		return err
	}
	payload := map[string]any{
		"delivery_id": delivery.ID,
		"event": map[string]any{
			"id":          eventValue.PublicID,
			"detected_at": eventValue.DetectedAt,
			"latitude":    eventValue.Latitude,
			"longitude":   eventValue.Longitude,
			"depth_km":    eventValue.DepthKM,
			"magnitude":   eventValue.Magnitude,
			"status":      eventValue.Status,
		},
	}
	return s.sender.Send(ctx, rule, delivery, payload)
}

func IsPermanent(err error) bool {
	type permanent interface{ Permanent() bool }
	var value permanent
	return errors.As(err, &value) && value.Permanent()
}

func (s *Service) ValidateDeliveryLease(delivery alert.Delivery, owner string) error {
	if err := delivery.ValidateOwner(owner, s.clock.Now()); err != nil {
		return fault.Wrap("validate delivery lease", err)
	}
	return nil
}
