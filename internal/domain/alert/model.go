package alert

import (
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

type RuleSnapshotCache struct {
	mu    sync.RWMutex
	rules map[string]Rule
}

func NewRuleSnapshotCache() *RuleSnapshotCache {
	return &RuleSnapshotCache{rules: make(map[string]Rule)}
}

func (c *RuleSnapshotCache) Get(id string) (Rule, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	value, found := c.rules[id]
	return value, found
}

func (c *RuleSnapshotCache) Put(value Rule) {
	c.mu.Lock()
	c.rules[value.ID] = value
	c.mu.Unlock()
}

type Rule struct {
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	MinimumMagnitude float64   `json:"minimum_magnitude"`
	MinLatitude      float64   `json:"min_latitude"`
	MaxLatitude      float64   `json:"max_latitude"`
	MinLongitude     float64   `json:"min_longitude"`
	MaxLongitude     float64   `json:"max_longitude"`
	Destination      string    `json:"destination"`
	Enabled          bool      `json:"enabled"`
	Version          int64     `json:"version"`
	CreatedAt        time.Time `json:"created_at"`
	UpdatedAt        time.Time `json:"updated_at"`
}

type Status string

const (
	StatusPending   Status = "pending"
	StatusLeased    Status = "leased"
	StatusDelivered Status = "delivered"
	StatusRetryWait Status = "retry_wait"
	StatusDead      Status = "dead"
)

type Delivery struct {
	ID            string     `json:"id"`
	EventID       string     `json:"event_id"`
	RuleID        string     `json:"rule_id"`
	Status        Status     `json:"status"`
	AttemptCount  int        `json:"attempt_count"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LeaseOwner    *string    `json:"lease_owner,omitempty"`
	LeaseUntil    *time.Time `json:"lease_until,omitempty"`
	LastError     string     `json:"last_error,omitempty"`
	DeliveredAt   *time.Time `json:"delivered_at,omitempty"`
	Version       int64      `json:"version"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

type EventEnvelope struct {
	EventID   string
	Magnitude float64
	Latitude  float64
	Longitude float64
}

func ValidateRule(rule Rule) (Rule, error) {
	rule.Name = strings.TrimSpace(rule.Name)
	rule.Destination = strings.TrimSpace(rule.Destination)
	if len(rule.Name) < 3 || len(rule.Name) > 120 {
		return rule, fault.Validation("name", "must contain 3 to 120 characters")
	}
	if rule.MinimumMagnitude < -2 || rule.MinimumMagnitude > 10 {
		return rule, fault.Validation("minimum_magnitude", "must be between -2 and 10")
	}
	if rule.MinLatitude < -90 || rule.MaxLatitude > 90 || rule.MinLatitude > rule.MaxLatitude {
		return rule, fault.Validation("latitude", "bounds must be ordered within -90 and 90")
	}
	if rule.MinLongitude < -180 || rule.MaxLongitude > 180 || rule.MinLongitude > rule.MaxLongitude {
		return rule, fault.Validation("longitude", "bounds must be ordered within -180 and 180")
	}
	destination, err := url.ParseRequestURI(rule.Destination)
	if err != nil || destination.Scheme != "https" || destination.Host == "" {
		return rule, fault.Validation("destination", "must be an absolute HTTPS URL")
	}
	return rule, nil
}

func (r Rule) Matches(event EventEnvelope) bool {
	return r.Enabled && event.Magnitude >= r.MinimumMagnitude &&
		event.Latitude >= r.MinLatitude && event.Latitude <= r.MaxLatitude &&
		event.Longitude >= r.MinLongitude && event.Longitude <= r.MaxLongitude
}

func (d Delivery) CanLease(owner string, now time.Time) error {
	if strings.TrimSpace(owner) == "" {
		return fault.Validation("lease_owner", "is required")
	}
	if d.Status == StatusDelivered || d.Status == StatusDead {
		return fmt.Errorf("%w: terminal delivery cannot be leased", fault.ErrInvalidState)
	}
	if d.Status == StatusLeased && d.LeaseUntil != nil && now.Before(*d.LeaseUntil) {
		return fmt.Errorf("%w: delivery already leased", fault.ErrConflict)
	}
	if now.Before(d.NextAttemptAt) {
		return fmt.Errorf("%w: delivery not ready until %s", fault.ErrInvalidState, d.NextAttemptAt)
	}
	return nil
}

func (d Delivery) ValidateOwner(owner string, now time.Time) error {
	if d.Status != StatusLeased || d.LeaseOwner == nil || *d.LeaseOwner != owner {
		return fmt.Errorf("%w: delivery lease owner mismatch", fault.ErrLeaseLost)
	}
	if d.LeaseUntil == nil || !now.Before(*d.LeaseUntil) {
		return fmt.Errorf("%w: delivery lease expired", fault.ErrLeaseLost)
	}
	return nil
}

func Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 10 {
		attempt = 10
	}
	return time.Duration(1<<(attempt-1)) * 5 * time.Second
}
