package alertsvc

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/storage/sqlite"
)

// recordingSender captures the destination URL used for each delivery so a
// test can assert which rule snapshot the service selected.
type recordingSender struct {
	destinations []string
}

func (r *recordingSender) Send(_ context.Context, rule alert.Rule, _ alert.Delivery, _ any) error {
	r.destinations = append(r.destinations, rule.Destination)
	return nil
}

func newServiceFixture(t *testing.T) (*Service, *recordingSender, *clock.Fake, *sqlite.DB) {
	t.Helper()
	now := time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC)
	valueClock := clock.NewFake(now)
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "alert.db"))
	if err != nil {
		t.Fatalf("sqlite.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("database.Close() error = %v", err)
		}
	})
	sender := &recordingSender{}
	ids := &idgen.Sequence{}
	service := New(database, database, valueClock, ids, sender)
	return service, sender, valueClock, database
}

func seedAdmin(t *testing.T, database *sqlite.DB, id string) auth.Principal {
	t.Helper()
	user := auth.User{
		ID: id, Email: id + "@example.invalid", DisplayName: "Admin " + id,
		PasswordHash: "x", Role: auth.RoleAdmin, Active: true, Version: 1,
		CreatedAt: time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 26, 0, 0, 0, 0, time.UTC),
	}
	if err := database.CreateUser(context.Background(), user); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	return auth.Principal{UserID: id, Email: user.Email, Role: auth.RoleAdmin}
}

func mustCreateRule(t *testing.T, service *Service, principal auth.Principal, destination string) alert.Rule {
	t.Helper()
	rule, err := service.CreateRule(context.Background(), principal, alert.Rule{
		Name: "Regional M4", MinimumMagnitude: 4,
		MinLatitude: 20, MaxLatitude: 50, MinLongitude: 90, MaxLongitude: 130,
		Destination: destination, Enabled: true,
	})
	if err != nil {
		t.Fatalf("CreateRule() error = %v", err)
	}
	return rule
}

func mustSeedEvent(t *testing.T, database *sqlite.DB) event.Candidate {
	t.Helper()
	candidate := event.Candidate{
		ID: "evt_deliver", PublicID: "EQ-DELIVER", DetectedAt: time.Date(2026, 8, 26, 7, 50, 0, 0, time.UTC),
		Latitude: 31, Longitude: 110, DepthKM: 12, Magnitude: 5,
		Status: event.StatusPublished, Version: 1,
		CreatedAt: time.Date(2026, 8, 26, 7, 55, 0, 0, time.UTC),
		UpdatedAt: time.Date(2026, 8, 26, 7, 55, 0, 0, time.UTC),
	}
	if err := database.CreateEvent(context.Background(), candidate); err != nil {
		t.Fatalf("CreateEvent() error = %v", err)
	}
	return candidate
}

// TestDeliverUsesUpdatedDestination reproduces the stale-snapshot bug: after a
// rule has delivered once (populating the in-memory cache), an admin updates
// the webhook destination. The next delivery must target the new URL rather
// than the cached stale value.
func TestDeliverUsesUpdatedDestination(t *testing.T) {
	service, sender, _, database := newServiceFixture(t)
	principal := seedAdmin(t, database, "admin_user")
	old := "https://alerts.example.invalid/old"
	newDest := "https://alerts.example.invalid/new"

	rule := mustCreateRule(t, service, principal, old)
	candidate := mustSeedEvent(t, database)

	delivery := alert.Delivery{
		ID: "del_1", EventID: candidate.ID, RuleID: rule.ID,
		Status: alert.StatusPending, Version: 1,
		NextAttemptAt: time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC),
		CreatedAt:   time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC),
		UpdatedAt:   time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC),
	}
	// First delivery populates the rule snapshot cache with the old destination.
	if err := service.Deliver(context.Background(), delivery); err != nil {
		t.Fatalf("first Deliver() error = %v", err)
	}

	rule.Destination = newDest
	updated, err := service.UpdateRule(context.Background(), principal, rule, rule.Version)
	if err != nil {
		t.Fatalf("UpdateRule() error = %v", err)
	}
	if updated.Destination != newDest {
		t.Fatalf("UpdateRule() destination = %q, want %q", updated.Destination, newDest)
	}

	// Second delivery must use the freshly committed destination.
	delivery.ID = "del_2"
	if err := service.Deliver(context.Background(), delivery); err != nil {
		t.Fatalf("second Deliver() error = %v", err)
	}

	if got := sender.destinations; len(got) != 2 || got[0] != old || got[1] != newDest {
		t.Fatalf("destinations = %v, want [%s %s]", got, old, newDest)
	}
}

// TestUpdateRuleReplacesDisabledSnapshot ensures that disabling a rule clears
// the cached enabled snapshot, so a later update can re-enable it cleanly.
func TestUpdateRuleReplacesStaleSnapshot(t *testing.T) {
	service, _, _, database := newServiceFixture(t)
	principal := seedAdmin(t, database, "admin_user")
	rule := mustCreateRule(t, service, principal, "https://alerts.example.invalid")
	candidate := mustSeedEvent(t, database)

	delivery := alert.Delivery{
		ID: "del_disable", EventID: candidate.ID, RuleID: rule.ID,
		Status: alert.StatusPending, Version: 1,
		NextAttemptAt: time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC),
		CreatedAt:   time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC),
		UpdatedAt:   time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC),
	}
	if err := service.Deliver(context.Background(), delivery); err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}

	rule.Enabled = false
	updated, err := service.UpdateRule(context.Background(), principal, rule, rule.Version)
	if err != nil {
		t.Fatalf("UpdateRule() error = %v", err)
	}

	// The cache must now hold the disabled snapshot rather than the stale
	// enabled one.
	cached, ok := service.rules.Get(rule.ID)
	if !ok {
		t.Fatalf("rule snapshot missing after update")
	}
	if cached.Version != updated.Version || cached.Enabled {
		t.Fatalf("cached snapshot = %+v, want version %d enabled false", cached, updated.Version)
	}
}
