package sqlite

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/station"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

var testNow = time.Date(2026, 8, 26, 1, 2, 3, 0, time.UTC)

func openTestDB(t *testing.T) *DB {
	t.Helper()
	path := filepath.Join(t.TempDir(), "quakewatch.db")
	database, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return database
}

func seedUser(t *testing.T, store repository.Store, id string, role auth.Role) auth.User {
	t.Helper()
	value := auth.User{
		ID: id, Email: id + "@example.invalid", DisplayName: "Test " + id,
		PasswordHash: "hash", Role: role, Active: true, Version: 1,
		CreatedAt: testNow, UpdatedAt: testNow,
	}
	if err := store.CreateUser(context.Background(), value); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	return value
}

func seedStation(t *testing.T, store repository.Store, id, code string, status station.Status) (station.Station, station.Sensor) {
	t.Helper()
	value := station.Station{
		ID: id, Code: code, Name: "Station " + code, Latitude: 31, Longitude: 105,
		ElevationM: 500, Timezone: "Asia/Shanghai", Status: status, Version: 1,
		CreatedAt: testNow, UpdatedAt: testNow,
	}
	sensor := station.Sensor{
		ID: "sen_" + id, StationID: id, SerialNumber: "SERIAL_" + id, Channel: "BHZ",
		SampleRateHz: 100, InstalledAt: testNow.AddDate(-1, 0, 0),
		CalibratedAt: testNow.AddDate(0, -1, 0), Version: 1, CreatedAt: testNow,
	}
	if err := store.CreateStation(context.Background(), value); err != nil {
		t.Fatalf("CreateStation() error = %v", err)
	}
	if err := store.CreateSensor(context.Background(), sensor); err != nil {
		t.Fatalf("CreateSensor() error = %v", err)
	}
	return value, sensor
}

func seedWaveform(t *testing.T, store repository.Store, stationValue station.Station, sensor station.Sensor, id string, starts time.Time) waveform.Batch {
	t.Helper()
	value := waveform.Batch{
		ID: id, StationID: stationValue.ID, SensorID: sensor.ID, SourceKey: "source-" + id,
		StartsAt: starts, EndsAt: starts.Add(time.Minute), SampleCount: 6000,
		Checksum: fmt.Sprintf("%064d", len(id)), Status: waveform.StatusProcessed,
		Version: 1, CreatedAt: testNow, UpdatedAt: testNow,
	}
	if err := store.CreateWaveform(context.Background(), value); err != nil {
		t.Fatalf("CreateWaveform() error = %v", err)
	}
	return value
}

func TestMigrationsCreateRelationalSchema(t *testing.T) {
	database := openTestDB(t)
	rows, err := database.sql.Query(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`)
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	defer rows.Close()
	seen := make(map[string]bool)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan table: %v", err)
		}
		seen[name] = true
	}
	for _, name := range []string{
		"users", "sessions", "stations", "sensors", "waveform_batches",
		"event_candidates", "phase_picks", "review_decisions", "alert_rules",
		"alert_deliveries", "worker_jobs", "idempotency_keys", "audit_events",
		"schema_migrations",
	} {
		if !seen[name] {
			t.Errorf("table %s missing; tables = %v", name, seen)
		}
	}
	var migrationCount int
	if err := database.sql.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if migrationCount != 3 {
		t.Fatalf("migration count = %d, want 3", migrationCount)
	}
	if err := database.Migrate(context.Background()); err != nil {
		t.Fatalf("second Migrate() error = %v", err)
	}
}

func TestPersistenceSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "restart.db")
	first, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	seedUser(t, first, "restart", auth.RoleAdmin)
	stationValue, sensor := seedStation(t, first, "restart", "RST1", station.StatusActive)
	wave := seedWaveform(t, first, stationValue, sensor, "wav_restart", testNow.Add(-time.Hour))
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	second, err := Open(context.Background(), path)
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	defer second.Close()
	gotUser, err := second.GetUserByID(context.Background(), "restart")
	if err != nil || gotUser.Role != auth.RoleAdmin {
		t.Fatalf("reloaded user = %#v, err = %v", gotUser, err)
	}
	gotWave, err := second.GetWaveform(context.Background(), wave.ID)
	if err != nil || gotWave.Checksum != wave.Checksum {
		t.Fatalf("reloaded waveform = %#v, err = %v", gotWave, err)
	}
}

func TestTransactionRollsBackCrossEntityWrites(t *testing.T) {
	database := openTestDB(t)
	err := database.WithinTx(context.Background(), func(store repository.Store) error {
		seedUser(t, store, "rolledback", auth.RoleOperator)
		if err := store.CreateSession(context.Background(), auth.Session{
			ID: "ses_rollback", UserID: "missing_user", TokenHash: "token",
			ExpiresAt: testNow.Add(time.Hour), CreatedAt: testNow, LastSeenAt: testNow,
		}); err == nil {
			return errors.New("expected foreign key failure")
		}
		return errors.New("force rollback after expected failure")
	})
	if err == nil {
		t.Fatal("WithinTx() unexpectedly succeeded")
	}
	if _, err := database.GetUserByID(context.Background(), "rolledback"); !errors.Is(err, fault.ErrNotFound) {
		t.Fatalf("rolled back user lookup error = %v", err)
	}
}

func TestSessionLifecycleAndUserState(t *testing.T) {
	database := openTestDB(t)
	user := seedUser(t, database, "analyst", auth.RoleAnalyst)
	session := auth.Session{
		ID: "ses_1", UserID: user.ID, TokenHash: "hash_1", ExpiresAt: testNow.Add(time.Hour),
		CreatedAt: testNow, LastSeenAt: testNow,
	}
	if err := database.CreateSession(context.Background(), session); err != nil {
		t.Fatalf("CreateSession() error = %v", err)
	}
	got, gotUser, err := database.GetSessionByHash(context.Background(), session.TokenHash)
	if err != nil {
		t.Fatalf("GetSessionByHash() error = %v", err)
	}
	if got.ID != session.ID || gotUser.ID != user.ID || gotUser.Role != auth.RoleAnalyst {
		t.Fatalf("session/user mismatch: %#v %#v", got, gotUser)
	}
	touchedAt := testNow.Add(5 * time.Minute)
	if err := database.TouchSession(context.Background(), session.ID, touchedAt); err != nil {
		t.Fatalf("TouchSession() error = %v", err)
	}
	got, _, err = database.GetSessionByHash(context.Background(), session.TokenHash)
	if err != nil || !got.LastSeenAt.Equal(touchedAt) {
		t.Fatalf("touched session = %#v, err = %v", got, err)
	}
	if err := database.RevokeSession(context.Background(), session.ID, touchedAt); err != nil {
		t.Fatalf("RevokeSession() error = %v", err)
	}
	got, _, _ = database.GetSessionByHash(context.Background(), session.TokenHash)
	if got.RevokedAt == nil || !got.RevokedAt.Equal(touchedAt) {
		t.Fatalf("revoked_at = %v", got.RevokedAt)
	}
	deleted, err := database.DeleteExpiredSessions(context.Background(), touchedAt, 10)
	if err != nil || deleted != 1 {
		t.Fatalf("DeleteExpiredSessions() = %d, %v", deleted, err)
	}
}

func TestUserOptimisticVersionPreventsLostUpdate(t *testing.T) {
	database := openTestDB(t)
	user := seedUser(t, database, "versioned", auth.RoleAnalyst)
	updated, err := database.UpdateUserRole(context.Background(), user.ID, auth.RoleOperator, user.Version, testNow.Add(time.Minute))
	if err != nil {
		t.Fatalf("first UpdateUserRole() error = %v", err)
	}
	if updated.Version != 2 || updated.Role != auth.RoleOperator {
		t.Fatalf("updated user = %#v", updated)
	}
	if _, err := database.UpdateUserRole(context.Background(), user.ID, auth.RoleAdmin, user.Version, testNow.Add(2*time.Minute)); !errors.Is(err, fault.ErrVersion) {
		t.Fatalf("stale UpdateUserRole() error = %v, want version", err)
	}
	current, _ := database.GetUserByID(context.Background(), user.ID)
	if current.Role != auth.RoleOperator || current.Version != 2 {
		t.Fatalf("lost update occurred: %#v", current)
	}
}

func TestStationSensorRelationsAndPaging(t *testing.T) {
	database := openTestDB(t)
	for index, code := range []string{"AA01", "AA02", "BB01"} {
		status := station.StatusProvisioning
		if index == 2 {
			status = station.StatusActive
		}
		seedStation(t, database, fmt.Sprintf("station_%d", index), code, status)
	}
	page, err := database.ListStations(context.Background(), repository.StationFilter{Search: "AA", Limit: 1})
	if err != nil {
		t.Fatalf("ListStations() error = %v", err)
	}
	if len(page.Items) != 1 || page.Items[0].Code != "AA01" || page.NextCursor == "" {
		t.Fatalf("first page = %#v", page)
	}
	page2, err := database.ListStations(context.Background(), repository.StationFilter{Search: "AA", After: page.NextCursor, Limit: 1})
	if err != nil || len(page2.Items) != 1 || page2.Items[0].Code != "AA02" {
		t.Fatalf("second page = %#v, err = %v", page2, err)
	}
	active, err := database.ListStations(context.Background(), repository.StationFilter{Status: station.StatusActive, Limit: 10})
	if err != nil || len(active.Items) != 1 || active.Items[0].Code != "BB01" {
		t.Fatalf("active page = %#v, err = %v", active, err)
	}
}

func TestWaveformUniquenessOverlapAndState(t *testing.T) {
	database := openTestDB(t)
	stationValue, sensor := seedStation(t, database, "wave_station", "WAV1", station.StatusActive)
	first := seedWaveform(t, database, stationValue, sensor, "wav_1", testNow.Add(-time.Hour))
	duplicate := first
	duplicate.ID = "wav_2"
	if err := database.CreateWaveform(context.Background(), duplicate); !errors.Is(err, fault.ErrAlreadyExists) {
		t.Fatalf("duplicate source error = %v", err)
	}
	overlap, err := database.HasWaveformOverlap(context.Background(), sensor.ID, first.StartsAt.Add(30*time.Second), first.EndsAt.Add(time.Minute))
	if err != nil || !overlap {
		t.Fatalf("HasWaveformOverlap() = %v, %v", overlap, err)
	}
	nonOverlap, err := database.HasWaveformOverlap(context.Background(), sensor.ID, first.EndsAt, first.EndsAt.Add(time.Minute))
	if err != nil || nonOverlap {
		t.Fatalf("boundary overlap = %v, %v", nonOverlap, err)
	}
	if _, err := database.UpdateWaveformStatus(context.Background(), first.ID, waveform.StatusRejected, "late quality failure", 99, testNow); !errors.Is(err, fault.ErrVersion) {
		t.Fatalf("stale waveform update error = %v", err)
	}
}

func TestJobLeaseIsExclusiveAcrossConcurrentWorkers(t *testing.T) {
	database := openTestDB(t)
	for index := 0; index < 12; index++ {
		value := job.Job{
			ID: fmt.Sprintf("job_%02d", index), Kind: job.KindCleanupSession,
			AggregateID: fmt.Sprintf("cleanup_%02d", index), PayloadJSON: `{}`,
			Status: job.StatusPending, MaxAttempts: 3, NextAttemptAt: testNow,
			Version: 1, CreatedAt: testNow, UpdatedAt: testNow,
		}
		if err := database.CreateJob(context.Background(), value); err != nil {
			t.Fatalf("CreateJob(%d) error = %v", index, err)
		}
	}
	start := make(chan struct{})
	results := make(chan []job.Job, 2)
	errorsChannel := make(chan error, 2)
	var wg sync.WaitGroup
	for _, owner := range []string{"worker-a", "worker-b"} {
		owner := owner
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			leased, err := database.LeaseJobs(context.Background(), owner, testNow, testNow.Add(time.Minute), 12)
			results <- leased
			errorsChannel <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		if err != nil {
			t.Fatalf("LeaseJobs() error = %v", err)
		}
	}
	seen := make(map[string]bool)
	for values := range results {
		for _, value := range values {
			if seen[value.ID] {
				t.Fatalf("job %s leased by multiple workers", value.ID)
			}
			seen[value.ID] = true
		}
	}
	if len(seen) != 12 {
		t.Fatalf("leased jobs = %d, want 12", len(seen))
	}
}

func TestExpiredJobLeaseRecoversAfterRestartBoundary(t *testing.T) {
	database := openTestDB(t)
	value := job.Job{
		ID: "job_recover", Kind: job.KindCleanupSession, AggregateID: "daily", PayloadJSON: `{}`,
		Status: job.StatusPending, MaxAttempts: 3, NextAttemptAt: testNow,
		Version: 1, CreatedAt: testNow, UpdatedAt: testNow,
	}
	if err := database.CreateJob(context.Background(), value); err != nil {
		t.Fatalf("CreateJob() error = %v", err)
	}
	leased, err := database.LeaseJobs(context.Background(), "worker-old", testNow, testNow.Add(time.Minute), 1)
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseJobs() = %#v, %v", leased, err)
	}
	recovered, err := database.RecoverExpiredJobs(context.Background(), testNow.Add(time.Minute))
	if err != nil || recovered != 1 {
		t.Fatalf("RecoverExpiredJobs() = %d, %v", recovered, err)
	}
	leasedAgain, err := database.LeaseJobs(context.Background(), "worker-new", testNow.Add(time.Minute), testNow.Add(2*time.Minute), 1)
	if err != nil || len(leasedAgain) != 1 || pointerValueForTest(leasedAgain[0].LeaseOwner) != "worker-new" {
		t.Fatalf("second LeaseJobs() = %#v, %v", leasedAgain, err)
	}
}

func pointerValueForTest(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func TestEventPickDecisionAndAlertRelations(t *testing.T) {
	database := openTestDB(t)
	analyst := seedUser(t, database, "event_analyst", auth.RoleAnalyst)
	stationValue, sensor := seedStation(t, database, "event_station", "EVT1", station.StatusActive)
	wave := seedWaveform(t, database, stationValue, sensor, "event_wave", testNow.Add(-time.Hour))
	candidate := event.Candidate{
		ID: "event_1", PublicID: "EQ-2026-1000", DetectedAt: testNow.Add(-time.Hour),
		Latitude: 30, Longitude: 105, DepthKM: 10, Magnitude: 5,
		Status: event.StatusDetected, Version: 1, CreatedAt: testNow, UpdatedAt: testNow,
	}
	if err := database.CreateEvent(context.Background(), candidate); err != nil {
		t.Fatalf("CreateEvent() error = %v", err)
	}
	pick := event.Pick{
		ID: "pick_1", EventID: candidate.ID, WaveformID: wave.ID, StationID: stationValue.ID,
		Phase: event.PhaseP, PickedAt: testNow.Add(-time.Hour), Confidence: .9, CreatedAt: testNow,
	}
	if err := database.CreatePick(context.Background(), pick); err != nil {
		t.Fatalf("CreatePick() error = %v", err)
	}
	if picks, err := database.ListPicks(context.Background(), candidate.ID); err != nil || len(picks) != 1 {
		t.Fatalf("ListPicks() = %#v, %v", picks, err)
	}
	claimed, err := database.ClaimEvent(context.Background(), candidate.ID, analyst.ID, testNow.Add(time.Minute), 1, testNow)
	if err != nil || claimed.Status != event.StatusUnderReview {
		t.Fatalf("ClaimEvent() = %#v, %v", claimed, err)
	}
	decision := event.ReviewDecision{
		ID: "decision_1", EventID: candidate.ID, AnalystID: analyst.ID, Decision: event.DecisionConfirm,
		Notes: "three stations agree", EventVersion: claimed.Version, CreatedAt: testNow,
	}
	if err := database.CreateDecision(context.Background(), decision); err != nil {
		t.Fatalf("CreateDecision() error = %v", err)
	}
	confirmed, err := database.DecideEvent(context.Background(), candidate.ID, event.StatusConfirmed, claimed.Version, testNow)
	if err != nil || confirmed.Status != event.StatusConfirmed {
		t.Fatalf("DecideEvent() = %#v, %v", confirmed, err)
	}
	published, err := database.PublishEvent(context.Background(), candidate.ID, confirmed.Version, testNow)
	if err != nil || published.Status != event.StatusPublished {
		t.Fatalf("PublishEvent() = %#v, %v", published, err)
	}
}

func TestAlertRuleMatchingAndDeliveryLease(t *testing.T) {
	database := openTestDB(t)
	rule := alert.Rule{
		ID: "rule_1", Name: "M4 Sichuan", MinimumMagnitude: 4,
		MinLatitude: 20, MaxLatitude: 40, MinLongitude: 90, MaxLongitude: 120,
		Destination: "https://alerts.example.invalid", Enabled: true, Version: 1,
		CreatedAt: testNow, UpdatedAt: testNow,
	}
	if err := database.CreateAlertRule(context.Background(), rule); err != nil {
		t.Fatalf("CreateAlertRule() error = %v", err)
	}
	matches, err := database.MatchingAlertRules(context.Background(), alert.EventEnvelope{Magnitude: 4.5, Latitude: 30, Longitude: 105})
	if err != nil || len(matches) != 1 {
		t.Fatalf("MatchingAlertRules() = %#v, %v", matches, err)
	}
	if misses, err := database.MatchingAlertRules(context.Background(), alert.EventEnvelope{Magnitude: 3, Latitude: 30, Longitude: 105}); err != nil || len(misses) != 0 {
		t.Fatalf("low magnitude matches = %#v, %v", misses, err)
	}
	seedUser(t, database, "delivery_user", auth.RoleAnalyst)
	stationValue, sensor := seedStation(t, database, "delivery_station", "DEL1", station.StatusActive)
	wave := seedWaveform(t, database, stationValue, sensor, "delivery_wave", testNow.Add(-time.Hour))
	_ = wave
	candidate := event.Candidate{
		ID: "delivery_event", PublicID: "EQ-DELIVERY", DetectedAt: testNow,
		Latitude: 30, Longitude: 105, DepthKM: 10, Magnitude: 5,
		Status: event.StatusConfirmed, Version: 1, CreatedAt: testNow, UpdatedAt: testNow,
	}
	if err := database.CreateEvent(context.Background(), candidate); err != nil {
		t.Fatalf("CreateEvent() error = %v", err)
	}
	delivery := alert.Delivery{
		ID: "delivery_1", EventID: candidate.ID, RuleID: rule.ID, Status: alert.StatusPending,
		NextAttemptAt: testNow, Version: 1, CreatedAt: testNow, UpdatedAt: testNow,
	}
	if err := database.CreateDelivery(context.Background(), delivery); err != nil {
		t.Fatalf("CreateDelivery() error = %v", err)
	}
	leased, err := database.LeaseDeliveries(context.Background(), "worker-a", testNow, testNow.Add(time.Minute), 10)
	if err != nil || len(leased) != 1 {
		t.Fatalf("LeaseDeliveries() = %#v, %v", leased, err)
	}
	if err := database.CompleteDelivery(context.Background(), delivery.ID, "worker-b", leased[0].Version, testNow); !errors.Is(err, fault.ErrLeaseLost) {
		t.Fatalf("foreign CompleteDelivery() error = %v", err)
	}
	if err := database.CompleteDelivery(context.Background(), delivery.ID, "worker-a", leased[0].Version, testNow); err != nil {
		t.Fatalf("CompleteDelivery() error = %v", err)
	}
	completed, _ := database.GetDelivery(context.Background(), delivery.ID)
	if completed.Status != alert.StatusDelivered || completed.DeliveredAt == nil {
		t.Fatalf("completed delivery = %#v", completed)
	}
}

func TestIdempotencyScopesAndAuditFilters(t *testing.T) {
	database := openTestDB(t)
	actor := seedUser(t, database, "idempotent", auth.RoleOperator)
	record := repository.IdempotencyRecord{
		ID: "idem_1", ActorID: actor.ID, Method: "POST", Path: "/v1/waveforms", Key: "key-1",
		RequestHash: "hash-a", ExpiresAt: testNow.Add(time.Hour), CreatedAt: testNow,
	}
	if err := database.CreateIdempotency(context.Background(), record); err != nil {
		t.Fatalf("CreateIdempotency() error = %v", err)
	}
	if err := database.CompleteIdempotency(context.Background(), record.ID, 201, `{"id":"wav_1"}`); err != nil {
		t.Fatalf("CompleteIdempotency() error = %v", err)
	}
	got, err := database.GetIdempotency(context.Background(), actor.ID, record.Method, record.Path, record.Key)
	if err != nil || got.ResponseCode == nil || *got.ResponseCode != 201 {
		t.Fatalf("GetIdempotency() = %#v, %v", got, err)
	}
	otherPath := record
	otherPath.ID = "idem_2"
	otherPath.Path = "/v1/events"
	if err := database.CreateIdempotency(context.Background(), otherPath); err != nil {
		t.Fatalf("same key in other path error = %v", err)
	}
	for index, action := range []string{"station.created", "waveform.received", "station.updated"} {
		value := audit.Event{
			ID: fmt.Sprintf("audit_%d", index), ActorID: &actor.ID, RequestID: fmt.Sprintf("req_%d", index),
			Action: action, ObjectType: "station", ObjectID: "sta_1", Result: "success",
			MetadataJSON: []byte(`{}`), CreatedAt: testNow.Add(time.Duration(index) * time.Second),
		}
		if err := database.CreateAudit(context.Background(), value); err != nil {
			t.Fatalf("CreateAudit(%d) error = %v", index, err)
		}
	}
	page, err := database.ListAudit(context.Background(), audit.Query{ActorID: actor.ID, Action: "station.updated", Limit: 10})
	if err != nil || len(page.Items) != 1 || page.Items[0].Action != "station.updated" {
		t.Fatalf("filtered audit page = %#v, %v", page, err)
	}
}
