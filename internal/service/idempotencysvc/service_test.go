package idempotencysvc

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/storage/sqlite"
)

func newIdempotencyFixture(t *testing.T) (*Service, *sqlite.DB) {
	t.Helper()
	valueClock := clock.NewFake(time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC))
	database, err := sqlite.Open(context.Background(), filepath.Join(t.TempDir(), "idem.db"))
	if err != nil {
		t.Fatalf("sqlite.Open() error = %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("database.Close() error = %v", err)
		}
	})
	ids := &idgen.Sequence{}
	return New(database, database, valueClock, ids, time.Hour), database
}

func seedIdempotencyUser(t *testing.T, database *sqlite.DB, id string, role auth.Role) auth.Principal {
	t.Helper()
	now := time.Date(2026, 8, 26, 8, 0, 0, 0, time.UTC)
	if err := database.CreateUser(context.Background(), auth.User{
		ID: id, Email: id + "@example.invalid", DisplayName: "Test " + id,
		PasswordHash: "hash", Role: role, Active: true, Version: 1,
		CreatedAt: now, UpdatedAt: now,
	}); err != nil {
		t.Fatalf("CreateUser() error = %v", err)
	}
	return auth.Principal{UserID: id, Email: id + "@example.invalid", Role: role}
}

func TestExecuteClearsPlaceholderWhenOperationCanceled(t *testing.T) {
	service, database := newIdempotencyFixture(t)
	actor := seedIdempotencyUser(t, database, "user-cancel", auth.RoleOperator)
	const key = "retry-key-cancel"

	// An operation that observes the client cancellation while a business write
	// would still be in flight, mirroring a waveform ingest request canceled
	// before its transaction commits.
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := service.Execute(canceledCtx, actor, "POST", "/v1/waveforms", key, []byte("payload"), func(ctx context.Context) (int, any, error) {
		if err := ctx.Err(); err != nil {
			return 0, nil, err
		}
		t.Fatal("operation should not run once the request context is already canceled")
		return 0, nil, nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Execute() error = %v, want context.Canceled", err)
	}

	// The placeholder must be removed so the same key can be retried instead of
	// returning "operation is still in progress" for the TTL window.
	record, err := database.GetIdempotency(context.Background(), actor.UserID, "POST", "/v1/waveforms", key)
	if !errors.Is(err, fault.ErrNotFound) {
		t.Fatalf("GetIdempotency() = %#v, err = %v, want ErrNotFound (placeholder cleaned up)", record, err)
	}

	// Retrying with the same key must succeed rather than report an in-progress conflict.
	result, err := service.Execute(context.Background(), actor, "POST", "/v1/waveforms", key, []byte("payload"), func(ctx context.Context) (int, any, error) {
		return 201, map[string]string{"id": "wav_1"}, nil
	})
	if err != nil {
		t.Fatalf("retry Execute() error = %v, want nil", err)
	}
	if result.Code != 201 {
		t.Fatalf("retry result code = %d, want 201", result.Code)
	}
}

func TestExecuteReplaysCompletedResult(t *testing.T) {
	service, database := newIdempotencyFixture(t)
	actor := seedIdempotencyUser(t, database, "user-replay", auth.RoleOperator)
	const key = "retry-key-replay"

	first, err := service.Execute(context.Background(), actor, "POST", "/v1/waveforms", key, []byte("payload"), func(ctx context.Context) (int, any, error) {
		return 201, map[string]string{"id": "wav_2"}, nil
	})
	if err != nil || first.Code != 201 {
		t.Fatalf("first Execute() = %#v, err = %v", first, err)
	}

	replayed, err := service.Execute(context.Background(), actor, "POST", "/v1/waveforms", key, []byte("payload"), func(ctx context.Context) (int, any, error) {
		t.Fatal("operation should not re-run for a completed key")
		return 0, nil, nil
	})
	if err != nil {
		t.Fatalf("replay Execute() error = %v", err)
	}
	if !replayed.Reused || replayed.Code != 201 {
		t.Fatalf("replay result = %#v, want Reused with code 201", replayed)
	}
}
