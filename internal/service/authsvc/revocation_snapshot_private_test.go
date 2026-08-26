package authsvc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/audit"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type revocationSnapshotStore struct {
	repository.Store
	mu               sync.Mutex
	session          auth.Session
	user             auth.User
	snapshotRead     chan struct{}
	continueIdentity chan struct{}
	signalOnce       sync.Once
}

func (s *revocationSnapshotStore) GetSessionSnapshot(ctx context.Context, _ string) (auth.Session, error) {
	s.mu.Lock()
	snapshot := s.session
	s.mu.Unlock()
	s.signalOnce.Do(func() { close(s.snapshotRead) })
	select {
	case <-s.continueIdentity:
		return snapshot, nil
	case <-ctx.Done():
		return auth.Session{}, ctx.Err()
	}
}

func (s *revocationSnapshotStore) GetUserByID(_ context.Context, id string) (auth.User, error) {
	if id != s.user.ID {
		return auth.User{}, fault.ErrNotFound
	}
	return s.user, nil
}

func (s *revocationSnapshotStore) RevokeSession(_ context.Context, id string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.session.ID != id || s.session.RevokedAt != nil {
		return fault.ErrUnauthorized
	}
	s.session.RevokedAt = &now
	return nil
}

func (s *revocationSnapshotStore) CreateAudit(context.Context, audit.Event) error {
	return nil
}

func (s *revocationSnapshotStore) WithinTx(ctx context.Context, operation func(repository.Store) error) error {
	return operation(s)
}

func TestLogoutRevocationBlocksInFlightAuthentication(t *testing.T) {
	now := time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC)
	const token = "session-token-shared-with-authenticate"
	store := &revocationSnapshotStore{
		session: auth.Session{
			ID: "ses_logout", UserID: "usr_operator", TokenHash: hashToken(token),
			ExpiresAt: now.Add(time.Hour), CreatedAt: now, LastSeenAt: now,
		},
		user: auth.User{
			ID: "usr_operator", Email: "operator@example.invalid", Role: auth.RoleOperator, Active: true,
		},
		snapshotRead:     make(chan struct{}),
		continueIdentity: make(chan struct{}),
	}
	service := New(store, store, clock.NewFake(now), &idgen.Sequence{}, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	type authenticationResult struct {
		principal auth.Principal
		err       error
	}
	authenticated := make(chan authenticationResult, 1)
	go func() {
		principal, err := service.Authenticate(ctx, token)
		authenticated <- authenticationResult{principal: principal, err: err}
	}()

	select {
	case <-store.snapshotRead:
	case <-ctx.Done():
		t.Fatalf("authentication did not read the session snapshot: %v", ctx.Err())
	}
	logoutPrincipal := auth.Principal{UserID: store.user.ID, SessionID: store.session.ID, Role: auth.RoleOperator}
	if err := service.Logout(ctx, logoutPrincipal); err != nil {
		t.Fatalf("Logout() error = %v", err)
	}
	close(store.continueIdentity)

	result := <-authenticated
	if !errors.Is(result.err, fault.ErrUnauthorized) {
		t.Fatalf("Authenticate() after completed revocation = principal %#v, error %v; want unauthorized", result.principal, result.err)
	}
	if result.principal.UserID != "" || result.principal.SessionID != "" {
		t.Fatalf("revoked session produced principal %#v", result.principal)
	}
}
