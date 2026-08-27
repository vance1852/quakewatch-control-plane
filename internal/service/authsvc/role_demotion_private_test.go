package authsvc

import (
	"context"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

type roleStore struct {
	repository.Store
	session auth.Session
	user    auth.User
}

func (s *roleStore) GetSessionByHash(context.Context, string) (auth.Session, auth.User, error) {
	return s.session, s.user, nil
}

func (s *roleStore) TouchSession(context.Context, string, time.Time) error { return nil }

func TestAuthenticateReflectsRoleDemotionForExistingSession(t *testing.T) {
	now := time.Date(2026, 8, 27, 3, 0, 0, 0, time.UTC)
	store := &roleStore{
		session: auth.Session{ID: "ses-1", UserID: "usr-1", ExpiresAt: now.Add(time.Hour), LastSeenAt: now},
		user:    auth.User{ID: "usr-1", Email: "user@example.invalid", Role: auth.RoleAdmin, Active: true},
	}
	service := New(store, nil, clock.NewFake(now), nil, time.Hour)
	first, err := service.Authenticate(context.Background(), "existing-token")
	if err != nil || first.Role != auth.RoleAdmin {
		t.Fatalf("first Authenticate() = (%#v, %v); want admin", first, err)
	}
	store.user.Role = auth.RoleAnalyst

	second, err := service.Authenticate(context.Background(), "existing-token")
	if err != nil {
		t.Fatalf("second Authenticate() error = %v", err)
	}
	if second.Role != auth.RoleAnalyst {
		t.Fatalf("second Authenticate() role = %s; want analyst after persisted demotion", second.Role)
	}
}
