package authsvc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
	"github.com/vance1852/quakewatch-control-plane/internal/requestmeta"
	"github.com/vance1852/quakewatch-control-plane/internal/service/shared"
	"golang.org/x/crypto/bcrypt"
)

type Service struct {
	store      repository.Store
	tx         repository.Transactor
	clock      clock.Clock
	ids        idgen.Generator
	sessionTTL time.Duration
}

type LoginResult struct {
	Token     string         `json:"token"`
	ExpiresAt time.Time      `json:"expires_at"`
	User      auth.Principal `json:"user"`
}

type CreateUserInput struct {
	Email       string
	DisplayName string
	Password    string
	Role        auth.Role
}

func New(store repository.Store, tx repository.Transactor, valueClock clock.Clock, ids idgen.Generator, sessionTTL time.Duration) *Service {
	return &Service{store: store, tx: tx, clock: valueClock, ids: ids, sessionTTL: sessionTTL}
}

func (s *Service) CreateUser(ctx context.Context, principal auth.Principal, input CreateUserInput) (auth.User, error) {
	if err := principal.Require(auth.PermissionManageUsers); err != nil {
		return auth.User{}, err
	}
	return s.createUser(ctx, shared.Actor(principal.UserID), input)
}

func (s *Service) Bootstrap(ctx context.Context, email, password string) (auth.User, error) {
	if strings.TrimSpace(email) == "" && password == "" {
		return auth.User{}, nil
	}
	normalized, err := auth.NormalizeEmail(email)
	if err != nil {
		return auth.User{}, err
	}
	existing, err := s.store.GetUserByEmail(ctx, normalized)
	if err == nil {
		if existing.Role != auth.RoleAdmin || !existing.Active {
			return auth.User{}, fmt.Errorf("bootstrap account exists without active admin role: %w", fault.ErrConflict)
		}
		return existing, nil
	}
	if !errors.Is(err, fault.ErrNotFound) {
		return auth.User{}, err
	}
	return s.createUser(ctx, nil, CreateUserInput{
		Email:       normalized,
		DisplayName: "Bootstrap Administrator",
		Password:    password,
		Role:        auth.RoleAdmin,
	})
}

func (s *Service) createUser(ctx context.Context, actorID *string, input CreateUserInput) (auth.User, error) {
	email, err := auth.NormalizeEmail(input.Email)
	if err != nil {
		return auth.User{}, err
	}
	name, err := auth.ValidateDisplayName(input.DisplayName)
	if err != nil {
		return auth.User{}, err
	}
	if err := auth.ValidatePassword(input.Password); err != nil {
		return auth.User{}, err
	}
	role, err := auth.ParseRole(string(input.Role))
	if err != nil {
		return auth.User{}, err
	}
	passwordHash, err := bcrypt.GenerateFromPassword([]byte(input.Password), bcrypt.DefaultCost)
	if err != nil {
		return auth.User{}, fmt.Errorf("hash password: %w", err)
	}
	now := s.clock.Now()
	user := auth.User{
		ID:           s.ids.New("usr"),
		Email:        email,
		DisplayName:  name,
		PasswordHash: string(passwordHash),
		Role:         role,
		Active:       true,
		Version:      1,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	err = s.tx.WithinTx(ctx, func(store repository.Store) error {
		if err := store.CreateUser(ctx, user); err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, actorID, "user.created", "user", user.ID, "success", map[string]any{
			"email": user.Email,
			"role":  user.Role,
		}, now)
	})
	if err != nil {
		return auth.User{}, fault.Wrap("create user transaction", err)
	}
	return user, nil
}

func (s *Service) Login(ctx context.Context, email, password string) (LoginResult, error) {
	normalized, err := auth.NormalizeEmail(email)
	if err != nil {
		return LoginResult{}, fault.ErrUnauthorized
	}
	user, err := s.store.GetUserByEmail(ctx, normalized)
	if err != nil {
		if errors.Is(err, fault.ErrNotFound) {
			return LoginResult{}, fault.ErrUnauthorized
		}
		return LoginResult{}, err
	}
	if !user.Active || bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)) != nil {
		return LoginResult{}, fault.ErrUnauthorized
	}
	token, hash, err := newToken()
	if err != nil {
		return LoginResult{}, err
	}
	now := s.clock.Now()
	session := auth.Session{
		ID:         s.ids.New("ses"),
		UserID:     user.ID,
		TokenHash:  hash,
		ExpiresAt:  now.Add(s.sessionTTL),
		CreatedAt:  now,
		LastSeenAt: now,
	}
	err = s.tx.WithinTx(ctx, func(store repository.Store) error {
		if err := store.CreateSession(ctx, session); err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &user.ID, "session.created", "session", session.ID, "success", map[string]any{
			"expires_at": session.ExpiresAt,
		}, now)
	})
	if err != nil {
		return LoginResult{}, fault.Wrap("create login session", err)
	}
	return LoginResult{
		Token:     token,
		ExpiresAt: session.ExpiresAt,
		User: auth.Principal{
			UserID:    user.ID,
			SessionID: session.ID,
			Email:     user.Email,
			Role:      user.Role,
		},
	}, nil
}

func (s *Service) Authenticate(ctx context.Context, token string) (auth.Principal, error) {
	if strings.TrimSpace(token) == "" {
		return auth.Principal{}, fault.ErrUnauthorized
	}
	hash := hashToken(token)
	session, user, err := s.store.GetSessionByHash(ctx, hash)
	if err != nil {
		if errors.Is(err, fault.ErrNotFound) {
			return auth.Principal{}, fault.ErrUnauthorized
		}
		return auth.Principal{}, err
	}
	now := s.clock.Now()
	if err := session.ValidAt(now); err != nil {
		return auth.Principal{}, fault.ErrUnauthorized
	}
	if !user.Active {
		return auth.Principal{}, fault.ErrUnauthorized
	}
	if now.Sub(session.LastSeenAt) >= time.Minute {
		if err := s.store.TouchSession(ctx, session.ID, now); err != nil {
			return auth.Principal{}, err
		}
	}
	return auth.Principal{UserID: user.ID, SessionID: session.ID, Email: user.Email, Role: user.Role}, nil
}

func (s *Service) Logout(ctx context.Context, principal auth.Principal) error {
	if principal.SessionID == "" {
		return fault.ErrUnauthorized
	}
	now := s.clock.Now()
	return s.tx.WithinTx(ctx, func(store repository.Store) error {
		if err := store.RevokeSession(ctx, principal.SessionID, now); err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "session.revoked", "session", principal.SessionID, "success", map[string]any{}, now)
	})
}

func (s *Service) UpdateRole(ctx context.Context, principal auth.Principal, userID string, role auth.Role, version int64) (auth.User, error) {
	if err := principal.Require(auth.PermissionManageUsers); err != nil {
		return auth.User{}, err
	}
	parsed, err := auth.ParseRole(string(role))
	if err != nil {
		return auth.User{}, err
	}
	if principal.UserID == userID && parsed != auth.RoleAdmin {
		return auth.User{}, fmt.Errorf("%w: administrator cannot remove own admin role", fault.ErrConflict)
	}
	now := s.clock.Now()
	var updated auth.User
	err = s.tx.WithinTx(ctx, func(store repository.Store) error {
		var txErr error
		updated, txErr = store.UpdateUserRole(ctx, userID, parsed, version, now)
		if txErr != nil {
			return txErr
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "user.role_changed", "user", userID, "success", map[string]any{
			"new_role": parsed,
		}, now)
	})
	return updated, err
}

func (s *Service) PrincipalFromContext(ctx context.Context) (auth.Principal, error) {
	principal, ok := requestmeta.Principal(ctx)
	if !ok {
		return auth.Principal{}, fault.ErrUnauthorized
	}
	return principal, nil
}

func newToken() (string, string, error) {
	var bytes [32]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", "", fmt.Errorf("generate session token: %w", err)
	}
	token := base64.RawURLEncoding.EncodeToString(bytes[:])
	return token, hashToken(token), nil
}

func hashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
