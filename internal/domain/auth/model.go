package auth

import (
	"fmt"
	"net/mail"
	"strings"
	"sync"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

type Role string

const (
	RoleAnalyst  Role = "analyst"
	RoleOperator Role = "operator"
	RoleAdmin    Role = "admin"
)

type Permission string

const (
	PermissionReviewEvents  Permission = "events:review"
	PermissionPublishEvents Permission = "events:publish"
	PermissionManageNetwork Permission = "network:manage"
	PermissionIngest        Permission = "waveforms:ingest"
	PermissionManageAlerts  Permission = "alerts:manage"
	PermissionReadAudit     Permission = "audit:read"
	PermissionManageUsers   Permission = "users:manage"
)

type User struct {
	ID           string    `json:"id"`
	Email        string    `json:"email"`
	DisplayName  string    `json:"display_name"`
	PasswordHash string    `json:"-"`
	Role         Role      `json:"role"`
	Active       bool      `json:"active"`
	Version      int64     `json:"version"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type Session struct {
	ID         string     `json:"id"`
	UserID     string     `json:"user_id"`
	TokenHash  string     `json:"-"`
	ExpiresAt  time.Time  `json:"expires_at"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	LastSeenAt time.Time  `json:"last_seen_at"`
}

type Principal struct {
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`
	Email     string `json:"email"`
	Role      Role   `json:"role"`
}

type PrincipalCache struct {
	mu    sync.RWMutex
	items map[string]Principal
}

func NewPrincipalCache() *PrincipalCache {
	return &PrincipalCache{items: make(map[string]Principal)}
}

func (c *PrincipalCache) Get(tokenHash string) (Principal, bool) {
	c.mu.RLock()
	principal, ok := c.items[tokenHash]
	c.mu.RUnlock()
	return principal, ok
}

func (c *PrincipalCache) Put(tokenHash string, principal Principal) {
	c.mu.Lock()
	c.items[tokenHash] = principal
	c.mu.Unlock()
}

// Invalidate drops every cached principal for the given user. Existing
// sessions are not revoked — the next Authenticate call repopulates the
// cache from the freshly read user record, so role or active-state changes
// take effect immediately across all of a user's outstanding sessions.
func (c *PrincipalCache) Invalidate(userID string) {
	if userID == "" {
		return
	}
	c.mu.Lock()
	for hash, principal := range c.items {
		if principal.UserID == userID {
			delete(c.items, hash)
		}
	}
	c.mu.Unlock()
}

func ParseRole(value string) (Role, error) {
	role := Role(strings.ToLower(strings.TrimSpace(value)))
	switch role {
	case RoleAnalyst, RoleOperator, RoleAdmin:
		return role, nil
	default:
		return "", fault.Validation("role", "must be analyst, operator, or admin")
	}
}

func NormalizeEmail(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	address, err := mail.ParseAddress(value)
	if err != nil || !strings.EqualFold(address.Address, value) {
		return "", fault.Validation("email", "must be a valid mailbox")
	}
	if len(value) > 254 {
		return "", fault.Validation("email", "must not exceed 254 characters")
	}
	return value, nil
}

func ValidateDisplayName(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) < 2 || len(value) > 80 {
		return "", fault.Validation("display_name", "must contain 2 to 80 characters")
	}
	return value, nil
}

func ValidatePassword(value string) error {
	if len(value) < 12 {
		return fault.Validation("password", "must contain at least 12 characters")
	}
	if len(value) > 128 {
		return fault.Validation("password", "must not exceed 128 characters")
	}
	var lower, upper, digit bool
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			lower = true
		case r >= 'A' && r <= 'Z':
			upper = true
		case r >= '0' && r <= '9':
			digit = true
		}
	}
	if !lower || !upper || !digit {
		return fault.Validation("password", "must contain lowercase, uppercase, and numeric characters")
	}
	return nil
}

func (r Role) Allows(permission Permission) bool {
	if r == RoleAdmin {
		return true
	}
	allowed := map[Role]map[Permission]bool{
		RoleAnalyst: {
			PermissionReviewEvents:  true,
			PermissionPublishEvents: true,
			PermissionReadAudit:     true,
		},
		RoleOperator: {
			PermissionManageNetwork: true,
			PermissionIngest:        true,
			PermissionManageAlerts:  true,
			PermissionReadAudit:     true,
		},
	}
	return allowed[r][permission]
}

func (p Principal) Require(permission Permission) error {
	if p.UserID == "" {
		return fault.ErrUnauthorized
	}
	if !p.Role.Allows(permission) {
		return fmt.Errorf("%w: role %s lacks %s", fault.ErrForbidden, p.Role, permission)
	}
	return nil
}

func (s Session) ValidAt(now time.Time) error {
	if s.RevokedAt != nil {
		return fmt.Errorf("%w: session revoked", fault.ErrUnauthorized)
	}
	if !now.Before(s.ExpiresAt) {
		return fmt.Errorf("%w: session expired", fault.ErrExpired)
	}
	return nil
}
