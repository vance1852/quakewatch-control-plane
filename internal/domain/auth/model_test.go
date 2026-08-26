package auth

import (
	"errors"
	"testing"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

func TestNormalizeEmail(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
		valid bool
	}{
		{name: "normalizes case and whitespace", input: "  Analyst@Example.COM ", want: "analyst@example.com", valid: true},
		{name: "rejects missing at", input: "analyst.example.com", valid: false},
		{name: "rejects display name form", input: "Analyst <analyst@example.com>", valid: false},
		{name: "rejects empty", input: "", valid: false},
		{name: "accepts subdomain", input: "ops@west.example.invalid", want: "ops@west.example.invalid", valid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeEmail(test.input)
			if test.valid && err != nil {
				t.Fatalf("NormalizeEmail() error = %v", err)
			}
			if !test.valid && !errors.Is(err, fault.ErrValidation) {
				t.Fatalf("NormalizeEmail() error = %v, want validation", err)
			}
			if got != test.want {
				t.Fatalf("NormalizeEmail() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestValidatePassword(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		password string
		valid    bool
	}{
		{name: "strong", password: "MonitorPass123", valid: true},
		{name: "minimum length strong", password: "Station9Pass", valid: true},
		{name: "too short", password: "Short1A", valid: false},
		{name: "missing uppercase", password: "lowercase1234", valid: false},
		{name: "missing lowercase", password: "UPPERCASE1234", valid: false},
		{name: "missing number", password: "NoNumbersHere", valid: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidatePassword(test.password)
			if test.valid && err != nil {
				t.Fatalf("ValidatePassword() error = %v", err)
			}
			if !test.valid && !errors.Is(err, fault.ErrValidation) {
				t.Fatalf("ValidatePassword() error = %v, want validation", err)
			}
		})
	}
}

func TestRolePermissions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		role       Role
		permission Permission
		want       bool
	}{
		{role: RoleAnalyst, permission: PermissionReviewEvents, want: true},
		{role: RoleAnalyst, permission: PermissionPublishEvents, want: true},
		{role: RoleAnalyst, permission: PermissionManageNetwork, want: false},
		{role: RoleOperator, permission: PermissionManageNetwork, want: true},
		{role: RoleOperator, permission: PermissionIngest, want: true},
		{role: RoleOperator, permission: PermissionReviewEvents, want: false},
		{role: RoleAdmin, permission: PermissionManageUsers, want: true},
		{role: RoleAdmin, permission: PermissionReviewEvents, want: true},
	}
	for _, test := range tests {
		if got := test.role.Allows(test.permission); got != test.want {
			t.Errorf("Role(%s).Allows(%s) = %v, want %v", test.role, test.permission, got, test.want)
		}
	}
}

func TestPrincipalRequire(t *testing.T) {
	t.Parallel()
	if err := (Principal{}).Require(PermissionReadAudit); !errors.Is(err, fault.ErrUnauthorized) {
		t.Fatalf("anonymous Require() error = %v", err)
	}
	operator := Principal{UserID: "usr_1", Role: RoleOperator}
	if err := operator.Require(PermissionIngest); err != nil {
		t.Fatalf("operator ingest permission error = %v", err)
	}
	if err := operator.Require(PermissionReviewEvents); !errors.Is(err, fault.ErrForbidden) {
		t.Fatalf("operator review permission error = %v, want forbidden", err)
	}
}

func TestSessionValidAt(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 26, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name    string
		session Session
		want    error
	}{
		{name: "active", session: Session{ExpiresAt: now.Add(time.Hour)}},
		{name: "expires exactly now", session: Session{ExpiresAt: now}, want: fault.ErrExpired},
		{name: "expired before now", session: Session{ExpiresAt: now.Add(-time.Nanosecond)}, want: fault.ErrExpired},
		{name: "revoked", session: Session{ExpiresAt: now.Add(time.Hour), RevokedAt: pointerTime(now.Add(-time.Minute))}, want: fault.ErrUnauthorized},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.session.ValidAt(now)
			if test.want == nil && err != nil {
				t.Fatalf("ValidAt() error = %v", err)
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("ValidAt() error = %v, want %v", err, test.want)
			}
		})
	}
}

func pointerTime(value time.Time) *time.Time { return &value }
