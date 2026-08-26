package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
)

func (q *Queries) CreateUser(ctx context.Context, user auth.User) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO users(
		id,email,display_name,password_hash,role,active,version,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?)`, user.ID, user.Email, user.DisplayName, user.PasswordHash,
		user.Role, boolInt(user.Active), user.Version, formatTime(user.CreatedAt), formatTime(user.UpdatedAt))
	return mapError("create user", err)
}

func scanUser(scanner interface{ Scan(...any) error }) (auth.User, error) {
	var user auth.User
	var active int
	var created, updated string
	err := scanner.Scan(&user.ID, &user.Email, &user.DisplayName, &user.PasswordHash, &user.Role,
		&active, &user.Version, &created, &updated)
	if err != nil {
		return auth.User{}, err
	}
	user.Active = active == 1
	user.CreatedAt, err = parseTime(created)
	if err != nil {
		return auth.User{}, err
	}
	user.UpdatedAt, err = parseTime(updated)
	return user, err
}

const userColumns = `id,email,display_name,password_hash,role,active,version,created_at,updated_at`

func (q *Queries) GetUserByID(ctx context.Context, id string) (auth.User, error) {
	user, err := scanUser(q.q.QueryRowContext(ctx, "SELECT "+userColumns+" FROM users WHERE id = ?", id))
	return user, mapError("get user by id", err)
}

func (q *Queries) GetUserByEmail(ctx context.Context, email string) (auth.User, error) {
	user, err := scanUser(q.q.QueryRowContext(ctx, "SELECT "+userColumns+" FROM users WHERE email = ?", email))
	return user, mapError("get user by email", err)
}

func (q *Queries) UpdateUserRole(ctx context.Context, id string, role auth.Role, version int64, now time.Time) (auth.User, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE users SET role=?, version=version+1, updated_at=? WHERE id=? AND version=?`, role, formatTime(now), id, version)
	if err != nil {
		return auth.User{}, mapError("update user role", err)
	}
	if err := requireUpdated(result, "update user role"); err != nil {
		return auth.User{}, err
	}
	return q.GetUserByID(ctx, id)
}

func (q *Queries) SetUserActive(ctx context.Context, id string, active bool, version int64, now time.Time) (auth.User, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE users SET active=?, version=version+1, updated_at=? WHERE id=? AND version=?`, boolInt(active), formatTime(now), id, version)
	if err != nil {
		return auth.User{}, mapError("set user active", err)
	}
	if err := requireUpdated(result, "set user active"); err != nil {
		return auth.User{}, err
	}
	return q.GetUserByID(ctx, id)
}

func (q *Queries) CreateSession(ctx context.Context, session auth.Session) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO sessions(
		id,user_id,token_hash,expires_at,revoked_at,created_at,last_seen_at
	) VALUES(?,?,?,?,?,?,?)`, session.ID, session.UserID, session.TokenHash, formatTime(session.ExpiresAt), nil,
		formatTime(session.CreatedAt), formatTime(session.LastSeenAt))
	return mapError("create session", err)
}

func (q *Queries) GetSessionByHash(ctx context.Context, hash string) (auth.Session, auth.User, error) {
	row := q.q.QueryRowContext(ctx, `SELECT
		s.id,s.user_id,s.token_hash,s.expires_at,s.revoked_at,s.created_at,s.last_seen_at,
		u.id,u.email,u.display_name,u.password_hash,u.role,u.active,u.version,u.created_at,u.updated_at
		FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=?`, hash)
	var session auth.Session
	var user auth.User
	var expires, created, seen, userCreated, userUpdated string
	var revoked sql.NullString
	var active int
	err := row.Scan(&session.ID, &session.UserID, &session.TokenHash, &expires, &revoked, &created, &seen,
		&user.ID, &user.Email, &user.DisplayName, &user.PasswordHash, &user.Role, &active, &user.Version, &userCreated, &userUpdated)
	if err != nil {
		return auth.Session{}, auth.User{}, mapError("get session", err)
	}
	session.ExpiresAt, err = parseTime(expires)
	if err != nil {
		return auth.Session{}, auth.User{}, err
	}
	session.RevokedAt, err = optionalTime(revoked)
	if err != nil {
		return auth.Session{}, auth.User{}, err
	}
	session.CreatedAt, err = parseTime(created)
	if err != nil {
		return auth.Session{}, auth.User{}, err
	}
	session.LastSeenAt, err = parseTime(seen)
	if err != nil {
		return auth.Session{}, auth.User{}, err
	}
	user.Active = active == 1
	user.CreatedAt, err = parseTime(userCreated)
	if err == nil {
		user.UpdatedAt, err = parseTime(userUpdated)
	}
	return session, user, err
}

func (q *Queries) TouchSession(ctx context.Context, id string, now time.Time) error {
	result, err := q.q.ExecContext(ctx, `UPDATE sessions SET last_seen_at=? WHERE id=? AND revoked_at IS NULL AND expires_at>?`, formatTime(now), id, formatTime(now))
	if err != nil {
		return mapError("touch session", err)
	}
	return requireUpdatedAs(result, "touch session", fault.ErrUnauthorized)
}

func (q *Queries) RevokeSession(ctx context.Context, id string, now time.Time) error {
	result, err := q.q.ExecContext(ctx, `UPDATE sessions SET revoked_at=? WHERE id=? AND revoked_at IS NULL`, formatTime(now), id)
	if err != nil {
		return mapError("revoke session", err)
	}
	return requireUpdatedAs(result, "revoke session", fault.ErrUnauthorized)
}

func (q *Queries) DeleteExpiredSessions(ctx context.Context, now time.Time, limit int) (int64, error) {
	limit = normalizeLimit(limit, 100)
	result, err := q.q.ExecContext(ctx, `DELETE FROM sessions WHERE id IN (
		SELECT id FROM sessions WHERE expires_at<=? OR revoked_at IS NOT NULL ORDER BY expires_at LIMIT ?
	)`, formatTime(now), limit)
	if err != nil {
		return 0, mapError("delete expired sessions", err)
	}
	count, err := result.RowsAffected()
	return count, mapError("count deleted sessions", err)
}

func requireUpdated(result sql.Result, op string) error {
	return requireUpdatedAs(result, op, fault.ErrVersion)
}

func requireUpdatedAs(result sql.Result, op string, missing error) error {
	count, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s rows affected: %w", op, err)
	}
	if count != 1 {
		return fmt.Errorf("%s: %w", op, missing)
	}
	return nil
}
