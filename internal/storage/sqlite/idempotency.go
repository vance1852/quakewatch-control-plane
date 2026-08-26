package sqlite

import (
	"context"
	"database/sql"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

func (q *Queries) CreateIdempotency(ctx context.Context, value repository.IdempotencyRecord) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO idempotency_keys(
		id,actor_id,method,path,key,request_hash,response_code,response_json,expires_at,created_at
	) VALUES(?,?,?,?,?,?,?,?,?,?)`, value.ID, value.ActorID, value.Method, value.Path, value.Key,
		value.RequestHash, nullableInt(value.ResponseCode), nullableResponse(value.ResponseCode, value.ResponseJSON),
		formatTime(value.ExpiresAt), formatTime(value.CreatedAt))
	return mapError("create idempotency record", err)
}

func nullableInt(value *int) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableResponse(code *int, value string) any {
	if code == nil {
		return nil
	}
	return value
}

func (q *Queries) GetIdempotency(ctx context.Context, actorID, method, path, key string) (repository.IdempotencyRecord, error) {
	var value repository.IdempotencyRecord
	var code sql.NullInt64
	var response sql.NullString
	var expires, created string
	err := q.q.QueryRowContext(ctx, `SELECT id,actor_id,method,path,key,request_hash,response_code,response_json,expires_at,created_at
		FROM idempotency_keys WHERE actor_id=? AND method=? AND path=? AND key=?`, actorID, method, path, key).Scan(
		&value.ID, &value.ActorID, &value.Method, &value.Path, &value.Key, &value.RequestHash, &code, &response, &expires, &created)
	if err != nil {
		return repository.IdempotencyRecord{}, mapError("get idempotency record", err)
	}
	if code.Valid {
		converted := int(code.Int64)
		value.ResponseCode = &converted
		value.ResponseJSON = response.String
	}
	value.ExpiresAt, err = parseTime(expires)
	if err == nil {
		value.CreatedAt, err = parseTime(created)
	}
	return value, err
}

func (q *Queries) CompleteIdempotency(ctx context.Context, id string, code int, response string) error {
	result, err := q.q.ExecContext(ctx, `UPDATE idempotency_keys SET response_code=?,response_json=? WHERE id=? AND response_code IS NULL`, code, response, id)
	if err != nil {
		return mapError("complete idempotency record", err)
	}
	return requireUpdated(result, "complete idempotency record")
}

func (q *Queries) DeleteIdempotency(ctx context.Context, id string) error {
	_, err := q.q.ExecContext(ctx, `DELETE FROM idempotency_keys WHERE id=? AND response_code IS NULL`, id)
	return mapError("delete idempotency record", err)
}

func (q *Queries) DeleteExpiredIdempotency(ctx context.Context, now time.Time, limit int) (int64, error) {
	limit = normalizeLimit(limit, 100)
	result, err := q.q.ExecContext(ctx, `DELETE FROM idempotency_keys WHERE id IN (
		SELECT id FROM idempotency_keys WHERE expires_at<=? ORDER BY expires_at LIMIT ?
	)`, formatTime(now), limit)
	if err != nil {
		return 0, mapError("delete expired idempotency", err)
	}
	count, err := result.RowsAffected()
	return count, mapError("count deleted idempotency", err)
}
