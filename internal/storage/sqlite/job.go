package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/job"
)

const jobColumns = `id,kind,aggregate_id,payload_json,status,attempt_count,max_attempts,next_attempt_at,lease_owner,lease_until,last_error,version,created_at,updated_at`

func (q *Queries) CreateJob(ctx context.Context, value job.Job) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO worker_jobs(
		id,kind,aggregate_id,payload_json,status,attempt_count,max_attempts,next_attempt_at,lease_owner,lease_until,last_error,version,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, value.ID, value.Kind, value.AggregateID, value.PayloadJSON, value.Status,
		value.AttemptCount, value.MaxAttempts, formatTime(value.NextAttemptAt), nullableString(value.LeaseOwner), nullableTime(value.LeaseUntil),
		value.LastError, value.Version, formatTime(value.CreatedAt), formatTime(value.UpdatedAt))
	return mapError("create worker job", err)
}

func scanJob(scanner interface{ Scan(...any) error }) (job.Job, error) {
	var value job.Job
	var next, created, updated string
	var owner, lease sql.NullString
	err := scanner.Scan(&value.ID, &value.Kind, &value.AggregateID, &value.PayloadJSON, &value.Status,
		&value.AttemptCount, &value.MaxAttempts, &next, &owner, &lease, &value.LastError, &value.Version, &created, &updated)
	if err != nil {
		return job.Job{}, err
	}
	value.NextAttemptAt, err = parseTime(next)
	if owner.Valid {
		value.LeaseOwner = &owner.String
	}
	if err == nil {
		value.LeaseUntil, err = optionalTime(lease)
	}
	if err == nil {
		value.CreatedAt, err = parseTime(created)
	}
	if err == nil {
		value.UpdatedAt, err = parseTime(updated)
	}
	return value, err
}

func (q *Queries) GetJob(ctx context.Context, id string) (job.Job, error) {
	value, err := scanJob(q.q.QueryRowContext(ctx, "SELECT "+jobColumns+" FROM worker_jobs WHERE id=?", id))
	return value, mapError("get worker job", err)
}

func (q *Queries) LeaseJobs(ctx context.Context, owner string, now, until time.Time, limit int) ([]job.Job, error) {
	limit = normalizeLimit(limit, 10)
	rows, err := q.q.QueryContext(ctx, `UPDATE worker_jobs SET status='leased',lease_owner=?,lease_until=?,attempt_count=attempt_count+1,version=version+1,updated_at=?
		WHERE id IN (SELECT id FROM worker_jobs WHERE status IN ('pending','retry_wait','leased') AND next_attempt_at<=?
		AND (status!='leased' OR lease_until<=?) ORDER BY next_attempt_at,id LIMIT ?)
		RETURNING `+jobColumns, owner, formatTime(until), formatTime(now), formatTime(now), formatTime(now), limit)
	if err != nil {
		return nil, mapError("lease worker jobs", err)
	}
	defer rows.Close()
	var values []job.Job
	for rows.Next() {
		value, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan leased worker job: %w", err)
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (q *Queries) CompleteJob(ctx context.Context, id, owner string, version int64, now time.Time) error {
	result, err := q.q.ExecContext(ctx, `UPDATE worker_jobs SET status='completed',lease_owner=NULL,lease_until=NULL,last_error='',version=version+1,updated_at=?
		WHERE id=? AND status='leased' AND lease_owner=? AND lease_until>? AND version=?`, formatTime(now), id, owner, formatTime(now), version)
	if err != nil {
		return mapError("complete worker job", err)
	}
	return requireUpdatedAs(result, "complete worker job", fault.ErrLeaseLost)
}

func (q *Queries) FailJob(ctx context.Context, id, owner string, version int64, terminal bool, retryAt time.Time, message string) error {
	status := job.StatusRetryWait
	if terminal {
		status = job.StatusDead
	}
	result, err := q.q.ExecContext(ctx, `UPDATE worker_jobs SET status=?,next_attempt_at=?,lease_owner=NULL,lease_until=NULL,last_error=?,version=version+1,updated_at=?
		WHERE id=? AND status='leased' AND lease_owner=? AND version=?`, status, formatTime(retryAt), message, formatTime(time.Now().UTC()), id, owner, version)
	if err != nil {
		return mapError("fail worker job", err)
	}
	return requireUpdatedAs(result, "fail worker job", fault.ErrLeaseLost)
}

func (q *Queries) RecoverExpiredJobs(ctx context.Context, now time.Time) (int64, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE worker_jobs SET status='retry_wait',lease_owner=NULL,lease_until=NULL,next_attempt_at=?,version=version+1,updated_at=?
		WHERE status='leased' AND lease_until<=?`, formatTime(now), formatTime(now), formatTime(now))
	if err != nil {
		return 0, mapError("recover expired worker jobs", err)
	}
	count, err := result.RowsAffected()
	return count, mapError("count recovered worker jobs", err)
}
