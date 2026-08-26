package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/alert"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

const ruleColumns = `id,name,minimum_magnitude,min_latitude,max_latitude,min_longitude,max_longitude,destination,enabled,version,created_at,updated_at`
const deliveryColumns = `id,event_id,rule_id,status,attempt_count,next_attempt_at,lease_owner,lease_until,last_error,delivered_at,version,created_at,updated_at`

func (q *Queries) CreateAlertRule(ctx context.Context, value alert.Rule) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO alert_rules(
		id,name,minimum_magnitude,min_latitude,max_latitude,min_longitude,max_longitude,destination,enabled,version,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`, value.ID, value.Name, value.MinimumMagnitude, value.MinLatitude, value.MaxLatitude,
		value.MinLongitude, value.MaxLongitude, value.Destination, boolInt(value.Enabled), value.Version, formatTime(value.CreatedAt), formatTime(value.UpdatedAt))
	return mapError("create alert rule", err)
}

func scanRule(scanner interface{ Scan(...any) error }) (alert.Rule, error) {
	var value alert.Rule
	var enabled int
	var created, updated string
	err := scanner.Scan(&value.ID, &value.Name, &value.MinimumMagnitude, &value.MinLatitude, &value.MaxLatitude,
		&value.MinLongitude, &value.MaxLongitude, &value.Destination, &enabled, &value.Version, &created, &updated)
	if err != nil {
		return alert.Rule{}, err
	}
	value.Enabled = enabled == 1
	value.CreatedAt, err = parseTime(created)
	if err == nil {
		value.UpdatedAt, err = parseTime(updated)
	}
	return value, err
}

func scanDelivery(scanner interface{ Scan(...any) error }) (alert.Delivery, error) {
	var value alert.Delivery
	var next, created, updated string
	var owner, lease, delivered sql.NullString
	err := scanner.Scan(&value.ID, &value.EventID, &value.RuleID, &value.Status, &value.AttemptCount, &next,
		&owner, &lease, &value.LastError, &delivered, &value.Version, &created, &updated)
	if err != nil {
		return alert.Delivery{}, err
	}
	value.NextAttemptAt, err = parseTime(next)
	if owner.Valid {
		value.LeaseOwner = &owner.String
	}
	if err == nil {
		value.LeaseUntil, err = optionalTime(lease)
	}
	if err == nil {
		value.DeliveredAt, err = optionalTime(delivered)
	}
	if err == nil {
		value.CreatedAt, err = parseTime(created)
	}
	if err == nil {
		value.UpdatedAt, err = parseTime(updated)
	}
	return value, err
}

func (q *Queries) GetAlertRule(ctx context.Context, id string) (alert.Rule, error) {
	value, err := scanRule(q.q.QueryRowContext(ctx, "SELECT "+ruleColumns+" FROM alert_rules WHERE id=?", id))
	return value, mapError("get alert rule", err)
}

func (q *Queries) ListAlertRules(ctx context.Context, filter repository.AlertFilter) (repository.Page[alert.Rule], error) {
	filter.Limit = normalizeLimit(filter.Limit, 50)
	where := []string{"1=1"}
	args := make([]any, 0, 4)
	if filter.Enabled != nil {
		where = append(where, "enabled=?")
		args = append(args, boolInt(*filter.Enabled))
	}
	if filter.After != "" {
		where = append(where, "id>?")
		args = append(args, filter.After)
	}
	args = append(args, filter.Limit+1)
	rows, err := q.q.QueryContext(ctx, "SELECT "+ruleColumns+" FROM alert_rules WHERE "+strings.Join(where, " AND ")+" ORDER BY id LIMIT ?", args...)
	if err != nil {
		return repository.Page[alert.Rule]{}, mapError("list alert rules", err)
	}
	defer rows.Close()
	items := make([]alert.Rule, 0, filter.Limit+1)
	for rows.Next() {
		item, err := scanRule(rows)
		if err != nil {
			return repository.Page[alert.Rule]{}, err
		}
		items = append(items, item)
	}
	page := repository.Page[alert.Rule]{Items: items}
	if len(items) > filter.Limit {
		page.NextCursor = items[filter.Limit-1].ID
		page.Items = items[:filter.Limit]
	}
	return page, rows.Err()
}

func (q *Queries) UpdateAlertRule(ctx context.Context, value alert.Rule, version int64, now time.Time) (alert.Rule, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE alert_rules SET name=?,minimum_magnitude=?,min_latitude=?,max_latitude=?,min_longitude=?,max_longitude=?,destination=?,enabled=?,version=version+1,updated_at=? WHERE id=? AND version=?`,
		value.Name, value.MinimumMagnitude, value.MinLatitude, value.MaxLatitude, value.MinLongitude, value.MaxLongitude,
		value.Destination, boolInt(value.Enabled), formatTime(now), value.ID, version)
	if err != nil {
		return alert.Rule{}, mapError("update alert rule", err)
	}
	if err := requireUpdated(result, "update alert rule"); err != nil {
		return alert.Rule{}, err
	}
	return q.GetAlertRule(ctx, value.ID)
}

func (q *Queries) MatchingAlertRules(ctx context.Context, value alert.EventEnvelope) ([]alert.Rule, error) {
	rows, err := q.q.QueryContext(ctx, `SELECT `+ruleColumns+` FROM alert_rules WHERE enabled=1 AND minimum_magnitude<=?
		AND min_latitude<=? AND max_latitude>=? AND min_longitude<=? AND max_longitude>=? ORDER BY minimum_magnitude DESC,id`,
		value.Magnitude, value.Latitude, value.Latitude, value.Longitude, value.Longitude)
	if err != nil {
		return nil, mapError("match alert rules", err)
	}
	defer rows.Close()
	var values []alert.Rule
	for rows.Next() {
		value, err := scanRule(rows)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (q *Queries) CreateDelivery(ctx context.Context, value alert.Delivery) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO alert_deliveries(
		id,event_id,rule_id,status,attempt_count,next_attempt_at,lease_owner,lease_until,last_error,delivered_at,version,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, value.ID, value.EventID, value.RuleID, value.Status, value.AttemptCount,
		formatTime(value.NextAttemptAt), nullableString(value.LeaseOwner), nullableTime(value.LeaseUntil), value.LastError,
		nullableTime(value.DeliveredAt), value.Version, formatTime(value.CreatedAt), formatTime(value.UpdatedAt))
	return mapError("create alert delivery", err)
}

func (q *Queries) GetDelivery(ctx context.Context, id string) (alert.Delivery, error) {
	value, err := scanDelivery(q.q.QueryRowContext(ctx, "SELECT "+deliveryColumns+" FROM alert_deliveries WHERE id=?", id))
	return value, mapError("get alert delivery", err)
}

func (q *Queries) LeaseDeliveries(ctx context.Context, owner string, now, until time.Time, limit int) ([]alert.Delivery, error) {
	limit = normalizeLimit(limit, 10)
	rows, err := q.q.QueryContext(ctx, `UPDATE alert_deliveries SET status='leased',lease_owner=?,lease_until=?,attempt_count=attempt_count+1,version=version+1,updated_at=?
		WHERE id IN (SELECT id FROM alert_deliveries WHERE status IN ('pending','retry_wait','leased') AND next_attempt_at<=?
		AND (status!='leased' OR lease_until<=?) ORDER BY next_attempt_at,id LIMIT ?)
		RETURNING `+deliveryColumns, owner, formatTime(until), formatTime(now), formatTime(now), formatTime(now), limit)
	if err != nil {
		return nil, mapError("lease alert deliveries", err)
	}
	defer rows.Close()
	var values []alert.Delivery
	for rows.Next() {
		value, err := scanDelivery(rows)
		if err != nil {
			return nil, fmt.Errorf("scan leased alert delivery: %w", err)
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (q *Queries) CompleteDelivery(ctx context.Context, id, owner string, version int64, now time.Time) error {
	result, err := q.q.ExecContext(ctx, `UPDATE alert_deliveries SET status='delivered',delivered_at=?,lease_owner=NULL,lease_until=NULL,last_error='',version=version+1,updated_at=?
		WHERE id=? AND status='leased' AND lease_owner=? AND lease_until>? AND version=?`, formatTime(now), formatTime(now), id, owner, formatTime(now), version)
	if err != nil {
		return mapError("complete alert delivery", err)
	}
	return requireUpdatedAs(result, "complete alert delivery", fault.ErrLeaseLost)
}

func (q *Queries) FailDelivery(ctx context.Context, id, owner string, version int64, terminal bool, retryAt time.Time, message string) error {
	status := alert.StatusRetryWait
	if terminal {
		status = alert.StatusDead
	}
	result, err := q.q.ExecContext(ctx, `UPDATE alert_deliveries SET status=?,next_attempt_at=?,lease_owner=NULL,lease_until=NULL,last_error=?,version=version+1,updated_at=?
		WHERE id=? AND status='leased' AND lease_owner=? AND version=?`, status, formatTime(retryAt), message, formatTime(time.Now().UTC()), id, owner, version)
	if err != nil {
		return mapError("fail alert delivery", err)
	}
	return requireUpdatedAs(result, "fail alert delivery", fault.ErrLeaseLost)
}
