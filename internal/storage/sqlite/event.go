package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/event"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

const eventColumns = `id,public_id,detected_at,latitude,longitude,depth_km,magnitude,status,review_owner_id,review_lease_until,version,created_at,updated_at`
const pickColumns = `id,event_id,waveform_id,station_id,phase,picked_at,confidence,created_at`

func (q *Queries) CreateEvent(ctx context.Context, value event.Candidate) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO event_candidates(
		id,public_id,detected_at,latitude,longitude,depth_km,magnitude,status,review_owner_id,review_lease_until,version,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, value.ID, value.PublicID, formatTime(value.DetectedAt), value.Latitude,
		value.Longitude, value.DepthKM, value.Magnitude, value.Status, nullableString(value.ReviewOwnerID),
		nullableTime(value.ReviewLeaseUntil), value.Version, formatTime(value.CreatedAt), formatTime(value.UpdatedAt))
	return mapError("create event", err)
}

func (q *Queries) CreatePick(ctx context.Context, value event.Pick) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO phase_picks(
		id,event_id,waveform_id,station_id,phase,picked_at,confidence,created_at
	) VALUES(?,?,?,?,?,?,?,?)`, value.ID, value.EventID, value.WaveformID, value.StationID,
		value.Phase, formatTime(value.PickedAt), value.Confidence, formatTime(value.CreatedAt))
	return mapError("create phase pick", err)
}

func nullableString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func scanEvent(scanner interface{ Scan(...any) error }) (event.Candidate, error) {
	var value event.Candidate
	var detected, created, updated string
	var owner, lease sql.NullString
	err := scanner.Scan(&value.ID, &value.PublicID, &detected, &value.Latitude, &value.Longitude, &value.DepthKM,
		&value.Magnitude, &value.Status, &owner, &lease, &value.Version, &created, &updated)
	if err != nil {
		return event.Candidate{}, err
	}
	value.DetectedAt, err = parseTime(detected)
	if owner.Valid {
		value.ReviewOwnerID = &owner.String
	}
	if err == nil {
		value.ReviewLeaseUntil, err = optionalTime(lease)
	}
	if err == nil {
		value.CreatedAt, err = parseTime(created)
	}
	if err == nil {
		value.UpdatedAt, err = parseTime(updated)
	}
	return value, err
}

func scanPick(scanner interface{ Scan(...any) error }) (event.Pick, error) {
	var value event.Pick
	var picked, created string
	err := scanner.Scan(&value.ID, &value.EventID, &value.WaveformID, &value.StationID, &value.Phase, &picked, &value.Confidence, &created)
	if err != nil {
		return event.Pick{}, err
	}
	value.PickedAt, err = parseTime(picked)
	if err == nil {
		value.CreatedAt, err = parseTime(created)
	}
	return value, err
}

func (q *Queries) GetEvent(ctx context.Context, id string) (event.Candidate, error) {
	value, err := scanEvent(q.q.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM event_candidates WHERE id=?", id))
	return value, mapError("get event", err)
}

func (q *Queries) GetEventByPublicID(ctx context.Context, publicID string) (event.Candidate, error) {
	value, err := scanEvent(q.q.QueryRowContext(ctx, "SELECT "+eventColumns+" FROM event_candidates WHERE public_id=?", publicID))
	return value, mapError("get event by public id", err)
}

func (q *Queries) ListEvents(ctx context.Context, filter repository.EventFilter) (repository.Page[event.Candidate], error) {
	filter.Limit = normalizeLimit(filter.Limit, 50)
	where := []string{"1=1"}
	args := make([]any, 0, 7)
	if filter.Status != "" {
		where = append(where, "status=?")
		args = append(args, filter.Status)
	}
	if filter.Magnitude != nil {
		where = append(where, "magnitude>=?")
		args = append(args, *filter.Magnitude)
	}
	if filter.From != nil {
		where = append(where, "detected_at>=?")
		args = append(args, formatTime(*filter.From))
	}
	if filter.Until != nil {
		where = append(where, "detected_at<?")
		args = append(args, formatTime(*filter.Until))
	}
	if filter.After != "" {
		where = append(where, "id>?")
		args = append(args, filter.After)
	}
	args = append(args, filter.Limit+1)
	rows, err := q.q.QueryContext(ctx, "SELECT "+eventColumns+" FROM event_candidates WHERE "+strings.Join(where, " AND ")+" ORDER BY id LIMIT ?", args...)
	if err != nil {
		return repository.Page[event.Candidate]{}, mapError("list events", err)
	}
	defer rows.Close()
	items := make([]event.Candidate, 0, filter.Limit+1)
	for rows.Next() {
		item, err := scanEvent(rows)
		if err != nil {
			return repository.Page[event.Candidate]{}, fmt.Errorf("scan event list: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return repository.Page[event.Candidate]{}, err
	}
	page := repository.Page[event.Candidate]{Items: items}
	if len(items) > filter.Limit {
		page.NextCursor = items[filter.Limit-1].ID
		page.Items = items[:filter.Limit]
	}
	return page, nil
}

func (q *Queries) ListPicks(ctx context.Context, eventID string) ([]event.Pick, error) {
	rows, err := q.q.QueryContext(ctx, "SELECT "+pickColumns+" FROM phase_picks WHERE event_id=? ORDER BY picked_at,phase", eventID)
	if err != nil {
		return nil, mapError("list phase picks", err)
	}
	defer rows.Close()
	var values []event.Pick
	for rows.Next() {
		value, err := scanPick(rows)
		if err != nil {
			return nil, fmt.Errorf("scan phase pick: %w", err)
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (q *Queries) CountDistinctPickStations(ctx context.Context, eventID string) (int, error) {
	var count int
	err := q.q.QueryRowContext(ctx, "SELECT COUNT(DISTINCT station_id) FROM phase_picks WHERE event_id=?", eventID).Scan(&count)
	return count, mapError("count pick stations", err)
}

func (q *Queries) ClaimEvent(ctx context.Context, id, owner string, leaseUntil time.Time, version int64, now time.Time) (event.Candidate, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE event_candidates SET status='under_review',review_owner_id=?,review_lease_until=?,version=version+1,updated_at=?
		WHERE id=? AND version=? AND status IN ('detected','under_review') AND (review_lease_until IS NULL OR review_lease_until<=? OR review_owner_id=?)`,
		owner, formatTime(leaseUntil), formatTime(now), id, version, formatTime(now), owner)
	if err != nil {
		return event.Candidate{}, mapError("claim event", err)
	}
	if err := requireUpdatedAs(result, "claim event", fault.ErrConflict); err != nil {
		return event.Candidate{}, err
	}
	return q.GetEvent(ctx, id)
}

func (q *Queries) CreateDecision(ctx context.Context, value event.ReviewDecision) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO review_decisions(id,event_id,analyst_id,decision,notes,event_version,created_at)
		VALUES(?,?,?,?,?,?,?)`, value.ID, value.EventID, value.AnalystID, value.Decision, value.Notes, value.EventVersion, formatTime(value.CreatedAt))
	return mapError("create review decision", err)
}

func (q *Queries) DecideEvent(ctx context.Context, id string, status event.Status, version int64, now time.Time) (event.Candidate, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE event_candidates SET status=?,review_owner_id=NULL,review_lease_until=NULL,version=version+1,updated_at=?
		WHERE id=? AND version=? AND status='under_review'`, status, formatTime(now), id, version)
	if err != nil {
		return event.Candidate{}, mapError("decide event", err)
	}
	if err := requireUpdatedAs(result, "decide event", fault.ErrConflict); err != nil {
		return event.Candidate{}, err
	}
	return q.GetEvent(ctx, id)
}

func (q *Queries) PublishEvent(ctx context.Context, id string, version int64, now time.Time) (event.Candidate, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE event_candidates SET status='published',version=version+1,updated_at=?
		WHERE id=? AND version=? AND status='confirmed'`, formatTime(now), id, version)
	if err != nil {
		return event.Candidate{}, mapError("publish event", err)
	}
	if err := requireUpdatedAs(result, "publish event", fault.ErrConflict); err != nil {
		return event.Candidate{}, err
	}
	return q.GetEvent(ctx, id)
}
