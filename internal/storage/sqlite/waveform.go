package sqlite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/waveform"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

const waveformColumns = `id,station_id,sensor_id,source_key,starts_at,ends_at,sample_count,checksum,status,rejection_reason,version,created_at,updated_at`

func (q *Queries) CreateWaveform(ctx context.Context, value waveform.Batch) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO waveform_batches(
		id,station_id,sensor_id,source_key,starts_at,ends_at,sample_count,checksum,status,rejection_reason,version,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, value.ID, value.StationID, value.SensorID, value.SourceKey,
		formatTime(value.StartsAt), formatTime(value.EndsAt), value.SampleCount, value.Checksum, value.Status,
		value.RejectionReason, value.Version, formatTime(value.CreatedAt), formatTime(value.UpdatedAt))
	return mapError("create waveform", err)
}

func scanWaveform(scanner interface{ Scan(...any) error }) (waveform.Batch, error) {
	var value waveform.Batch
	var starts, ends, created, updated string
	err := scanner.Scan(&value.ID, &value.StationID, &value.SensorID, &value.SourceKey, &starts, &ends,
		&value.SampleCount, &value.Checksum, &value.Status, &value.RejectionReason, &value.Version, &created, &updated)
	if err != nil {
		return waveform.Batch{}, err
	}
	value.StartsAt, err = parseTime(starts)
	if err == nil {
		value.EndsAt, err = parseTime(ends)
	}
	if err == nil {
		value.CreatedAt, err = parseTime(created)
	}
	if err == nil {
		value.UpdatedAt, err = parseTime(updated)
	}
	return value, err
}

func (q *Queries) GetWaveform(ctx context.Context, id string) (waveform.Batch, error) {
	value, err := scanWaveform(q.q.QueryRowContext(ctx, "SELECT "+waveformColumns+" FROM waveform_batches WHERE id=?", id))
	return value, mapError("get waveform", err)
}

func (q *Queries) GetWaveformBySource(ctx context.Context, sensorID, sourceKey string) (waveform.Batch, error) {
	value, err := scanWaveform(q.q.QueryRowContext(ctx, "SELECT "+waveformColumns+" FROM waveform_batches WHERE sensor_id=? AND source_key=?", sensorID, sourceKey))
	return value, mapError("get waveform by source", err)
}

func (q *Queries) ListWaveforms(ctx context.Context, filter repository.WaveformFilter) (repository.Page[waveform.Batch], error) {
	filter.Limit = normalizeLimit(filter.Limit, 50)
	where := []string{"1=1"}
	args := make([]any, 0, 8)
	if filter.StationID != "" {
		where = append(where, "station_id=?")
		args = append(args, filter.StationID)
	}
	if filter.SensorID != "" {
		where = append(where, "sensor_id=?")
		args = append(args, filter.SensorID)
	}
	if filter.Status != "" {
		where = append(where, "status=?")
		args = append(args, filter.Status)
	}
	if filter.From != nil {
		where = append(where, "ends_at>?")
		args = append(args, formatTime(*filter.From))
	}
	if filter.Until != nil {
		where = append(where, "starts_at<?")
		args = append(args, formatTime(*filter.Until))
	}
	if filter.After != "" {
		where = append(where, "id>?")
		args = append(args, filter.After)
	}
	args = append(args, filter.Limit+1)
	rows, err := q.q.QueryContext(ctx, "SELECT "+waveformColumns+" FROM waveform_batches WHERE "+strings.Join(where, " AND ")+" ORDER BY id LIMIT ?", args...)
	if err != nil {
		return repository.Page[waveform.Batch]{}, mapError("list waveforms", err)
	}
	defer rows.Close()
	items := make([]waveform.Batch, 0, filter.Limit+1)
	for rows.Next() {
		item, err := scanWaveform(rows)
		if err != nil {
			return repository.Page[waveform.Batch]{}, fmt.Errorf("scan waveform list: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return repository.Page[waveform.Batch]{}, err
	}
	page := repository.Page[waveform.Batch]{Items: items}
	if len(items) > filter.Limit {
		page.NextCursor = items[filter.Limit-1].ID
		page.Items = items[:filter.Limit]
	}
	return page, nil
}

func (q *Queries) UpdateWaveformStatus(ctx context.Context, id string, status waveform.Status, reason string, version int64, now time.Time) (waveform.Batch, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE waveform_batches SET status=?,rejection_reason=?,version=version+1,updated_at=? WHERE id=? AND version=?`,
		status, reason, formatTime(now), id, version)
	if err != nil {
		return waveform.Batch{}, mapError("update waveform status", err)
	}
	if err := requireUpdated(result, "update waveform status"); err != nil {
		return waveform.Batch{}, err
	}
	return q.GetWaveform(ctx, id)
}

func (q *Queries) AdvanceWaveformValidation(ctx context.Context, id string, version int64, now time.Time) (waveform.Batch, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE waveform_batches SET status='validated',rejection_reason='',version=version+1,updated_at=?
		WHERE id=? AND version=? AND status='received'`, formatTime(now), id, version)
	if err != nil {
		return waveform.Batch{}, mapError("advance waveform validation", err)
	}
	if err := requireUpdated(result, "advance waveform validation"); err != nil {
		return waveform.Batch{}, err
	}
	return q.GetWaveform(ctx, id)
}

func (q *Queries) HasWaveformOverlap(ctx context.Context, sensorID string, startsAt, endsAt time.Time) (bool, error) {
	var count int
	err := q.q.QueryRowContext(ctx, `SELECT COUNT(*) FROM waveform_batches
		WHERE sensor_id=? AND starts_at<? AND ends_at>? AND status!='rejected'`, sensorID, formatTime(endsAt), formatTime(startsAt)).Scan(&count)
	if err != nil {
		return false, mapError("check waveform overlap", err)
	}
	return count > 0, nil
}
