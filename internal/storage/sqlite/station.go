package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/station"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
)

const stationColumns = `id,code,name,latitude,longitude,elevation_m,timezone,status,maintenance_from,maintenance_until,version,created_at,updated_at`
const sensorColumns = `id,station_id,serial_number,channel,sample_rate_hz,installed_at,calibrated_at,disabled_at,version,created_at`

func (q *Queries) CreateStation(ctx context.Context, value station.Station) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO stations(
		id,code,name,latitude,longitude,elevation_m,timezone,status,maintenance_from,maintenance_until,version,created_at,updated_at
	) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)`, value.ID, value.Code, value.Name, value.Latitude, value.Longitude,
		value.ElevationM, value.Timezone, value.Status, nullableTime(value.MaintenanceFrom), nullableTime(value.MaintenanceUntil),
		value.Version, formatTime(value.CreatedAt), formatTime(value.UpdatedAt))
	return mapError("create station", err)
}

func (q *Queries) ReserveStationIdentity(ctx context.Context, value station.Station) error {
	if err := q.CreateStation(ctx, value); err != nil {
		return fmt.Errorf("reserve station identity: %w", err)
	}
	return nil
}

func (q *Queries) CreateSensor(ctx context.Context, value station.Sensor) error {
	_, err := q.q.ExecContext(ctx, `INSERT INTO sensors(
		id,station_id,serial_number,channel,sample_rate_hz,installed_at,calibrated_at,disabled_at,version,created_at
	) VALUES(?,?,?,?,?,?,?,?,?,?)`, value.ID, value.StationID, value.SerialNumber, value.Channel, value.SampleRateHz,
		formatTime(value.InstalledAt), formatTime(value.CalibratedAt), nullableTime(value.DisabledAt), value.Version, formatTime(value.CreatedAt))
	return mapError("create sensor", err)
}

func scanStation(scanner interface{ Scan(...any) error }) (station.Station, error) {
	var value station.Station
	var maintenanceFrom, maintenanceUntil sql.NullString
	var created, updated string
	err := scanner.Scan(&value.ID, &value.Code, &value.Name, &value.Latitude, &value.Longitude, &value.ElevationM,
		&value.Timezone, &value.Status, &maintenanceFrom, &maintenanceUntil, &value.Version, &created, &updated)
	if err != nil {
		return station.Station{}, err
	}
	value.MaintenanceFrom, err = optionalTime(maintenanceFrom)
	if err == nil {
		value.MaintenanceUntil, err = optionalTime(maintenanceUntil)
	}
	if err == nil {
		value.CreatedAt, err = parseTime(created)
	}
	if err == nil {
		value.UpdatedAt, err = parseTime(updated)
	}
	return value, err
}

func scanSensor(scanner interface{ Scan(...any) error }) (station.Sensor, error) {
	var value station.Sensor
	var installed, calibrated, created string
	var disabled sql.NullString
	err := scanner.Scan(&value.ID, &value.StationID, &value.SerialNumber, &value.Channel, &value.SampleRateHz,
		&installed, &calibrated, &disabled, &value.Version, &created)
	if err != nil {
		return station.Sensor{}, err
	}
	value.InstalledAt, err = parseTime(installed)
	if err == nil {
		value.CalibratedAt, err = parseTime(calibrated)
	}
	if err == nil {
		value.DisabledAt, err = optionalTime(disabled)
	}
	if err == nil {
		value.CreatedAt, err = parseTime(created)
	}
	return value, err
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}

func (q *Queries) GetStation(ctx context.Context, id string) (station.Station, error) {
	value, err := scanStation(q.q.QueryRowContext(ctx, "SELECT "+stationColumns+" FROM stations WHERE id=?", id))
	return value, mapError("get station", err)
}

func (q *Queries) GetStationByCode(ctx context.Context, code string) (station.Station, error) {
	value, err := scanStation(q.q.QueryRowContext(ctx, "SELECT "+stationColumns+" FROM stations WHERE code=?", code))
	return value, mapError("get station by code", err)
}

func (q *Queries) ListStations(ctx context.Context, filter repository.StationFilter) (repository.Page[station.Station], error) {
	filter.Limit = normalizeLimit(filter.Limit, 50)
	where := []string{"1=1"}
	args := make([]any, 0, 5)
	if filter.Status != "" {
		where = append(where, "status=?")
		args = append(args, filter.Status)
	}
	if value := strings.TrimSpace(filter.Search); value != "" {
		where = append(where, "(code LIKE ? OR name LIKE ?)")
		args = append(args, "%"+value+"%", "%"+value+"%")
	}
	if filter.After != "" {
		where = append(where, "code>?")
		args = append(args, filter.After)
	}
	args = append(args, filter.Limit+1)
	rows, err := q.q.QueryContext(ctx, "SELECT "+stationColumns+" FROM stations WHERE "+strings.Join(where, " AND ")+" ORDER BY code LIMIT ?", args...)
	if err != nil {
		return repository.Page[station.Station]{}, mapError("list stations", err)
	}
	defer rows.Close()
	items := make([]station.Station, 0, filter.Limit+1)
	for rows.Next() {
		item, err := scanStation(rows)
		if err != nil {
			return repository.Page[station.Station]{}, fmt.Errorf("scan station list: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return repository.Page[station.Station]{}, fmt.Errorf("iterate station list: %w", err)
	}
	page := repository.Page[station.Station]{Items: items}
	if len(items) > filter.Limit {
		page.NextCursor = items[filter.Limit-1].Code
		page.Items = items[:filter.Limit]
	}
	return page, nil
}

func (q *Queries) ListSensors(ctx context.Context, stationID string, includeDisabled bool) ([]station.Sensor, error) {
	query := "SELECT " + sensorColumns + " FROM sensors WHERE station_id=?"
	if !includeDisabled {
		query += " AND disabled_at IS NULL"
	}
	query += " ORDER BY channel"
	rows, err := q.q.QueryContext(ctx, query, stationID)
	if err != nil {
		return nil, mapError("list sensors", err)
	}
	defer rows.Close()
	var values []station.Sensor
	for rows.Next() {
		value, err := scanSensor(rows)
		if err != nil {
			return nil, fmt.Errorf("scan sensor list: %w", err)
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

func (q *Queries) GetSensor(ctx context.Context, id string) (station.Sensor, error) {
	value, err := scanSensor(q.q.QueryRowContext(ctx, "SELECT "+sensorColumns+" FROM sensors WHERE id=?", id))
	return value, mapError("get sensor", err)
}

func (q *Queries) UpdateStationState(ctx context.Context, id string, status station.Status, from, until *time.Time, version int64, now time.Time) (station.Station, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE stations SET status=?,maintenance_from=?,maintenance_until=?,version=version+1,updated_at=? WHERE id=? AND version=?`,
		status, nullableTime(from), nullableTime(until), formatTime(now), id, version)
	if err != nil {
		return station.Station{}, mapError("update station state", err)
	}
	if err := requireUpdated(result, "update station state"); err != nil {
		return station.Station{}, err
	}
	return q.GetStation(ctx, id)
}

func (q *Queries) UpdateStationCoordinates(ctx context.Context, id string, latitude, longitude, elevation float64, version int64, now time.Time) (station.Station, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE stations SET latitude=?,longitude=?,elevation_m=?,version=version+1,updated_at=? WHERE id=? AND version=?`,
		latitude, longitude, elevation, formatTime(now), id, version)
	if err != nil {
		return station.Station{}, mapError("update station coordinates", err)
	}
	if err := requireUpdated(result, "update station coordinates"); err != nil {
		return station.Station{}, err
	}
	return q.GetStation(ctx, id)
}

func (q *Queries) DisableSensor(ctx context.Context, id string, version int64, now time.Time) (station.Sensor, error) {
	result, err := q.q.ExecContext(ctx, `UPDATE sensors SET disabled_at=?,version=version+1 WHERE id=? AND version=? AND disabled_at IS NULL`, formatTime(now), id, version)
	if err != nil {
		return station.Sensor{}, mapError("disable sensor", err)
	}
	if err := requireUpdated(result, "disable sensor"); err != nil {
		return station.Sensor{}, err
	}
	return q.GetSensor(ctx, id)
}

func (q *Queries) CountEnabledSensors(ctx context.Context, stationID string) (int, time.Time, error) {
	var count int
	var latest sql.NullString
	err := q.q.QueryRowContext(ctx, `SELECT COUNT(*),MAX(calibrated_at) FROM sensors WHERE station_id=? AND disabled_at IS NULL`, stationID).Scan(&count, &latest)
	if err != nil {
		return 0, time.Time{}, mapError("count enabled sensors", err)
	}
	if count == 0 || !latest.Valid {
		return count, time.Time{}, nil
	}
	parsed, err := parseTime(latest.String)
	return count, parsed, err
}

func (q *Queries) assertStationActive(ctx context.Context, stationID string) error {
	value, err := q.GetStation(ctx, stationID)
	if err != nil {
		return err
	}
	if value.Status != station.StatusActive {
		return fmt.Errorf("%w: station is %s", fault.ErrInvalidState, value.Status)
	}
	return nil
}
