package stationsvc

import (
	"context"
	"fmt"
	"time"

	"github.com/vance1852/quakewatch-control-plane/internal/clock"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/auth"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/fault"
	"github.com/vance1852/quakewatch-control-plane/internal/domain/station"
	"github.com/vance1852/quakewatch-control-plane/internal/idgen"
	"github.com/vance1852/quakewatch-control-plane/internal/repository"
	"github.com/vance1852/quakewatch-control-plane/internal/service/shared"
)

type Service struct {
	store repository.Store
	tx    repository.Transactor
	clock clock.Clock
	ids   idgen.Generator
}

type StationDetail struct {
	Station station.Station  `json:"station"`
	Sensors []station.Sensor `json:"sensors"`
}

func New(store repository.Store, tx repository.Transactor, valueClock clock.Clock, ids idgen.Generator) *Service {
	return &Service{store: store, tx: tx, clock: valueClock, ids: ids}
}

func (s *Service) Register(ctx context.Context, principal auth.Principal, input station.RegisterInput) (StationDetail, error) {
	if err := principal.Require(auth.PermissionManageNetwork); err != nil {
		return StationDetail{}, err
	}
	validated, err := station.ValidateRegister(input)
	if err != nil {
		return StationDetail{}, err
	}
	now := s.clock.Now()
	stationValue := station.Station{
		ID:         s.ids.New("sta"),
		Code:       validated.Code,
		Name:       validated.Name,
		Latitude:   validated.Latitude,
		Longitude:  validated.Longitude,
		ElevationM: validated.ElevationM,
		Timezone:   validated.Timezone,
		Status:     station.StatusProvisioning,
		Version:    1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	sensorValue := station.Sensor{
		ID:           s.ids.New("sen"),
		StationID:    stationValue.ID,
		SerialNumber: validated.SensorSerial,
		Channel:      validated.SensorChannel,
		SampleRateHz: validated.SensorSampleRate,
		InstalledAt:  validated.InstalledAt.UTC(),
		CalibratedAt: validated.CalibratedAt.UTC(),
		Version:      1,
		CreatedAt:    now,
	}
	if err := s.store.ReserveStationIdentity(ctx, stationValue); err != nil {
		return StationDetail{}, fault.Wrap("reserve station identity", err)
	}
	err = s.tx.WithinTx(ctx, func(store repository.Store) error {
		if err := store.CreateSensor(ctx, sensorValue); err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "station.registered", "station", stationValue.ID, "success", map[string]any{
			"code":      stationValue.Code,
			"sensor_id": sensorValue.ID,
			"channel":   sensorValue.Channel,
		}, now)
	})
	if err != nil {
		return StationDetail{}, fault.Wrap("register station transaction", err)
	}
	return StationDetail{Station: stationValue, Sensors: []station.Sensor{sensorValue}}, nil
}

func (s *Service) Get(ctx context.Context, id string) (StationDetail, error) {
	value, err := s.store.GetStation(ctx, id)
	if err != nil {
		return StationDetail{}, err
	}
	sensors, err := s.store.ListSensors(ctx, id, true)
	if err != nil {
		return StationDetail{}, err
	}
	return StationDetail{Station: value, Sensors: sensors}, nil
}

func (s *Service) List(ctx context.Context, filter repository.StationFilter) (repository.Page[station.Station], error) {
	return s.store.ListStations(ctx, filter)
}

func (s *Service) Activate(ctx context.Context, principal auth.Principal, stationID string, version int64) (station.Station, error) {
	if err := principal.Require(auth.PermissionManageNetwork); err != nil {
		return station.Station{}, err
	}
	now := s.clock.Now()
	var updated station.Station
	err := s.tx.WithinTx(ctx, func(store repository.Store) error {
		current, err := store.GetStation(ctx, stationID)
		if err != nil {
			return err
		}
		if current.Version != version {
			return fault.ErrVersion
		}
		count, latest, err := store.CountEnabledSensors(ctx, stationID)
		if err != nil {
			return err
		}
		if err := current.CanTransition(station.StatusActive, count, latest, now); err != nil {
			return err
		}
		updated, err = store.UpdateStationState(ctx, stationID, station.StatusActive, nil, nil, version, now)
		if err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "station.activated", "station", stationID, "success", map[string]any{
			"previous_status": current.Status,
			"sensor_count":    count,
		}, now)
	})
	return updated, fault.Wrap("activate station transaction", err)
}

func (s *Service) ScheduleMaintenance(ctx context.Context, principal auth.Principal, stationID string, version int64, from, until time.Time) (station.Station, error) {
	if err := principal.Require(auth.PermissionManageNetwork); err != nil {
		return station.Station{}, err
	}
	now := s.clock.Now()
	window, err := station.ValidateMaintenance(from, until, now)
	if err != nil {
		return station.Station{}, err
	}
	var updated station.Station
	err = s.tx.WithinTx(ctx, func(store repository.Store) error {
		current, err := store.GetStation(ctx, stationID)
		if err != nil {
			return err
		}
		if current.Version != version {
			return fault.ErrVersion
		}
		if err := current.CanTransition(station.StatusMaintenance, 0, time.Time{}, now); err != nil {
			return err
		}
		overlaps, err := store.ListWaveforms(ctx, repository.WaveformFilter{
			StationID: stationID,
			From:      &window.From,
			Until:     &window.Until,
			Limit:     1,
		})
		if err != nil {
			return err
		}
		if len(overlaps.Items) > 0 {
			return fmt.Errorf("%w: maintenance overlaps accepted waveform %s", fault.ErrConflict, overlaps.Items[0].ID)
		}
		updated, err = store.UpdateStationState(ctx, stationID, station.StatusMaintenance, &window.From, &window.Until, version, now)
		if err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "station.maintenance_started", "station", stationID, "success", map[string]any{
			"from":  window.From,
			"until": window.Until,
		}, now)
	})
	return updated, fault.Wrap("schedule maintenance transaction", err)
}

func (s *Service) Retire(ctx context.Context, principal auth.Principal, stationID string, version int64) (station.Station, error) {
	if err := principal.Require(auth.PermissionManageNetwork); err != nil {
		return station.Station{}, err
	}
	now := s.clock.Now()
	var updated station.Station
	err := s.tx.WithinTx(ctx, func(store repository.Store) error {
		current, err := store.GetStation(ctx, stationID)
		if err != nil {
			return err
		}
		if current.Version != version {
			return fault.ErrVersion
		}
		if err := current.CanTransition(station.StatusRetired, 0, time.Time{}, now); err != nil {
			return err
		}
		updated, err = store.UpdateStationState(ctx, stationID, station.StatusRetired, nil, nil, version, now)
		if err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "station.retired", "station", stationID, "success", map[string]any{}, now)
	})
	return updated, err
}

func (s *Service) UpdateCoordinates(ctx context.Context, principal auth.Principal, stationID string, latitude, longitude, elevation float64, version int64) (station.Station, error) {
	if err := principal.Require(auth.PermissionManageNetwork); err != nil {
		return station.Station{}, err
	}
	if err := station.ValidateCoordinates(latitude, longitude, elevation); err != nil {
		return station.Station{}, err
	}
	now := s.clock.Now()
	var updated station.Station
	err := s.tx.WithinTx(ctx, func(store repository.Store) error {
		var err error
		updated, err = store.UpdateStationCoordinates(ctx, stationID, latitude, longitude, elevation, version, now)
		if err != nil {
			return err
		}
		return shared.Audit(ctx, store, s.ids, &principal.UserID, "station.coordinates_updated", "station", stationID, "success", map[string]any{
			"latitude":    latitude,
			"longitude":   longitude,
			"elevation_m": elevation,
		}, now)
	})
	return updated, err
}
