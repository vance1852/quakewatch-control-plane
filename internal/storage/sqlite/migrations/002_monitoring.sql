CREATE TABLE waveform_batches (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES stations(id) ON DELETE RESTRICT,
    sensor_id TEXT NOT NULL REFERENCES sensors(id) ON DELETE RESTRICT,
    source_key TEXT NOT NULL,
    starts_at TEXT NOT NULL,
    ends_at TEXT NOT NULL,
    sample_count INTEGER NOT NULL CHECK (sample_count > 0),
    checksum TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('received','validated','processed','rejected')),
    rejection_reason TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(sensor_id, source_key),
    UNIQUE(sensor_id, starts_at, ends_at, checksum)
);
CREATE INDEX waveform_station_time_idx ON waveform_batches(station_id, starts_at, ends_at);
CREATE TABLE event_candidates (
    id TEXT PRIMARY KEY,
    public_id TEXT NOT NULL UNIQUE,
    detected_at TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    depth_km REAL NOT NULL,
    magnitude REAL NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('detected','under_review','confirmed','dismissed','published')),
    review_owner_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    review_lease_until TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX events_status_time_idx ON event_candidates(status, detected_at DESC);
CREATE TABLE phase_picks (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES event_candidates(id) ON DELETE CASCADE,
    waveform_id TEXT NOT NULL REFERENCES waveform_batches(id) ON DELETE RESTRICT,
    station_id TEXT NOT NULL REFERENCES stations(id) ON DELETE RESTRICT,
    phase TEXT NOT NULL CHECK (phase IN ('P','S')),
    picked_at TEXT NOT NULL,
    confidence REAL NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    created_at TEXT NOT NULL,
    UNIQUE(event_id, waveform_id, phase)
);
CREATE INDEX picks_event_station_idx ON phase_picks(event_id, station_id);
CREATE TABLE review_decisions (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES event_candidates(id) ON DELETE RESTRICT,
    analyst_id TEXT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    decision TEXT NOT NULL CHECK (decision IN ('confirm','dismiss')),
    notes TEXT NOT NULL,
    event_version INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(event_id, event_version)
);
