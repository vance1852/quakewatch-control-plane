CREATE TABLE users (
    id TEXT PRIMARY KEY,
    email TEXT NOT NULL COLLATE NOCASE UNIQUE,
    display_name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    role TEXT NOT NULL CHECK (role IN ('analyst','operator','admin')),
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0,1)),
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash TEXT NOT NULL UNIQUE,
    expires_at TEXT NOT NULL,
    revoked_at TEXT,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL
);
CREATE INDEX sessions_user_expiry_idx ON sessions(user_id, expires_at);
CREATE TABLE stations (
    id TEXT PRIMARY KEY,
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL,
    elevation_m REAL NOT NULL,
    timezone TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('provisioning','active','maintenance','retired')),
    maintenance_from TEXT,
    maintenance_until TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX stations_status_code_idx ON stations(status, code);
CREATE TABLE sensors (
    id TEXT PRIMARY KEY,
    station_id TEXT NOT NULL REFERENCES stations(id) ON DELETE RESTRICT,
    serial_number TEXT NOT NULL UNIQUE,
    channel TEXT NOT NULL,
    sample_rate_hz REAL NOT NULL,
    installed_at TEXT NOT NULL,
    calibrated_at TEXT NOT NULL,
    disabled_at TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    UNIQUE(station_id, channel)
);
CREATE INDEX sensors_station_enabled_idx ON sensors(station_id, disabled_at);
