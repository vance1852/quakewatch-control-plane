CREATE TABLE alert_rules (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    minimum_magnitude REAL NOT NULL,
    min_latitude REAL NOT NULL,
    max_latitude REAL NOT NULL,
    min_longitude REAL NOT NULL,
    max_longitude REAL NOT NULL,
    destination TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0,1)),
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX alert_rules_enabled_magnitude_idx ON alert_rules(enabled, minimum_magnitude);
CREATE TABLE alert_deliveries (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES event_candidates(id) ON DELETE RESTRICT,
    rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE RESTRICT,
    status TEXT NOT NULL CHECK (status IN ('pending','leased','delivered','retry_wait','dead')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_until TEXT,
    last_error TEXT,
    delivered_at TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(event_id, rule_id)
);
CREATE INDEX deliveries_claim_idx ON alert_deliveries(status, next_attempt_at, lease_until);
CREATE TABLE worker_jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('process_waveform','deliver_alert','cleanup_sessions')),
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','leased','retry_wait','completed','dead')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL,
    next_attempt_at TEXT NOT NULL,
    lease_owner TEXT,
    lease_until TEXT,
    last_error TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(kind, aggregate_id)
);
CREATE INDEX worker_jobs_claim_idx ON worker_jobs(status, next_attempt_at, lease_until);
CREATE TABLE idempotency_keys (
    id TEXT PRIMARY KEY,
    actor_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    method TEXT NOT NULL,
    path TEXT NOT NULL,
    key TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    response_code INTEGER,
    response_json TEXT,
    expires_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(actor_id, method, path, key)
);
CREATE INDEX idempotency_expiry_idx ON idempotency_keys(expires_at);
CREATE TABLE audit_events (
    id TEXT PRIMARY KEY,
    actor_id TEXT REFERENCES users(id) ON DELETE SET NULL,
    request_id TEXT NOT NULL,
    action TEXT NOT NULL,
    object_type TEXT NOT NULL,
    object_id TEXT NOT NULL,
    result TEXT NOT NULL,
    metadata_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX audit_object_time_idx ON audit_events(object_type, object_id, created_at DESC);
CREATE INDEX audit_actor_time_idx ON audit_events(actor_id, created_at DESC);
