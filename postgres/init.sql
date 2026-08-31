-- Fleet Pipeline: PostgreSQL schema
-- Auto-run by the postgres container on first startup via docker-entrypoint-initdb.d

-- ============================================================
-- SPEED LAYER (Spark Structured Streaming writes here)
-- ============================================================

-- Live, windowed fleet utilization metrics by zone.
-- One row per (window, zone). Overwritten/appended every micro-batch.
CREATE TABLE IF NOT EXISTS live_fleet_utilization (
    window_start        TIMESTAMP NOT NULL,
    window_end           TIMESTAMP NOT NULL,
    zone                  VARCHAR(64) NOT NULL,
    active_vehicles       INTEGER NOT NULL DEFAULT 0,
    idle_vehicles         INTEGER NOT NULL DEFAULT 0,
    enroute_vehicles      INTEGER NOT NULL DEFAULT 0,
    on_trip_vehicles      INTEGER NOT NULL DEFAULT 0,
    idle_ratio            DOUBLE PRECISION,
    total_earnings        DOUBLE PRECISION NOT NULL DEFAULT 0,
    trip_count            INTEGER NOT NULL DEFAULT 0,
    avg_speed             DOUBLE PRECISION,
    ingested_at            TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, zone)
);

CREATE INDEX IF NOT EXISTS idx_live_util_window ON live_fleet_utilization (window_start DESC);

-- Per-vehicle last-known state, used for idle-time alerting.
-- Upserted on every micro-batch (latest event per vehicle wins).
CREATE TABLE IF NOT EXISTS vehicle_last_state (
    vehicle_id         VARCHAR(32) PRIMARY KEY,
    driver_id           VARCHAR(32),
    status                VARCHAR(16),
    zone                  VARCHAR(64),
    lat                   DOUBLE PRECISION,
    lon                   DOUBLE PRECISION,
    last_event_ts         TIMESTAMP NOT NULL,
    idle_since             TIMESTAMP,     -- set when status transitions into idle; cleared otherwise
    updated_at             TIMESTAMP NOT NULL DEFAULT now()
);

-- Raw trip-completion events, appended by Spark as they stream in.
-- This is the batch layer's source of truth for "what happened today" --
-- Airflow's daily DAG aggregates this table per vehicle and joins it
-- against the daily expense CSV to produce the reconciliation report.
-- Only on_trip events are appended (idle/enroute pings are not needed
-- for profitability reconciliation and would bloat this table).
CREATE TABLE IF NOT EXISTS raw_trip_events (
    id                     BIGSERIAL PRIMARY KEY,
    trip_id                VARCHAR(64),
    vehicle_id             VARCHAR(32) NOT NULL,
    driver_id              VARCHAR(32),
    zone                   VARCHAR(64),
    fare                   DOUBLE PRECISION,
    speed                  DOUBLE PRECISION,
    event_time             TIMESTAMP NOT NULL,
    ingested_at            TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_trip_events_vehicle_date
    ON raw_trip_events (vehicle_id, (event_time::date));
CREATE INDEX IF NOT EXISTS idx_raw_trip_events_time ON raw_trip_events (event_time);

-- Dead-letter queue: malformed/unparseable Kafka messages that failed
-- schema validation in the speed layer. Captures the raw payload plus a
-- reason so they can be inspected and, if needed, replayed later --
-- this is the ingestion-robustness / error-rate observability piece.
CREATE TABLE IF NOT EXISTS dlq_events (
    id           BIGSERIAL PRIMARY KEY,
    source_topic VARCHAR(128) NOT NULL,
    raw_payload  TEXT,
    error_reason VARCHAR(256),
    ingested_at  TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dlq_events_time ON dlq_events (ingested_at DESC);

-- ============================================================
-- BATCH LAYER (Airflow/Spark batch job writes here)
-- ============================================================

-- Daily per-vehicle profitability reconciliation.
CREATE TABLE IF NOT EXISTS daily_vehicle_reconciliation (
    sim_date               DATE NOT NULL,
    vehicle_id             VARCHAR(32) NOT NULL,
    trip_count             INTEGER NOT NULL DEFAULT 0,
    total_earnings         DOUBLE PRECISION NOT NULL DEFAULT 0,
    distance_covered       DOUBLE PRECISION,
    fuel_cost               DOUBLE PRECISION,
    maintenance_cost       DOUBLE PRECISION,
    total_cost               DOUBLE PRECISION,
    net_profit               DOUBLE PRECISION,
    is_unprofitable         BOOLEAN NOT NULL DEFAULT FALSE,
    service_flag             VARCHAR(4),
    computed_at               TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (sim_date, vehicle_id)
);

-- ============================================================
-- OBSERVABILITY
-- ============================================================

-- Alert log: threshold breaches (idle vehicles, no-data, error rate, etc.)
CREATE TABLE IF NOT EXISTS pipeline_alerts (
    id                     SERIAL PRIMARY KEY,
    alert_type             VARCHAR(64) NOT NULL,   -- e.g. 'vehicle_idle_too_long', 'no_data_received', 'batch_job_failed'
    severity                 VARCHAR(16) NOT NULL DEFAULT 'WARNING',
    component               VARCHAR(64) NOT NULL,
    message                 TEXT NOT NULL,
    details                   JSONB,
    triggered_at             TIMESTAMP NOT NULL DEFAULT now(),
    resolved_at               TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON pipeline_alerts (triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unresolved ON pipeline_alerts (resolved_at) WHERE resolved_at IS NULL;

-- Pipeline health heartbeats: last-seen timestamps per component, used by
-- the no-data health check.
CREATE TABLE IF NOT EXISTS pipeline_health (
    component               VARCHAR(64) PRIMARY KEY,   -- 'telemetry_stream', 'expense_batch', 'spark_streaming', 'airflow_daily_dag'
    last_heartbeat           TIMESTAMP NOT NULL DEFAULT now(),
    status                     VARCHAR(16) NOT NULL DEFAULT 'HEALTHY',
    detail                     TEXT
);
