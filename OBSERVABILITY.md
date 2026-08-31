# Observability Design

This document describes what the pipeline measures, how, and why -- covering
the "Observability" rubric criterion (structured logging, metrics, tracing
to detect and diagnose pipeline failures).

## 1. Structured Logging

Every component (both simulated sources, the Spark speed layer, and the
Airflow batch DAG) logs in a consistent JSON shape:

```json
{"ts": "...", "level": "INFO", "component": "telemetry_producer", "msg": "..."}
```

This makes logs greppable/parseable uniformly across `docker logs` for any
container, regardless of which layer produced them -- useful both for local
debugging and for feeding into a log aggregator in a production deployment
(out of scope here, but the structured format is what enables it).

## 2. Metrics / Storage-backed Observability

Rather than a separate metrics stack (Prometheus/Grafana, out of scope for
this mini-project), observability signals are written directly to
PostgreSQL tables that the serving API exposes as JSON, and the dashboard
renders live:

| Table | Written by | Purpose |
|---|---|---|
| `pipeline_health` | Spark speed layer, Airflow DAG | Per-component heartbeat: last-seen timestamp, status (OK/WARN/ERROR), and a human-readable detail string. Basis of the "no data received in N minutes" health check. |
| `pipeline_alerts` | Spark speed layer, Airflow DAG | Append-only alert log: idle-vehicle alerts, unprofitable-vehicle alerts, and high-error-rate alerts (see below). Each row has a severity, a message, and a JSON `details` blob for drill-down. |
| `dlq_events` | Spark speed layer | Dead-letter queue for Kafka messages that failed schema validation. Captures the raw payload and a reason so bad data is inspectable rather than silently dropped. |

## 3. Health Check: "No Data Received in N Minutes"

`GET /api/health` computes `seconds_since_heartbeat` for every component and
flags it `STALE` if that exceeds a threshold -- but the threshold is
component-specific, because components have different natural cadences:

- `spark_streaming`: 120s threshold (writes a heartbeat every 30s micro-batch trigger; anything past 2 missed cycles indicates the stream has actually gone quiet, e.g. Kafka down or producer stopped).
- `airflow_daily_dag`: 360s threshold (only runs once per simulated day, i.e. every 5 real minutes; a shorter threshold would falsely flag it as stale between scheduled runs).

This distinction matters: a fixed threshold across components either flags
Airflow as constantly "stale" (false positive) or would be too lenient to
catch a genuinely stalled Spark job (false negative).

## 4. Alert Rules

| Alert type | Trigger | Severity | Raised by |
|---|---|---|---|
| `vehicle_idle_too_long` | A vehicle's continuous idle streak exceeds `IDLE_ALERT_THRESHOLD_MIN` (default 3 min) | WARNING | Spark speed layer |
| `vehicles_unprofitable` | Any vehicle's daily net profit (earnings − fuel − maintenance) is negative | WARNING | Airflow batch DAG |
| `high_error_rate` | A single micro-batch has ≥ `DLQ_ALERT_THRESHOLD` (default 5) malformed/unparseable Kafka messages | ERROR | Spark speed layer |

Idle-duration tracking is accurate (not just an idle *count*) because
`vehicle_last_state.idle_since` is preserved across micro-batches via an
upsert keyed on `vehicle_id` -- the idle streak length is computed as
`current_event_time - idle_since`, not approximated from a single window.

## 5. Ingestion Robustness (Dead-Letter Queue)

The speed layer splits every incoming Kafka message into two paths based on
schema validation (`vehicle_id IS NULL` after `from_json` indicates a
parse/schema failure):

- **Valid events** flow into the normal processing pipeline (utilization
  windows, vehicle state, trip events).
- **Malformed events** are routed to `dlq_events` with their raw payload
  and reason, and never silently dropped. If malformed volume in a single
  micro-batch crosses the threshold, a `high_error_rate` alert fires.

This means a burst of bad data (e.g. a misconfigured producer, a schema
change) is both recorded for later inspection and immediately surfaced as
an alert, rather than failing silently or crashing the stream.

## 6. Where to See All of This

- **Dashboard** (`http://localhost:8000`): live view of health, zone
  utilization, recent alerts, and the daily reconciliation table.
- **API docs** (`http://localhost:8000/docs`): interactive Swagger UI for
  every endpoint, including `/api/health`, `/api/alerts`, and `/api/dlq`.
- **Container logs**: `docker logs -f fleet-spark-streaming` /
  `docker logs -f fleet-airflow` for raw structured log lines.
- **Airflow UI** (`http://localhost:8082`): task-level graph, logs, and
  retry history for the batch DAG.
