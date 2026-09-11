# Fleet Pipeline — Ride-Hailing Fleet Operations (Lambda Architecture)

A data engineering mini-project implementing an end-to-end **Lambda architecture**
pipeline for a ride-hailing fleet operator: live fleet utilization tracking
via a streaming speed layer, and daily per-vehicle profitability reconciliation
via a batch layer — built on Kafka, Spark Structured Streaming, Airflow, and
PostgreSQL, fully containerized with Docker Compose.

## Architecture Summary

**Chosen architecture: Lambda** (not Kappa). Two genuinely different data
sources feed this system — continuous GPS/telemetry events, and a daily
batch file of garage/fuel expense records — with different consistency
needs (live metrics can be approximate and self-correcting; the daily
profitability reconciliation must be accurate and auditable). See
[`docs/architecture-decision.md`](docs/architecture-decision.md) for the
full justification and rejected alternatives.

```
                         ┌─────────────────────┐
   telemetry_producer.py │                      │
   (every 2s per vehicle)│                      │
   ─────────────────────▶│   Kafka              │
                         │   vehicle_telemetry  │
                         │                      │
                         └──────────┬───────────┘
                                    │
                    ┌───────────────┴───────────────┐
                    ▼                                ▼
        ┌───────────────────────┐      ┌──────────────────────────┐
        │  SPEED LAYER          │      │  raw_trip_events (PG)    │
        │  Spark Structured     │─────▶│  (source of truth for    │
        │  Streaming            │      │   the batch layer)       │
        │                       │      └──────────┬───────────────┘
        │  • live_fleet_        │                 │
        │    utilization        │                 │
        │  • vehicle_last_state │                 │
        │  • pipeline_alerts    │                 ▼
        │  • dlq_events         │      ┌──────────────────────────┐
        │  • pipeline_health    │      │  BATCH LAYER              │
        └───────────┬───────────┘      │  Airflow DAG (every       │
                    │                  │  simulated day)           │
                    │                  │                            │
                    │                  │  joins raw_trip_events     │
                    │                  │  against the daily expense │
                    │                  │  CSV → daily_vehicle_      │
                    │                  │  reconciliation            │
                    │                  └──────────┬─────────────────┘
                    │                             │
                    └──────────────┬──────────────┘
                                   ▼
                      ┌─────────────────────────┐
                      │  SERVING LAYER          │
                      │  FastAPI + dashboard    │
                      │  (localhost:8000)       │
                      └─────────────────────────┘

   expense_batch_source.py
   (1 CSV per simulated day) ──────▶ data/raw/expenses/ ──▶ read by Airflow
```

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| Ingestion (stream) | Apache Kafka | Ordered, replayable, partitioned per-vehicle event stream |
| Ingestion (batch) | Python script → CSV | Simulates an external fuel/garage partner's daily extract |
| Speed layer | Spark Structured Streaming | Windowed aggregation with watermarking, matches the streaming half of Lambda |
| Batch layer | Airflow (TaskFlow API) + SQL aggregation | Scheduled, retryable, auditable daily recompute |
| Storage | PostgreSQL | Single queryable store for both live and batch outputs, simple ops for a project of this scale |
| Serving | FastAPI + static HTML/JS dashboard | Read-only REST API + lightweight live dashboard, no separate BI tool needed |
| Observability | Structured JSON logs, `pipeline_health`, `pipeline_alerts`, `dlq_events` tables | See [`OBSERVABILITY.md`](OBSERVABILITY.md) |

## Project Structure

```
fleet-pipeline/
├── docker-compose.yml          # Full stack: Kafka, Postgres, Spark, Airflow, serving API
├── OBSERVABILITY.md            # Observability design doc
├── postgres/
│   └── init.sql                # Schema for all tables (auto-applied on first Postgres boot)
├── sources/                    # Simulated data sources (run locally, outside Docker)
│   ├── telemetry_producer.py
│   ├── expense_batch_source.py
│   └── requirements.txt
├── spark/                      # Speed layer
│   ├── speed_layer.py
│   └── Dockerfile
├── airflow/                    # Batch layer
│   └── dags/daily_reconciliation_dag.py
├── serving/                    # Serving layer (API + dashboard)
│   ├── api.py
│   ├── static/index.html
│   └── Dockerfile
└── data/                       # Runtime data (gitignored except folder structure)
    └── raw/expenses/           # Daily expense CSVs land here
```

## Prerequisites

- Docker Desktop (with WSL2 backend on Windows)
- Python 3.10+ (for running the simulated sources locally)
- ~4 GB free RAM for the container stack

## Setup & Run

### 1. Clone and set up a Python virtual environment for the sources

```bash
git clone <this-repo>
cd fleet-pipeline
python -m venv venv
# Windows:
.\venv\Scripts\Activate.ps1
# Mac/Linux:
source venv/bin/activate

cd sources
pip install -r requirements.txt
cd ..
```

### 2. Bring up the full container stack

```bash
docker compose up -d --build
```

First run downloads the Kafka, Postgres, Spark, and Airflow images and can
take several minutes. Verify everything is healthy:

```bash
docker ps --filter "name=fleet-"
```

You should see 5 containers: `fleet-kafka`, `fleet-postgres`,
`fleet-spark-streaming`, `fleet-airflow`, `fleet-serving-api`.

### 3. Start the simulated data sources

In one terminal (from `sources/`), start the streaming telemetry producer:

```bash
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"   # PowerShell
python telemetry_producer.py
```

In another terminal, generate a daily expense file (stop it with Ctrl+C
after it writes one file, or let it run to auto-generate a new one every
simulated day):

```bash
$env:EXPENSE_OUTPUT_DIR = "<absolute path to>/fleet-pipeline/data/raw/expenses"
python expense_batch_source.py
```

**Simulated clock:** 1 simulated day = 5 real minutes (`SIM_DAY_SECONDS=300`),
configurable via environment variable on both source scripts.

### 4. View the results

| What | Where |
|---|---|
| Live dashboard | http://localhost:8000 |
| API docs (Swagger) | http://localhost:8000/docs |
| Airflow UI | http://localhost:8082 (user: `admin`, password: printed in `docker logs fleet-airflow` on first boot) |
| Postgres (psql) | `docker exec -it fleet-postgres psql -U fleet -d fleet_db` |

The Airflow DAG `daily_vehicle_reconciliation` runs automatically every 5
minutes; trigger it manually from the UI (▶ button) to see results sooner.

## Reproducing Results

1. Follow Setup & Run above.
2. Let the telemetry producer run for at least 2–3 minutes so Spark has
   enough windowed data to write to `live_fleet_utilization`.
3. Trigger (or wait for) the Airflow DAG to populate
   `daily_vehicle_reconciliation`.
4. Open http://localhost:8000 — all panels should populate with live data.

## Troubleshooting

- **`kafka.errors.NoBrokersAvailable` from the producer**: make sure
  `KAFKA_BOOTSTRAP_SERVERS` is set to `localhost:29092` (the host-accessible
  listener), not `kafka:9092` (only resolvable inside the Docker network).
- **`bitnami/spark` / other image pull failures**: transient Docker Hub
  issues; retry `docker compose up -d --build`.
- **Port conflicts** (5432, 8080, 8081): this project uses non-default host
  ports (5433 for Postgres, 8082 for Airflow, 8000 for the API) specifically
  to avoid clashing with other local stacks — check `docker-compose.yml` if
  you still hit a conflict.
- **`raw_trip_events does not exist` in Airflow logs**: the Postgres schema
  only auto-applies `postgres/init.sql` on a first-time (empty) data volume.
  If you're reusing an existing volume from before a schema change, apply
  the missing table manually via `psql`.

## Assumptions & Simplifications

- Simulated time compression: 1 day = 5 real minutes.
- 20 vehicles, 4 fixed zones, single-node Spark (`local[2]`) and single-node
  Kafka broker (KRaft mode, no Zookeeper) — sized for a demo, not production
  throughput.
- Airflow runs in `standalone` mode (webserver + scheduler + triggerer in
  one container) for simplicity; a production deployment would split these.
- No authentication on the serving API (read-only, local-only scope).

## Documentation

- [`OBSERVABILITY.md`](OBSERVABILITY.md) — logging, metrics, alerting, and DLQ design
- [`docs/architecture-decision.md`](docs/architecture-decision.md) — Lambda vs Kappa justification
