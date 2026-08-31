"""
Serving layer: FastAPI app exposing the pipeline's outputs as REST endpoints,
plus a static HTML/JS dashboard (served at "/") that polls these endpoints.

This is the "serving" layer of the Lambda architecture -- it reads from
Postgres (populated by the speed layer / batch layer) and never writes to
the pipeline itself. Read-only by design.

Endpoints:
    GET /api/health                 pipeline component heartbeats + status
    GET /api/utilization/live       latest live_fleet_utilization windows
    GET /api/utilization/by-zone    utilization aggregated over the last N minutes, per zone
    GET /api/vehicles               current vehicle_last_state snapshot
    GET /api/alerts                 recent pipeline_alerts (idle vehicles, unprofitable, etc.)
    GET /api/reconciliation/daily   daily_vehicle_reconciliation rows for a given date
    GET /api/reconciliation/dates   list of sim_dates available in reconciliation table
"""

import os
from datetime import date, datetime
from typing import Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "fleet_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "fleet")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "fleet_pw")

app = FastAPI(
    title="Fleet Pipeline Serving API",
    description="Read-only API over the Lambda-architecture fleet data pipeline",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


def pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=5,
        cursor_factory=psycopg2.extras.RealDictCursor,
    )


def to_jsonable(rows):
    """Convert datetime/date objects in query results to ISO strings."""
    out = []
    for row in rows:
        d = dict(row)
        for k, v in d.items():
            if isinstance(v, (datetime, date)):
                d[k] = v.isoformat()
        out.append(d)
    return out


@app.get("/api/health")
def get_health():
    """Pipeline component heartbeats -- the basis of the 'no data received' health check."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT component, last_heartbeat, status, detail,
                       EXTRACT(EPOCH FROM (now() - last_heartbeat)) AS seconds_since_heartbeat
                FROM pipeline_health
                ORDER BY component
                """
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()

    # Flag any component that hasn't reported in over 2 minutes as stale,
    # regardless of what status it last wrote -- this is the "no data
    # received in N minutes" health rule from the assignment brief.
    # Airflow's DAG only runs every 5 minutes by design, so it gets a
    # longer allowance than the near-continuous Spark streaming job.
    STALE_THRESHOLD_SECONDS = {
        "airflow_daily_dag": 360,   # DAG runs every 5 min; allow one missed cycle
        "spark_streaming": 120,
    }
    for r in rows:
        threshold = STALE_THRESHOLD_SECONDS.get(r["component"], 120)
        if r["seconds_since_heartbeat"] is not None and r["seconds_since_heartbeat"] > threshold:
            r["status"] = "STALE"

    overall = "HEALTHY" if all(r["status"] in ("OK", "HEALTHY") for r in rows) else "DEGRADED"
    return {"overall_status": overall, "components": rows}


@app.get("/api/utilization/live")
def get_live_utilization(limit: int = Query(20, ge=1, le=200)):
    """Most recent windowed utilization rows across all zones."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT window_start, window_end, zone, active_vehicles,
                       idle_vehicles, enroute_vehicles, on_trip_vehicles,
                       idle_ratio, total_earnings, trip_count, avg_speed
                FROM live_fleet_utilization
                ORDER BY window_start DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"count": len(rows), "windows": rows}


@app.get("/api/utilization/by-zone")
def get_utilization_by_zone(minutes: int = Query(10, ge=1, le=1440)):
    """Utilization summed/averaged over the last N minutes, one row per zone --
    this is what the dashboard's zone cards render."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT zone,
                       AVG(active_vehicles) AS avg_active_vehicles,
                       AVG(idle_ratio) AS avg_idle_ratio,
                       SUM(total_earnings) AS total_earnings,
                       SUM(trip_count) AS total_trips,
                       AVG(avg_speed) AS avg_speed
                FROM live_fleet_utilization
                WHERE window_start >= now() - (%s || ' minutes')::interval
                GROUP BY zone
                ORDER BY zone
                """,
                (minutes,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"window_minutes": minutes, "zones": rows}


@app.get("/api/vehicles")
def get_vehicles():
    """Current snapshot of every vehicle's last known state."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT vehicle_id, driver_id, status, zone, lat, lon,
                       last_event_ts, idle_since, updated_at
                FROM vehicle_last_state
                ORDER BY vehicle_id
                """
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"count": len(rows), "vehicles": rows}


@app.get("/api/alerts")
def get_alerts(
    limit: int = Query(50, ge=1, le=500),
    unresolved_only: bool = Query(False),
):
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            where_clause = "WHERE resolved_at IS NULL" if unresolved_only else ""
            cur.execute(
                f"""
                SELECT id, alert_type, severity, component, message, details,
                       triggered_at, resolved_at
                FROM pipeline_alerts
                {where_clause}
                ORDER BY triggered_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"count": len(rows), "alerts": rows}


@app.get("/api/reconciliation/dates")
def get_reconciliation_dates():
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT sim_date FROM daily_vehicle_reconciliation ORDER BY sim_date DESC"
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"dates": [r["sim_date"] for r in rows]}


@app.get("/api/reconciliation/daily")
def get_daily_reconciliation(sim_date: Optional[str] = Query(None)):
    """Per-vehicle profitability for a given sim_date (defaults to the most recent)."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            if sim_date is None:
                cur.execute("SELECT MAX(sim_date) AS d FROM daily_vehicle_reconciliation")
                latest = cur.fetchone()
                if not latest or not latest["d"]:
                    return {"sim_date": None, "vehicles": []}
                sim_date = latest["d"].isoformat()

            cur.execute(
                """
                SELECT sim_date, vehicle_id, trip_count, total_earnings,
                       distance_covered, fuel_cost, maintenance_cost,
                       total_cost, net_profit, is_unprofitable, service_flag,
                       computed_at
                FROM daily_vehicle_reconciliation
                WHERE sim_date = %s
                ORDER BY net_profit ASC
                """,
                (sim_date,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()

    if not rows:
        raise HTTPException(status_code=404, detail=f"no reconciliation data for {sim_date}")

    unprofitable_count = sum(1 for r in rows if r["is_unprofitable"])
    return {
        "sim_date": sim_date,
        "vehicle_count": len(rows),
        "unprofitable_count": unprofitable_count,
        "vehicles": rows,
    }


@app.get("/api/dlq")
def get_dlq_events(limit: int = Query(50, ge=1, le=500)):
    """Recent malformed/dead-lettered Kafka messages -- ingestion error visibility."""
    conn = pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, source_topic, error_reason, ingested_at
                FROM dlq_events
                ORDER BY ingested_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = to_jsonable(cur.fetchall())
    finally:
        conn.close()
    return {"count": len(rows), "events": rows}


# ---- Dashboard (static HTML/JS, polls the endpoints above) ----
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def dashboard():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
