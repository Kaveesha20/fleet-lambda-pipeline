"""
Batch layer DAG: daily per-vehicle profitability reconciliation.

This is the BATCH side of the Lambda architecture. Once per simulated day
(scheduled to match SIM_DAY_SECONDS = 5 real minutes -> every 5 minutes),
it:

    1. Waits for that day's expense CSV to be dropped by expense_batch_source.py
    2. Aggregates raw_trip_events (written by the Spark speed layer) per
       vehicle for that sim date: trip_count, total_earnings
    3. Joins that against the expense CSV (fuel_cost, maintenance_cost,
       distance_covered, service_flag) on vehicle_id
    4. Computes net_profit and flags unprofitable vehicles
    5. Upserts the result into daily_vehicle_reconciliation
    6. Writes a heartbeat + raises a pipeline_alerts row if the expense
       file never showed up (observability requirement)

Architecture note: this recompute-from-source-of-truth pattern (full daily
aggregation + join, not incremental streaming state) is exactly why this
is the BATCH layer of Lambda -- it trades latency for auditability and
correctness, unlike the speed layer's approximate, continuously-updating
view.
"""

from __future__ import annotations

import csv
import glob
import logging
import os
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

logger = logging.getLogger("daily_reconciliation_dag")

POSTGRES_HOST = os.environ.get("FLEET_POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("FLEET_POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("FLEET_POSTGRES_DB", "fleet_db")
POSTGRES_USER = os.environ.get("FLEET_POSTGRES_USER", "fleet")
POSTGRES_PASSWORD = os.environ.get("FLEET_POSTGRES_PASSWORD", "fleet_pw")
EXPENSE_DIR = os.environ.get("FLEET_EXPENSE_DIR", "/opt/airflow/data/raw/expenses")


def pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=10,
    )


def log_health(component: str, status: str, detail: str):
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_health (component, last_heartbeat, status, detail)
                VALUES (%s, now(), %s, %s)
                ON CONFLICT (component) DO UPDATE
                SET last_heartbeat = now(), status = EXCLUDED.status, detail = EXCLUDED.detail
                """,
                (component, status, detail),
            )
        conn.close()
    except Exception as e:
        logger.warning(f"health heartbeat write failed (non-fatal): {e}")


def raise_alert(alert_type: str, severity: str, message: str, details: dict | None = None):
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_alerts (alert_type, severity, component, message, details)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (alert_type, severity, "airflow_daily_dag", message,
                 psycopg2.extras.Json(details or {})),
            )
        conn.close()
    except Exception as e:
        logger.warning(f"alert write failed (non-fatal): {e}")


@dag(
    dag_id="daily_vehicle_reconciliation",
    description="Batch layer: join daily trip earnings against fuel/maintenance expenses per vehicle",
    schedule="*/5 * * * *",  # every 5 real minutes = every simulated day
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
    },
    tags=["fleet", "batch-layer", "reconciliation"],
)
def daily_vehicle_reconciliation():

    @task
    def find_expense_file(**context) -> str:
        """
        Find the most recent expense CSV that hasn't been processed yet.
        Rather than relying on Airflow's own execution_date matching the
        simulated clock exactly (they drift independently), pick the
        newest file in the expense directory -- this is robust to the
        sim-day / DAG-schedule cadences not lining up perfectly.
        """
        pattern = os.path.join(EXPENSE_DIR, "expenses_*.csv")
        files = sorted(glob.glob(pattern))
        if not files:
            msg = f"no expense files found matching {pattern}"
            logger.warning(msg)
            raise_alert("expense_file_missing", "WARNING", msg, {"pattern": pattern})
            log_health("airflow_daily_dag", "WARN", msg)
            raise AirflowException(msg)

        latest = files[-1]
        logger.info(f"using expense file: {latest}")
        return latest

    @task
    def load_expenses(filepath: str) -> list[dict]:
        rows = []
        with open(filepath, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append({
                    "vehicle_id": row["vehicle_id"],
                    "fuel_cost": float(row["fuel_cost"]),
                    "maintenance_cost": float(row["maintenance_cost"]),
                    "distance_covered": float(row["distance_covered"]),
                    "service_flag": row["service_flag"],
                })
        logger.info(f"loaded {len(rows)} expense records from {filepath}")
        return rows

    @task
    def extract_sim_date(filepath: str) -> str:
        # expenses_YYYY-MM-DD.csv
        basename = os.path.basename(filepath)
        date_str = basename.replace("expenses_", "").replace(".csv", "")
        return date_str

    @task
    def aggregate_trip_earnings(sim_date: str) -> list[dict]:
        """Aggregate raw_trip_events per vehicle for the given sim_date."""
        conn = pg_connect()
        results = []
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT vehicle_id,
                           COUNT(DISTINCT trip_id) AS trip_count,
                           COALESCE(SUM(fare), 0) AS total_earnings
                    FROM raw_trip_events
                    WHERE event_time::date = %s::date
                    GROUP BY vehicle_id
                    """,
                    (sim_date,),
                )
                for vehicle_id, trip_count, total_earnings in cur.fetchall():
                    results.append({
                        "vehicle_id": vehicle_id,
                        "trip_count": trip_count,
                        "total_earnings": float(total_earnings),
                    })
        finally:
            conn.close()
        logger.info(f"aggregated trip earnings for {len(results)} vehicles on {sim_date}")
        return results

    @task
    def reconcile_and_upsert(sim_date: str, expenses: list[dict], earnings: list[dict]):
        earnings_by_vehicle = {e["vehicle_id"]: e for e in earnings}
        conn = pg_connect()
        upserted = 0
        unprofitable = 0
        try:
            with conn, conn.cursor() as cur:
                for exp in expenses:
                    vid = exp["vehicle_id"]
                    e = earnings_by_vehicle.get(vid, {"trip_count": 0, "total_earnings": 0.0})
                    total_cost = exp["fuel_cost"] + exp["maintenance_cost"]
                    net_profit = e["total_earnings"] - total_cost
                    is_unprofitable = net_profit < 0
                    if is_unprofitable:
                        unprofitable += 1

                    cur.execute(
                        """
                        INSERT INTO daily_vehicle_reconciliation
                            (sim_date, vehicle_id, trip_count, total_earnings,
                             distance_covered, fuel_cost, maintenance_cost,
                             total_cost, net_profit, is_unprofitable, service_flag)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (sim_date, vehicle_id) DO UPDATE SET
                            trip_count = EXCLUDED.trip_count,
                            total_earnings = EXCLUDED.total_earnings,
                            distance_covered = EXCLUDED.distance_covered,
                            fuel_cost = EXCLUDED.fuel_cost,
                            maintenance_cost = EXCLUDED.maintenance_cost,
                            total_cost = EXCLUDED.total_cost,
                            net_profit = EXCLUDED.net_profit,
                            is_unprofitable = EXCLUDED.is_unprofitable,
                            service_flag = EXCLUDED.service_flag,
                            computed_at = now()
                        """,
                        (sim_date, vid, e["trip_count"], e["total_earnings"],
                         exp["distance_covered"], exp["fuel_cost"], exp["maintenance_cost"],
                         total_cost, net_profit, is_unprofitable, exp["service_flag"]),
                    )
                    upserted += 1
        finally:
            conn.close()

        msg = f"reconciled {upserted} vehicles for {sim_date}, {unprofitable} unprofitable"
        logger.info(msg)
        log_health("airflow_daily_dag", "OK", msg)
        if unprofitable > 0:
            raise_alert(
                "vehicles_unprofitable", "WARNING", msg,
                {"sim_date": sim_date, "unprofitable_count": unprofitable},
            )

    expense_file = find_expense_file()
    sim_date = extract_sim_date(expense_file)
    expenses = load_expenses(expense_file)
    earnings = aggregate_trip_earnings(sim_date)
    reconcile_and_upsert(sim_date, expenses, earnings)


daily_vehicle_reconciliation()
