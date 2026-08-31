"""
Speed layer: Spark Structured Streaming job.

Consumes the `vehicle_telemetry` topic from Kafka, computes live fleet
utilization metrics over 1-minute tumbling windows, and writes results to
PostgreSQL using foreachBatch (Structured Streaming has no native JDBC sink).

Tables written (see postgres/init.sql):
    - live_fleet_utilization   windowed metrics per zone, one row per
                                (window_start, zone) per micro-batch
    - vehicle_last_state       latest known state per vehicle, upserted via
                                psycopg2 (vehicle_id is the primary key), with
                                idle_since preserved across batches so idle
                                DURATION can be computed accurately
    - pipeline_alerts          a row is inserted whenever a vehicle's
                                continuous idle duration crosses the threshold
    - pipeline_health          heartbeat row updated every micro-batch, used
                                by the "no data received" health check

Architecture note (Lambda): this is the SPEED layer only. It reads the live
Kafka stream and produces fast, continuously-updating views. It never reads
the daily batch expense file -- that reconciliation happens in the separate
batch layer job (Airflow-orchestrated, Step 4), consistent with Lambda's
separation of speed and batch layers.
"""

import logging
import os

import psycopg2
import psycopg2.extras

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, sum as spark_sum, avg, when, current_timestamp, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "component": "spark_streaming", "msg": "%(message)s"}',
)
logger = logging.getLogger("spark_streaming")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("TELEMETRY_TOPIC", "vehicle_telemetry")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "fleet_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "fleet")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "fleet_pw")
PG_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

WINDOW_DURATION = os.environ.get("WINDOW_DURATION", "1 minute")
WATERMARK_DELAY = os.environ.get("WATERMARK_DELAY", "2 minutes")
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/spark-checkpoints")
IDLE_ALERT_THRESHOLD_MIN = int(os.environ.get("IDLE_ALERT_THRESHOLD_MIN", "3"))
DLQ_ALERT_THRESHOLD = int(os.environ.get("DLQ_ALERT_THRESHOLD", "5"))

EVENT_SCHEMA = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("zone", StringType(), True),
    StructField("speed", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("timestamp", StringType(), True),
])


def get_spark():
    return (
        SparkSession.builder
        .appName("FleetSpeedLayer")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def pg_connect():
    return psycopg2.connect(
        host=POSTGRES_HOST, port=POSTGRES_PORT, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASSWORD, connect_timeout=5,
    )


def heartbeat(status, detail):
    """Best-effort health heartbeat; failures are logged, never raised, so a
    heartbeat problem never takes down the main streaming query."""
    try:
        conn = pg_connect()
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_health (component, last_heartbeat, status, detail)
                VALUES ('spark_streaming', now(), %s, %s)
                ON CONFLICT (component) DO UPDATE
                SET last_heartbeat = now(), status = EXCLUDED.status, detail = EXCLUDED.detail
                """,
                (status, detail),
            )
        conn.close()
    except Exception as e:
        logger.warning(f"heartbeat write failed (non-fatal): {e}")


def jdbc_append(df, table_name):
    (
        df.write
        .format("jdbc")
        .option("url", PG_URL)
        .option("dbtable", table_name)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )


def write_utilization_batch(df, epoch_id):
    n = df.count()
    if n == 0:
        logger.info(f"batch {epoch_id}: no utilization rows, skipping")
        return
    try:
        jdbc_append(df, "live_fleet_utilization")
        logger.info(f"batch {epoch_id}: wrote {n} rows to live_fleet_utilization")
        heartbeat("OK", f"wrote {n} utilization rows in batch {epoch_id}")
    except Exception as e:
        logger.error(f"batch {epoch_id}: FAILED writing live_fleet_utilization: {e}")
        heartbeat("ERROR", str(e))
        raise


def write_vehicle_state_and_alerts_batch(df, epoch_id):
    """
    Upsert per-vehicle latest state via psycopg2 (vehicle_id is PK), tracking
    idle_since across batches so idle DURATION (not just idle count) can be
    computed accurately, then raise a pipeline_alerts row for any vehicle
    whose continuous idle streak crosses IDLE_ALERT_THRESHOLD_MIN.
    """
    rows = df.collect()
    if not rows:
        logger.info(f"batch {epoch_id}: no vehicle-state rows, skipping")
        return

    try:
        conn = pg_connect()
        alert_count = 0
        with conn, conn.cursor() as cur:
            for r in rows:
                # fetch previous idle_since to preserve continuity across batches
                cur.execute(
                    "SELECT status, idle_since FROM vehicle_last_state WHERE vehicle_id = %s",
                    (r.vehicle_id,),
                )
                prev = cur.fetchone()
                prev_status, prev_idle_since = prev if prev else (None, None)

                if r.status == "idle":
                    idle_since = prev_idle_since if prev_status == "idle" and prev_idle_since else r.last_event_ts
                else:
                    idle_since = None

                cur.execute(
                    """
                    INSERT INTO vehicle_last_state
                        (vehicle_id, driver_id, status, zone, lat, lon, last_event_ts, idle_since, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (vehicle_id) DO UPDATE SET
                        driver_id = EXCLUDED.driver_id,
                        status = EXCLUDED.status,
                        zone = EXCLUDED.zone,
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        last_event_ts = EXCLUDED.last_event_ts,
                        idle_since = EXCLUDED.idle_since,
                        updated_at = now()
                    """,
                    (r.vehicle_id, r.driver_id, r.status, r.zone, r.lat, r.lon,
                     r.last_event_ts, idle_since),
                )

                if idle_since is not None:
                    idle_minutes = (r.last_event_ts - idle_since).total_seconds() / 60.0
                    if idle_minutes >= IDLE_ALERT_THRESHOLD_MIN:
                        cur.execute(
                            """
                            INSERT INTO pipeline_alerts
                                (alert_type, severity, component, message, details)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
                            (
                                "vehicle_idle_too_long", "WARNING", "spark_streaming",
                                f"Vehicle {r.vehicle_id} idle for {idle_minutes:.1f} min",
                                psycopg2.extras.Json({
                                    "vehicle_id": r.vehicle_id,
                                    "idle_minutes": round(idle_minutes, 1),
                                    "zone": r.zone,
                                }),
                            ),
                        )
                        alert_count += 1
        conn.close()
        logger.info(
            f"batch {epoch_id}: upserted {len(rows)} vehicle states, raised {alert_count} idle alerts"
        )
        heartbeat("OK", f"batch {epoch_id}: {len(rows)} states, {alert_count} alerts")
    except Exception as e:
        logger.error(f"batch {epoch_id}: FAILED updating vehicle_last_state/pipeline_alerts: {e}")
        heartbeat("ERROR", str(e))
        raise


def write_trip_events_batch(df, epoch_id):
    n = df.count()
    if n == 0:
        logger.info(f"batch {epoch_id}: no trip events, skipping")
        return
    try:
        jdbc_append(df, "raw_trip_events")
        logger.info(f"batch {epoch_id}: wrote {n} rows to raw_trip_events")
    except Exception as e:
        logger.error(f"batch {epoch_id}: FAILED writing raw_trip_events: {e}")
        heartbeat("ERROR", str(e))
        raise


def write_dlq_batch(df, epoch_id):
    """Write malformed events to the DLQ and raise an alert if the error
    volume in this micro-batch crosses a threshold -- the assignment's
    'error rate above threshold' health-check rule."""
    n = df.count()
    if n == 0:
        return
    try:
        jdbc_append(df, "dlq_events")
        logger.warning(f"batch {epoch_id}: {n} malformed events routed to DLQ")
        if n >= DLQ_ALERT_THRESHOLD:
            try:
                conn = pg_connect()
                with conn, conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO pipeline_alerts (alert_type, severity, component, message, details)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            "high_error_rate", "ERROR", "spark_streaming",
                            f"{n} malformed messages in one micro-batch (threshold: {DLQ_ALERT_THRESHOLD})",
                            psycopg2.extras.Json({"malformed_count": n, "batch": epoch_id}),
                        ),
                    )
                conn.close()
            except Exception as e:
                logger.warning(f"DLQ alert write failed (non-fatal): {e}")
        heartbeat("OK" if n < DLQ_ALERT_THRESHOLD else "WARN", f"batch {epoch_id}: {n} malformed events")
    except Exception as e:
        logger.error(f"batch {epoch_id}: FAILED writing dlq_events: {e}")
        heartbeat("ERROR", str(e))
        raise


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"starting speed layer: topic={TOPIC}, bootstrap={KAFKA_BOOTSTRAP}, pg={PG_URL}")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_raw = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .withColumn("data", from_json(col("json_str"), EVENT_SCHEMA))
    )

    # A message that fails schema validation parses to a row of all-null
    # fields (Spark's from_json behavior on mismatch) -- vehicle_id is
    # required in every legitimate event, so its absence flags a bad record.
    malformed = parsed_raw.filter(col("data.vehicle_id").isNull()).select(
        lit(TOPIC).alias("source_topic"),
        col("json_str").alias("raw_payload"),
        lit("schema_validation_failed").alias("error_reason"),
    )

    malformed_query = (
        malformed.writeStream
        .outputMode("append")
        .foreachBatch(write_dlq_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/dlq")
        .trigger(processingTime="30 seconds")
        .start()
    )

    parsed = (
        parsed_raw.filter(col("data.vehicle_id").isNotNull())
        .select("data.*")
        .withColumn("event_time", col("timestamp").cast(TimestampType()))
        .withWatermark("event_time", WATERMARK_DELAY)
    )

    # ---- 1. Live utilization metrics per zone, tumbling windows ----
    utilization = (
        parsed.groupBy(window(col("event_time"), WINDOW_DURATION), col("zone"))
        .agg(
            count("vehicle_id").alias("active_vehicles"),
            spark_sum(when(col("status") == "idle", 1).otherwise(0)).alias("idle_vehicles"),
            spark_sum(when(col("status") == "enroute", 1).otherwise(0)).alias("enroute_vehicles"),
            spark_sum(when(col("status") == "on_trip", 1).otherwise(0)).alias("on_trip_vehicles"),
            spark_sum("fare").alias("total_earnings"),
            count(when(col("status") == "on_trip", col("trip_id"))).alias("trip_count"),
            avg("speed").alias("avg_speed"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("zone"),
            col("active_vehicles"),
            col("idle_vehicles"),
            col("enroute_vehicles"),
            col("on_trip_vehicles"),
            (col("idle_vehicles") / col("active_vehicles")).alias("idle_ratio"),
            col("total_earnings"),
            col("trip_count"),
            col("avg_speed"),
            current_timestamp().alias("ingested_at"),
        )
    )

    utilization_query = (
        utilization.writeStream
        .outputMode("update")
        .foreachBatch(write_utilization_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/utilization")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ---- 2. Per-vehicle latest-state upsert + idle-duration alerting ----
    vehicle_snapshot = parsed.select(
        col("vehicle_id"), col("driver_id"), col("status"), col("zone"),
        col("lat"), col("lon"), col("event_time").alias("last_event_ts"),
    )

    vehicle_state_query = (
        vehicle_snapshot.writeStream
        .outputMode("append")
        .foreachBatch(write_vehicle_state_and_alerts_batch)
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/vehicle_state")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # ---- 3. Raw trip-event log (batch layer's source of truth) ----
    # Only on_trip pings are kept -- this is what Airflow's daily job
    # aggregates and joins against the expense CSV for reconciliation.
    trip_events = (
        parsed.filter(col("status") == "on_trip")
        .select(
            col("trip_id"), col("vehicle_id"), col("driver_id"), col("zone"),
            col("fare"), col("speed"), col("event_time"),
        )
    )

    trip_events_query = (
        trip_events.writeStream
        .outputMode("append")
        .foreachBatch(lambda df, eid: write_trip_events_batch(df, eid))
        .option("checkpointLocation", f"{CHECKPOINT_DIR}/trip_events")
        .trigger(processingTime="30 seconds")
        .start()
    )

    logger.info(
        "speed layer streaming queries started: live_fleet_utilization, "
        "vehicle_last_state/pipeline_alerts, raw_trip_events"
    )
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
