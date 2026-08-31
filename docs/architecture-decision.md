# Architecture Decision: Lambda vs Kappa

## Use Case

**Ride-Hailing Fleet Operations.** A fleet operator needs live visibility
into fleet activity and utilization, reconciled daily against per-vehicle
running costs submitted by fuel/garage partners.

Two data sources:
1. **Streaming**: GPS/telemetry events per vehicle, emitted every few
   seconds (`trip_id`, `driver_id`, `vehicle_id`, `lat`, `lon`, `speed`,
   `status`, `fare`, `timestamp`).
2. **Daily batch**: one CSV per simulated day of vehicle expense records
   from garages and fuel partners (`vehicle_id`, `fuel_cost`,
   `maintenance_cost`, `distance_covered`, `service_flag`).

Business question: *What is fleet utilization and earnings by area/time-of-day
right now, and which vehicles are becoming unprofitable once yesterday's
fuel/maintenance costs are factored in?*

## Decision: Lambda Architecture

### Why not Kappa

Kappa architecture unifies all processing — real-time and historical —
around a single stream, with batch views produced by replaying that same
stream. It is the right choice when there is fundamentally **one** source
of truth that both live and historical views derive from.

This use case does not have that property. The two data sources are
**genuinely heterogeneous**:

- The telemetry stream is naturally continuous, high-frequency,
  per-vehicle event data.
- The expense data is a **daily batch extract from an external partner** —
  it has no natural streaming form. It arrives once a day, as a file, from
  a system outside this pipeline's control.

Forcing the expense CSV into Kafka just to unify processing under Kappa
would mean: (a) publishing a batch file as a stream artificially, gaining
no real benefit since the data isn't naturally event-shaped, and (b) losing
the clean separation between "what's true right now" and "what's true once
the books are closed for the day" — which this business question explicitly
needs (the reconciliation must reflect *yesterday's* costs, not a replay of
a stream).

### Why Lambda fits

Lambda's speed/batch split maps directly onto the two source types and the
two outputs the business actually asked for:

| | Speed layer | Batch layer |
|---|---|---|
| **Input** | `vehicle_telemetry` Kafka stream | `raw_trip_events` (accumulated from the stream) + daily expense CSV |
| **Output** | `live_fleet_utilization`, `vehicle_last_state`, idle alerts | `daily_vehicle_reconciliation` (profitability per vehicle) |
| **Latency** | Seconds (30s micro-batch trigger) | Once per simulated day |
| **Correctness model** | Approximate, continuously self-correcting on the next micro-batch | Full daily recompute — auditable, reproducible, not incrementally patched |
| **Failure mode** | A missed window is superseded by the next one within 30s | A failed run retries (Airflow: 3 retries, exponential backoff) and is fully re-runnable without state corruption |

The **consistency requirements genuinely differ** between the two outputs:
a slightly stale live utilization number for one 1-minute window is a
non-issue (the next window corrects it), but an incorrect profitability
figure that determines whether a vehicle is flagged unprofitable is not
something you want computed incrementally from partial state — it should
be a clean, auditable join over the full day's data, recomputed from
source each time. That is exactly what Lambda's batch layer gives you and
what a purely incremental/streaming approach does not.

### Rejected Alternative: Pure streaming (no batch layer at all)

Could the daily reconciliation be done as a stateful streaming
aggregation instead (e.g., a long-running stream that joins telemetry
against expense data as it "arrives")? Rejected because:

- The expense file is not a stream — it is one atomic daily snapshot.
  Treating it as streaming input adds complexity (state management,
  watermarking a source with no continuous throughput) without benefit.
- A daily batch join is trivially replayable and debuggable (re-run the
  DAG, get the same answer) — a pure streaming join with long-lived state
  is harder to reason about, replay, and audit, which matters for a
  financial reconciliation report.

## Trade-offs and Limitations of the Chosen Approach

- **Code duplication**: aggregation logic conceptually overlaps between the
  speed layer (windowed utilization) and batch layer (daily aggregation) —
  a known Lambda criticism. At this project's scale, the two aggregations
  are different enough (windowed zone metrics vs. per-vehicle daily join)
  that this duplication is minimal, but it would grow with more business
  questions added to each layer.
- **Two systems to operate**: Spark Structured Streaming and Airflow both
  need to be running and healthy; Kappa would reduce this to one processing
  paradigm. Mitigated here by consolidated observability (`pipeline_health`,
  `pipeline_alerts` shared across both layers) rather than treating them as
  fully separate systems operationally.
- **Eventual consistency window**: the batch layer's view of "today's
  trips" depends on `raw_trip_events` having been fully written by the
  speed layer before the batch job runs — a race condition in principle
  (mitigated in practice by the batch job's 5-minute schedule being well
  spaced from the 30-second micro-batch writes).

## What Would Change at Production Scale

- Kafka: multiple brokers, higher partition count, replication factor > 1.
- Spark: multi-executor cluster instead of `local[2]`.
- Airflow: split `standalone` into separate webserver/scheduler/worker
  containers with `CeleryExecutor` or `KubernetesExecutor`.
- Postgres: read replicas for the serving API, or a dedicated OLAP store
  (e.g. ClickHouse) for the utilization time-series if query volume grew.
- Add a Schema Registry for the Kafka topic instead of an inline schema
  definition in the Spark job.
