"""
Streaming source: simulates live GPS/telemetry events from ride-hailing vehicles.

Emits one JSON event every EMIT_INTERVAL_SECONDS to Kafka topic `vehicle_telemetry`.
Each vehicle independently transitions between states (idle / enroute / on_trip)
using a simple Markov-style state machine so the stream looks realistic.

Simulated clock: 1 simulated day = SIM_DAY_SECONDS real seconds (default 300s = 5 min).
The wall-clock "timestamp" field in each event is scaled so that a full 24h
day passes in SIM_DAY_SECONDS real seconds. This lets Airflow's daily batch
job trigger on a short real-world cadence while still representing "once a day".
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "component": "telemetry_producer", "msg": %(message)s}',
)
logger = logging.getLogger("telemetry_producer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.environ.get("TELEMETRY_TOPIC", "vehicle_telemetry")
NUM_VEHICLES = int(os.environ.get("NUM_VEHICLES", "20"))
EMIT_INTERVAL_SECONDS = float(os.environ.get("EMIT_INTERVAL_SECONDS", "2"))
SIM_DAY_SECONDS = float(os.environ.get("SIM_DAY_SECONDS", "300"))  # 5 real min = 1 sim day

# Colombo-ish bounding box, purely illustrative for a ride-hailing city
LAT_RANGE = (6.85, 6.95)
LON_RANGE = (79.83, 79.90)
ZONES = ["ZoneA_Fort", "ZoneB_Kollupitiya", "ZoneC_Dehiwala", "ZoneD_Rajagiriya"]

STATES = ["idle", "enroute", "on_trip"]
TRANSITIONS = {
    "idle": {"idle": 0.6, "enroute": 0.4, "on_trip": 0.0},
    "enroute": {"idle": 0.1, "enroute": 0.2, "on_trip": 0.7},
    "on_trip": {"idle": 0.0, "enroute": 0.05, "on_trip": 0.95},
}


class Vehicle:
    def __init__(self, vehicle_id, driver_id):
        self.vehicle_id = vehicle_id
        self.driver_id = driver_id
        self.state = "idle"
        self.lat = random.uniform(*LAT_RANGE)
        self.lon = random.uniform(*LON_RANGE)
        self.trip_id = None
        self.zone = random.choice(ZONES)

    def step(self):
        probs = TRANSITIONS[self.state]
        self.state = random.choices(list(probs), weights=list(probs.values()))[0]

        if self.state == "on_trip" and self.trip_id is None:
            self.trip_id = str(uuid.uuid4())
        elif self.state != "on_trip":
            self.trip_id = None

        # small random walk for position
        self.lat += random.uniform(-0.002, 0.002)
        self.lon += random.uniform(-0.002, 0.002)
        self.zone = random.choice(ZONES) if random.random() < 0.05 else self.zone

        speed = 0.0
        fare = 0.0
        if self.state == "enroute":
            speed = random.uniform(10, 40)
        elif self.state == "on_trip":
            speed = random.uniform(5, 60)
            fare = round(random.uniform(1.5, 4.0), 2)  # incremental fare this tick

        return {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "zone": self.zone,
            "speed": round(speed, 1),
            "status": self.state,
            "fare": fare,
        }


def sim_now():
    """Return a timestamp scaled so 24h passes every SIM_DAY_SECONDS real seconds."""
    scale = 86400.0 / SIM_DAY_SECONDS
    real_elapsed = time.time() - sim_now.start_real
    sim_elapsed = real_elapsed * scale
    return (sim_now.start_wall + timedelta(seconds=sim_elapsed)).isoformat()


sim_now.start_real = time.time()
sim_now.start_wall = datetime.now(timezone.utc)


def build_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        retries=5,
        acks="all",
        linger_ms=50,
    )


def main():
    vehicles = [Vehicle(f"veh_{i:03d}", f"drv_{i:03d}") for i in range(NUM_VEHICLES)]
    producer = build_producer()
    logger.info(json.dumps(f"starting telemetry producer: {NUM_VEHICLES} vehicles, topic={TOPIC}"))

    sent = 0
    try:
        while True:
            for v in vehicles:
                event = v.step()
                event["timestamp"] = sim_now()
                try:
                    producer.send(TOPIC, key=event["vehicle_id"], value=event)
                    sent += 1
                except KafkaError as e:
                    logger.error(json.dumps(f"failed to send event for {event['vehicle_id']}: {e}"))
            producer.flush()
            if sent % 100 < NUM_VEHICLES:
                logger.info(json.dumps(f"heartbeat: {sent} events sent so far"))
            time.sleep(EMIT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info(json.dumps("shutting down telemetry producer"))
    finally:
        producer.close()


if __name__ == "__main__":
    main()
