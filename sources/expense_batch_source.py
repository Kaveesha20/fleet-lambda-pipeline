"""
Daily-batch source: simulates garage/fuel-partner vehicle expense records.

Once per simulated day (default: every SIM_DAY_SECONDS = 300 real seconds =
5 minutes), drops one CSV file into data/raw/expenses/ containing that day's
expense records for a subset (or all) of the fleet:

    vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag

This mimics an end-of-day extract from an external fuel/garage partner API.
File naming: expenses_<sim_date>.csv  (sim_date = YYYY-MM-DD of the simulated
clock at drop time), so Airflow's daily DAG can pick up exactly one file per
simulated day via a sensor or glob.
"""

import csv
import logging
import os
import random
import time
from datetime import datetime, timedelta, timezone

logging.basicConfig(
    level=logging.INFO,
    format='{"ts": "%(asctime)s", "level": "%(levelname)s", "component": "expense_batch_source", "msg": %(message)s}',
)
logger = logging.getLogger("expense_batch_source")

NUM_VEHICLES = int(os.environ.get("NUM_VEHICLES", "20"))
SIM_DAY_SECONDS = float(os.environ.get("SIM_DAY_SECONDS", "300"))
OUTPUT_DIR = os.environ.get("EXPENSE_OUTPUT_DIR", "/data/raw/expenses")


def sim_now():
    scale = 86400.0 / SIM_DAY_SECONDS
    real_elapsed = time.time() - sim_now.start_real
    sim_elapsed = real_elapsed * scale
    return sim_now.start_wall + timedelta(seconds=sim_elapsed)


sim_now.start_real = time.time()
sim_now.start_wall = datetime.now(timezone.utc)


def generate_daily_file(sim_date_str):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f"expenses_{sim_date_str}.csv")

    rows = []
    for i in range(NUM_VEHICLES):
        vehicle_id = f"veh_{i:03d}"
        distance = round(random.uniform(50, 400), 1)
        fuel_cost = round(distance * random.uniform(0.15, 0.35), 2)
        maintenance_cost = round(random.choice([0, 0, 0, random.uniform(20, 150)]), 2)
        service_flag = "Y" if maintenance_cost > 0 and random.random() < 0.3 else "N"
        rows.append(
            {
                "vehicle_id": vehicle_id,
                "fuel_cost": fuel_cost,
                "maintenance_cost": maintenance_cost,
                "distance_covered": distance,
                "service_flag": service_flag,
            }
        )

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"]
        )
        writer.writeheader()
        writer.writerows(rows)

    logger.info(f'"wrote daily expense file: {filepath} ({len(rows)} rows)"')
    return filepath


def main():
    logger.info(
        f'"starting expense batch source: 1 file every {SIM_DAY_SECONDS}s (simulated day), '
        f'output_dir={OUTPUT_DIR}"'
    )
    last_written_date = None
    try:
        while True:
            current = sim_now()
            sim_date_str = current.strftime("%Y-%m-%d")
            if sim_date_str != last_written_date:
                generate_daily_file(sim_date_str)
                last_written_date = sim_date_str
            time.sleep(5)
    except KeyboardInterrupt:
        logger.info('"shutting down expense batch source"')


if __name__ == "__main__":
    main()
