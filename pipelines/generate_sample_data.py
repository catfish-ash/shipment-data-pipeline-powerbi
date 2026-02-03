import random
from datetime import date, timedelta
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
PORTS = pd.read_csv(ROOT / "data/reference/ports.csv")
CARRIERS = pd.read_csv(ROOT / "data/reference/carriers.csv")

OUT = ROOT / "data/raw/shipments.csv"
OUT.parent.mkdir(parents=True, exist_ok=True)

CUSTOMERS = [
    ("C001", "NorthStar Imports", "Retail"),
    ("C002", "Pacific Manufacturing", "Manufacturing"),
    ("C003", "Evergreen Foods", "CPG"),
    ("C004", "BlueWave Logistics", "3PL"),
    ("C005", "Metro Construction", "Construction"),
]

def rand_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

def main(n_rows: int = 1200, seed: int = 42):
    random.seed(seed)
    rows = []

    start = date(2024, 1, 1)
    end = date(2025, 12, 31)

    for i in range(n_rows):
        customer_id, customer_name, segment = random.choice(CUSTOMERS)
        carrier = CARRIERS.sample(1).iloc[0]
        origin = PORTS.sample(1).iloc[0]
        dest = PORTS.sample(1).iloc[0]
        while dest["port_code"] == origin["port_code"]:
            dest = PORTS.sample(1).iloc[0]

        ship_dt = rand_date(start, end)

        # Planned transit by mode
        base = {"Sea": 18, "Air": 5, "Rail": 7, "Ground": 4}.get(carrier["mode"], 7)
        variability = random.randint(-2, 4)
        planned_days = max(1, base + variability)

        promised = ship_dt + timedelta(days=planned_days)

        # Late probability varies by mode
        late_prob = {"Sea": 0.22, "Air": 0.10, "Rail": 0.15, "Ground": 0.12}.get(carrier["mode"], 0.15)
        is_late = random.random() < late_prob

        delay = random.randint(1, 6) if is_late else random.randint(-1, 1)
        actual_days = max(1, planned_days + delay)
        actual = ship_dt + timedelta(days=actual_days)

        weight = round(random.uniform(50, 8000), 2)
        # Cost roughly depends on mode and weight
        mode_multiplier = {"Sea": 0.18, "Air": 1.25, "Rail": 0.35, "Ground": 0.28}.get(carrier["mode"], 0.30)
        cost = round(weight * mode_multiplier + random.uniform(100, 600), 2)

        status = "Delivered" if actual <= end else "In Transit"

        rows.append({
            "shipment_id": f"SHP-{ship_dt.strftime('%Y%m')}-{i:05d}",
            "customer_id": customer_id,
            "customer_name": customer_name,
            "segment": segment,
            "carrier_name": carrier["carrier_name"],
            "mode": carrier["mode"],
            "origin_port_code": origin["port_code"],
            "origin_port_name": origin["port_name"],
            "origin_country": origin["country"],
            "origin_region": origin["region"],
            "dest_port_code": dest["port_code"],
            "dest_port_name": dest["port_name"],
            "dest_country": dest["country"],
            "dest_region": dest["region"],
            "ship_date": ship_dt,
            "promised_delivery_date": promised,
            "actual_delivery_date": actual,
            "weight_kg": weight,
            "cost_usd": cost,
            "status": status
        })

    df = pd.DataFrame(rows)
    df.to_csv(OUT, index=False)
    print(f"Generated: {OUT} ({len(df)} rows)")

if __name__ == "__main__":
    main()
