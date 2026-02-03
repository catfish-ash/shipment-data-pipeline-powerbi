from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
INP = ROOT / "data/raw/shipments_clean.csv"

def main():
    df = pd.read_csv(INP)

    errors = []

    # shipment_id unique
    if df["shipment_id"].duplicated().any():
        errors.append("shipment_id contains duplicates")

    # required columns not null
    required = ["shipment_id", "ship_date", "carrier_name", "origin_port_code", "dest_port_code"]
    for c in required:
        if df[c].isna().any():
            errors.append(f"{c} has nulls")

    # non-negative cost/weight
    if (df["cost_usd"] < 0).any():
        errors.append("cost_usd has negative values")
    if (df["weight_kg"] <= 0).any():
        errors.append("weight_kg has non-positive values")

    # date logic
    ship = pd.to_datetime(df["ship_date"], errors="coerce")
    promised = pd.to_datetime(df["promised_delivery_date"], errors="coerce")
    actual = pd.to_datetime(df["actual_delivery_date"], errors="coerce")

    if (promised < ship).any():
        errors.append("promised_delivery_date earlier than ship_date")
    if (actual < ship).any():
        errors.append("actual_delivery_date earlier than ship_date")

    if errors:
        raise SystemExit("DATA QUALITY FAILED:\n- " + "\n- ".join(errors))

    print("Data quality checks passed ✅")

if __name__ == "__main__":
    main()
