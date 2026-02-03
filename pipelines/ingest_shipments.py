from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data/raw/shipments.csv"

def main():
    df = pd.read_csv(RAW)
    df.columns = [c.strip().lower() for c in df.columns]

    date_cols = ["ship_date", "promised_delivery_date", "actual_delivery_date"]
    for c in date_cols:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    df["cost_usd"] = pd.to_numeric(df["cost_usd"], errors="coerce")
    df["weight_kg"] = pd.to_numeric(df["weight_kg"], errors="coerce")

    # basic cleanup
    df = df.dropna(subset=["shipment_id", "ship_date", "carrier_name", "origin_port_code", "dest_port_code"])
    out = ROOT / "data/raw/shipments_clean.csv"
    df.to_csv(out, index=False)
    print(f"Saved cleaned file: {out} ({len(df)} rows)")

if __name__ == "__main__":
    main()
