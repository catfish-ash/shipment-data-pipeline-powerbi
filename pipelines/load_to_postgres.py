import os
from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]
INP = ROOT / "data/raw/shipments_clean.csv"

load_dotenv()

DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))
DB_NAME = os.getenv("PGDATABASE", "shipment_dw")
DB_USER = os.getenv("PGUSER", "dw_user")
DB_PASS = os.getenv("PGPASSWORD", "dw_pass")

def date_key(d):
    if pd.isna(d):
        return None
    return int(pd.to_datetime(d).strftime("%Y%m%d"))

def upsert_dim_date(cur, dates):
    rows = []
    for d in sorted(set(dates)):
        if pd.isna(d):
            continue
        dt = pd.to_datetime(d).date()
        dk = int(pd.to_datetime(dt).strftime("%Y%m%d"))
        rows.append((dk, dt, dt.month, (dt.month-1)//3 + 1, dt.year))

    if not rows:
        return

    cur.execute("""
        INSERT INTO dw.dim_date (date_key, date, month, quarter, year)
        VALUES %s
        ON CONFLICT (date_key) DO NOTHING;
    """, (execute_values(cur, "SELECT 1", rows, fetch=False),))

def ensure_dim_ports(cur, df):
    ports = pd.concat([
        df[["origin_port_code","origin_port_name","origin_country","origin_region"]]
          .rename(columns={"origin_port_code":"port_code","origin_port_name":"port_name","origin_country":"country","origin_region":"region"}),
        df[["dest_port_code","dest_port_name","dest_country","dest_region"]]
          .rename(columns={"dest_port_code":"port_code","dest_port_name":"port_name","dest_country":"country","dest_region":"region"})
    ]).drop_duplicates()

    rows = [tuple(x) for x in ports[["port_code","port_name","country","region"]].fillna("").to_records(index=False)]
    execute_values(cur, """
        INSERT INTO dw.dim_port (port_code, port_name, country, region)
        VALUES %s
        ON CONFLICT (port_code) DO UPDATE SET
          port_name = EXCLUDED.port_name,
          country = EXCLUDED.country,
          region = EXCLUDED.region;
    """, rows)

def ensure_dim_carriers(cur, df):
    carriers = df[["carrier_name","mode"]].drop_duplicates()
    rows = [tuple(x) for x in carriers.fillna("").to_records(index=False)]
    execute_values(cur, """
        INSERT INTO dw.dim_carrier (carrier_name, mode)
        VALUES %s
        ON CONFLICT (carrier_name) DO UPDATE SET mode = EXCLUDED.mode;
    """, rows)

def ensure_dim_customers(cur, df):
    customers = df[["customer_id","customer_name","segment"]].drop_duplicates()
    rows = [tuple(x) for x in customers.fillna("").to_records(index=False)]
    execute_values(cur, """
        INSERT INTO dw.dim_customer (customer_id, customer_name, segment)
        VALUES %s
        ON CONFLICT (customer_id) DO UPDATE SET
          customer_name = EXCLUDED.customer_name,
          segment = EXCLUDED.segment;
    """, rows)

def fetch_dim_maps(cur):
    cur.execute("SELECT port_key, port_code FROM dw.dim_port;")
    port_map = {code: key for key, code in cur.fetchall()}

    cur.execute("SELECT carrier_key, carrier_name FROM dw.dim_carrier;")
    carrier_map = {name: key for key, name in cur.fetchall()}

    cur.execute("SELECT customer_key, customer_id FROM dw.dim_customer;")
    cust_map = {cid: key for key, cid in cur.fetchall()}

    return port_map, carrier_map, cust_map

def main():
    df = pd.read_csv(INP)
    df.columns = [c.strip().lower() for c in df.columns]

    # Connect
    conn = psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS
    )
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # Load staging
            cur.execute("TRUNCATE TABLE dw.stg_shipments_raw;")
            cols = [
                "shipment_id","customer_id","customer_name","carrier_name","mode",
                "origin_port_code","origin_port_name","origin_country","origin_region",
                "dest_port_code","dest_port_name","dest_country","dest_region",
                "ship_date","promised_delivery_date","actual_delivery_date",
                "weight_kg","cost_usd","status"
            ]
            rows = [tuple(x) for x in df[cols].to_records(index=False)]
            execute_values(cur, f"""
                INSERT INTO dw.stg_shipments_raw ({",".join(cols)})
                VALUES %s;
            """, rows)

            # Dim date (ship_date only for simplicity)
            ship_dates = df["ship_date"]
            # safer insert dim_date using normal execute_values approach
            dim_rows = []
            for d in ship_dates.dropna().unique():
                dt = pd.to_datetime(d).date()
                dk = int(pd.to_datetime(dt).strftime("%Y%m%d"))
                dim_rows.append((dk, dt, dt.month, (dt.month-1)//3 + 1, dt.year))
            if dim_rows:
                execute_values(cur, """
                    INSERT INTO dw.dim_date (date_key, date, month, quarter, year)
                    VALUES %s
                    ON CONFLICT (date_key) DO NOTHING;
                """, dim_rows)

            # Dims
            ensure_dim_ports(cur, df)
            ensure_dim_carriers(cur, df)
            ensure_dim_customers(cur, df)

            # Maps
            port_map, carrier_map, cust_map = fetch_dim_maps(cur)

            # Build fact rows
            fact_rows = []
            for r in df.to_dict(orient="records"):
                ship_dt = r.get("ship_date")
                promised = r.get("promised_delivery_date")
                actual = r.get("actual_delivery_date")

                ship_dt_ts = pd.to_datetime(ship_dt)
                promised_ts = pd.to_datetime(promised)
                actual_ts = pd.to_datetime(actual)

                planned_days = int((promised_ts - ship_dt_ts).days) if pd.notna(promised_ts) else None
                actual_days = int((actual_ts - ship_dt_ts).days) if pd.notna(actual_ts) else None
                delay_days = (actual_days - planned_days) if (actual_days is not None and planned_days is not None) else None
                on_time = 1 if (pd.notna(actual_ts) and pd.notna(promised_ts) and actual_ts <= promised_ts) else 0

                fact_rows.append((
                    r["shipment_id"],
                    date_key(ship_dt),
                    port_map.get(r["origin_port_code"]),
                    port_map.get(r["dest_port_code"]),
                    carrier_map.get(r["carrier_name"]),
                    cust_map.get(r["customer_id"]),
                    promised if pd.notna(promised) else None,
                    actual if pd.notna(actual) else None,
                    planned_days,
                    actual_days,
                    delay_days,
                    on_time,
                    float(r["weight_kg"]) if pd.notna(r.get("weight_kg")) else None,
                    float(r["cost_usd"]) if pd.notna(r.get("cost_usd")) else None,
                    r.get("status")
                ))

            execute_values(cur, """
                INSERT INTO dw.fact_shipments (
                  shipment_id, date_key, origin_port_key, dest_port_key, carrier_key, customer_key,
                  promised_delivery_date, actual_delivery_date, planned_days, actual_days, delay_days,
                  on_time_flag, weight_kg, cost_usd, status
                )
                VALUES %s
                ON CONFLICT (shipment_id) DO UPDATE SET
                  date_key = EXCLUDED.date_key,
                  origin_port_key = EXCLUDED.origin_port_key,
                  dest_port_key = EXCLUDED.dest_port_key,
                  carrier_key = EXCLUDED.carrier_key,
                  customer_key = EXCLUDED.customer_key,
                  promised_delivery_date = EXCLUDED.promised_delivery_date,
                  actual_delivery_date = EXCLUDED.actual_delivery_date,
                  planned_days = EXCLUDED.planned_days,
                  actual_days = EXCLUDED.actual_days,
                  delay_days = EXCLUDED.delay_days,
                  on_time_flag = EXCLUDED.on_time_flag,
                  weight_kg = EXCLUDED.weight_kg,
                  cost_usd = EXCLUDED.cost_usd,
                  status = EXCLUDED.status;
            """, fact_rows)

        conn.commit()
        print(f"Loaded {len(df)} shipments into dw.fact_shipments ✅")

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()
