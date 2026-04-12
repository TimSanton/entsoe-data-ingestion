import os
from dotenv import load_dotenv
load_dotenv()

import time
import datetime as dt
import traceback
import pandas as pd
from entsoe import EntsoePandasClient
import psycopg2
from psycopg2.extras import execute_values

# ---------------- CONFIG ----------------

ENTSOE_API_TOKEN = os.getenv("ENTSOE_API_TOKEN")
SOURCE_NAME = "ENTSOE"

PG_HOST    = os.getenv("PG_HOST")
PG_PORT    = os.getenv("PG_PORT", "5432")
PG_DB      = os.getenv("PG_DB")
PG_USER    = os.getenv("PG_USER")
PG_PASS    = os.getenv("PG_PASS")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")

SLEEP_BETWEEN_ZONES_SEC = float(os.getenv("SLEEP_BETWEEN_ZONES_SEC", "1.0"))
ROLLING_WINDOW_DAYS = int(os.getenv("ROLLING_WINDOW_DAYS", "3"))

ZONES = [
    # Core Europe
    ("DE_LU", "DE-LU"),
    ("FR",    "FR"),
    ("NL",    "NL"),
    ("BE",    "BE"),
    ("AT",    "AT"),
    ("CH",    "CH"),
    ("PL",    "PL"),
    ("CZ",    "CZ"),
    ("SK",    "SK"),
    ("HU",    "HU"),
    ("SI",    "SI"),
    ("HR",    "HR"),

    # Iberia
    ("ES", "ES"),
    ("PT", "PT"),

    # Nordics
    ("NO_1", "NO1"),
    ("NO_2", "NO2"),
    ("NO_3", "NO3"),
    ("NO_4", "NO4"),
    ("NO_5", "NO5"),
    ("SE_1", "SE1"),
    ("SE_2", "SE2"),
    ("SE_3", "SE3"),
    ("SE_4", "SE4"),
    ("FI",   "FI"),
    ("DK_1", "DK1"),
    ("DK_2", "DK2"),

    # Baltics
    ("EE", "EE"),
    ("LV", "LV"),
    ("LT", "LT"),

    # Italy (split zones)
    ("IT_NORD", "IT-NORD"),
    ("IT_CNOR", "IT-CNOR"),
    ("IT_CSUD", "IT-CSUD"),
    ("IT_SUD",  "IT-SUD"),
    ("IT_SICI", "IT-SICI"),
    ("IT_SARD", "IT-SARD"),

    # Balkans
    ("RO", "RO"),
    ("BG", "BG"),
    ("GR", "GR"),
    ("RS", "RS"),
    ("BA", "BA"),
    ("ME", "ME"),

    # Ireland (SEM)
    ("IE_SEM", "IE-SEM"),
]

# ------------- CLIENT ----------------

_client = None

def get_client():
    global _client
    if _client is not None:
        return _client
    if not ENTSOE_API_TOKEN:
        raise RuntimeError("ENTSOE_API_TOKEN is not set")
    _client = EntsoePandasClient(api_key=ENTSOE_API_TOKEN)
    return _client

# ------------- FETCH ----------------

def fetch_load_series(country_code: str, bidding_zone: str,
                      start_utc: dt.datetime, end_utc: dt.datetime) -> pd.Series:
    print(f"[INFO] Fetching load for {country_code} ({bidding_zone}) "
          f"from {start_utc} → {end_utc}")

    client = get_client()

    start = pd.Timestamp(start_utc).tz_convert("Europe/Berlin")
    end   = pd.Timestamp(end_utc).tz_convert("Europe/Berlin")

    series = client.query_load(
        country_code=country_code,
        start=start,
        end=end,
    )
    print(f"[INFO] Got {len(series)} load points for {bidding_zone}")
    return series

# ------------- TRANSFORM ----------------

def series_to_records(series: pd.Series, bidding_zone: str):
    if series is None or (hasattr(series, 'empty') and series.empty):
        print(f"[WARN] No data returned from ENTSO-E for {bidding_zone}.")
        return []

    # query_load returns a DataFrame with actual + forecast columns — take actual load only
    if isinstance(series, pd.DataFrame):
        series = series.iloc[:, 0].dropna()

    series = series.copy()

    if series.index.tz is None:
        series.index = series.index.tz_localize("Europe/Berlin").tz_convert("UTC")
    else:
        series.index = series.index.tz_convert("UTC")

    records = []
    for ts, val in series.items():
        if pd.isna(val):
            continue
        records.append((
            ts.to_pydatetime(),
            bidding_zone,
            float(val),
            SOURCE_NAME,
        ))

    print(f"[INFO] Converted {len(records)} records for {bidding_zone}.")
    return records

# ------------- UPSERT ----------------

def upsert_load(conn, records):
    if not records:
        print("[WARN] Nothing to upsert.")
        return

    with conn.cursor() as cur:
        sql = """
            INSERT INTO load_ts
                (time_utc, bidding_zone, load_mw, source)
            VALUES %s
            ON CONFLICT (time_utc, bidding_zone, source)
            DO UPDATE SET
                load_mw    = EXCLUDED.load_mw,
                created_at = NOW();
        """
        execute_values(cur, sql, records, page_size=10_000)
    conn.commit()
    print(f"[INFO] Upserted {len(records)} rows.")

# ------------- MAIN ----------------

def main():
    now_utc   = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_utc = now_utc - dt.timedelta(days=ROLLING_WINDOW_DAYS)

    print(f"=== ENTSO-E Load update (rolling {ROLLING_WINDOW_DAYS}d) ===")
    print(f"[INFO] Window: {start_utc} → {now_utc}")
    print(f"[INFO] Zones: {len(ZONES)}")

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, sslmode=PG_SSLMODE,
    )
    print(f"[INFO] Connected to DB at {PG_HOST}:{PG_PORT}/{PG_DB}")

    failures = []

    try:
        for country_code, bidding_zone in ZONES:
            print(f"\n[ZONE] Processing {bidding_zone} ({country_code})")
            try:
                series  = fetch_load_series(country_code, bidding_zone, start_utc, now_utc)
                records = series_to_records(series, bidding_zone)
                upsert_load(conn, records)
            except Exception as e:
                print(f"[ERROR] Failed {bidding_zone}: {e}")
                traceback.print_exc()
                failures.append((bidding_zone, str(e)))
            time.sleep(SLEEP_BETWEEN_ZONES_SEC)

    finally:
        conn.close()
        print("[INFO] DB connection closed.")

    if failures:
        print("\n[SUMMARY] Some zones failed:")
        for bz, msg in failures:
            print(f"  - {bz}: {msg}")
        raise SystemExit(1)
    else:
        print("\n=== UPDATE COMPLETE ===")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except Exception as e:
        print("\n[UNCAUGHT ERROR]")
        traceback.print_exc()
        raise
