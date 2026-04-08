print(">>> TOP OF SCRIPT REACHED")

import os
from dotenv import load_dotenv
load_dotenv()

import datetime as dt
import traceback
import pandas as pd
import requests
from entsoe import EntsoePandasClient
from entsoe.exceptions import NoMatchingDataError
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

ROLLING_WINDOW_DAYS = int(os.getenv("ROLLING_WINDOW_DAYS", "3"))

ZONES = [
    ("AT",      "AT"),
    ("BE",      "BE"),
    ("BG",      "BG"),
    ("HR",      "HR"),
    ("CZ",      "CZ"),
    ("DE_LU",   "DE-LU"),
    ("DK_1",    "DK1"),
    ("DK_2",    "DK2"),
    ("EE",      "EE"),
    ("FI",      "FI"),
    ("FR",      "FR"),
    ("GR",      "GR"),
    ("HU",      "HU"),
    ("IE_SEM",  "IE-SEM"),
    ("IT_NORD", "IT-NORD"),
    ("IT_CNOR", "IT-CNOR"),
    ("IT_CSUD", "IT-CSUD"),
    ("IT_SUD",  "IT-SUD"),
    ("IT_CALA", "IT-CALA"),
    ("IT_SICI", "IT-SICI"),
    ("IT_SARD", "IT-SARD"),
    ("LV",      "LV"),
    ("LT",      "LT"),
    ("ME",      "ME"),
    ("NL",      "NL"),
    ("NO_1",    "NO1"),
    ("NO_2",    "NO2"),
    ("NO_3",    "NO3"),
    ("NO_4",    "NO4"),
    ("NO_5",    "NO5"),
    ("PL",      "PL"),
    ("PT",      "PT"),
    ("RO",      "RO"),
    ("RS",      "RS"),
    ("SK",      "SK"),
    ("SI",      "SI"),
    ("ES",      "ES"),
    ("SE_1",    "SE1"),
    ("SE_2",    "SE2"),
    ("SE_3",    "SE3"),
    ("SE_4",    "SE4"),
    ("CH",      "CH"),
    ("MK",      "MK"),
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

# ------------- ENTSO-E FETCH ----------------

def fetch_prices(country_code: str, start_utc: dt.datetime, end_utc: dt.datetime,
                 depth: int = 0, max_depth: int = 3):
    print(f"[INFO] Fetching day-ahead prices for {country_code} from {start_utc} → {end_utc} (depth={depth})")
    client = get_client()

    start = pd.Timestamp(start_utc).tz_convert("Europe/Berlin")
    end   = pd.Timestamp(end_utc).tz_convert("Europe/Berlin")

    try:
        series = client.query_day_ahead_prices(
            country_code=country_code,
            start=start,
            end=end,
        )
        print(f"[INFO] Got {len(series)} price points for {country_code}")
        return series

    except NoMatchingDataError:
        print(f"[WARN] No data available for {country_code}, skipping.")
        return None

    except requests.HTTPError as e:
        body = getattr(e.response, "text", "") if hasattr(e, "response") else ""
        print("[ERROR] HTTPError from ENTSO-E:", e)
        if body:
            print("[ERROR] Response body (first 1000 chars):")
            print(body[:1000])

        body_lower = body.lower()

        no_data_markers = [
            "no data",
            "no matching data",
            "no timeseries",
            "no matching time series",
            "no corresponding data",
        ]
        if any(marker in body_lower for marker in no_data_markers):
            print("[WARN] ENTSO-E reports no data for this window, skipping.")
            return None

        total_seconds = (end_utc - start_utc).total_seconds()
        if depth < max_depth and total_seconds > 24 * 3600:
            print("[WARN] Unexpected HTTP error; attempting to split window and retry.")
            mid_utc = start_utc + (end_utc - start_utc) / 2

            left  = fetch_prices(country_code, start_utc, mid_utc, depth=depth + 1, max_depth=max_depth)
            right = fetch_prices(country_code, mid_utc,   end_utc, depth=depth + 1, max_depth=max_depth)

            if left is None and right is None:
                return None
            elif left is None:
                return right
            elif right is None:
                return left
            else:
                return pd.concat([left, right]).sort_index()

        print("[ERROR] Cannot split further. Stopping.")
        raise

# ------------- TRANSFORM ----------------

def series_to_records(series, bidding_zone: str):
    if series is None or series.empty:
        print(f"[WARN] No price data for {bidding_zone}.")
        return []

    series = series.copy()

    if series.index.tz is None:
        series.index = series.index.tz_localize("UTC")
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

    print(f"[INFO] Converted {len(records)} price records for {bidding_zone}.")
    return records

# ------------- DB UPSERT ----------------

def upsert_prices(conn, records):
    if not records:
        print("[WARN] Nothing to upsert.")
        return

    with conn.cursor() as cur:
        sql = """
            INSERT INTO day_ahead_prices_ts
                (time_utc, bidding_zone, price_eur_mwh, source)
            VALUES %s
            ON CONFLICT (time_utc, bidding_zone, source)
            DO UPDATE SET price_eur_mwh = EXCLUDED.price_eur_mwh;
        """
        execute_values(cur, sql, records)
    conn.commit()
    print(f"[INFO] Upserted {len(records)} price rows.")

# ------------- MAIN ----------------

def main():
    print(f"=== ENTSO-E Day-Ahead Prices | Rolling {ROLLING_WINDOW_DAYS}d window ===")

    now_utc   = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    start_utc = now_utc - dt.timedelta(days=ROLLING_WINDOW_DAYS)
    end_utc   = now_utc + dt.timedelta(days=2)  # include tomorrow's published prices

    print(f"[INFO] Fetching from {start_utc} → {end_utc}")
    print(f"[INFO] Zones: {len(ZONES)}")

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode=PG_SSLMODE,
    )
    print(f"[INFO] Connected to DB at {PG_HOST}:{PG_PORT}/{PG_DB}")

    try:
        for country_code, bidding_zone in ZONES:
            print(f"\n[ZONE] Processing {bidding_zone} ({country_code})")
            try:
                series  = fetch_prices(country_code, start_utc, end_utc)
                records = series_to_records(series, bidding_zone)
                upsert_prices(conn, records)
            except Exception as e:
                print(f"[ERROR] Failed for {bidding_zone}: {e}")
                traceback.print_exc()
                print("[INFO] Continuing to next zone...")
    finally:
        conn.close()
        print("[INFO] DB connection closed.")

    print("\n=== COMPLETE ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n[UNCAUGHT ERROR]")
        print(e)
        print("\n[TRACEBACK]")
        traceback.print_exc()

print(">>> BOTTOM OF SCRIPT REACHED")
