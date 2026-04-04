
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

DEFAULT_FUEL_DETAIL = "Unknown"

# Rolling window:
# - default: hours
# - if ROLLING_WINDOW_DAYS > 0, it overrides hours
ROLLING_WINDOW_HOURS = int(os.getenv("ROLLING_WINDOW_HOURS", "48"))
ROLLING_WINDOW_DAYS  = int(os.getenv("ROLLING_WINDOW_DAYS", "0"))

# Chunk large windows to avoid huge ENTSO-E responses / timeouts
# For a 30-day run, CHUNK_DAYS=1 or 2 is usually best.
CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "0"))

# Small pause between zones to avoid hammering ENTSO-E / DB
SLEEP_BETWEEN_ZONES_SEC = float(os.getenv("SLEEP_BETWEEN_ZONES_SEC", "1.0"))

ZONES = [
    # Core Europe
    ("DE_LU", "DE-LU"),
    ("FR", "FR"),
    ("NL", "NL"),
    ("BE", "BE"),
    ("AT", "AT"),
    ("CH", "CH"),
    ("PL", "PL"),
    ("CZ", "CZ"),
    ("SK", "SK"),
    ("HU", "HU"),
    ("SI", "SI"),
    ("HR", "HR"),

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
    ("FI", "FI"),
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
    
    # Ireland (ISEM)
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

# ------------- HELPERS ----------------

def normalize_fuel_detail(x) -> str:
    if x is None:
        return DEFAULT_FUEL_DETAIL
    s = str(x).strip()
    return s if s else DEFAULT_FUEL_DETAIL

def normalize_fuel_type(x) -> str:
    if x is None:
        return "UnknownFuel"
    s = str(x).strip()
    return s if s else "UnknownFuel"

def iter_chunks(start_utc: dt.datetime, end_utc: dt.datetime, chunk_days: int):
    """
    Yield (chunk_start_utc, chunk_end_utc) pairs.
    """
    if chunk_days <= 0:
        yield start_utc, end_utc
        return
    cur = start_utc
    step = dt.timedelta(days=chunk_days)
    while cur < end_utc:
        nxt = min(cur + step, end_utc)
        yield cur, nxt
        cur = nxt

# ------------- FETCH ----------------

def fetch_generation_df(country_code: str, bidding_zone: str,
                        start_utc: dt.datetime, end_utc: dt.datetime) -> pd.DataFrame:
    print(f"[INFO] Fetching generation for {country_code} ({bidding_zone}) "
          f"from {start_utc} → {end_utc}")

    client = get_client()

    # Convert UTC -> Europe/Berlin for entsoe-py calls
    start = pd.Timestamp(start_utc).tz_convert("Europe/Berlin")
    end   = pd.Timestamp(end_utc).tz_convert("Europe/Berlin")

    df = client.query_generation(
        country_code=country_code,
        start=start,
        end=end,
        psr_type=None,
    )
    print(f"[INFO] Raw DataFrame shape for {bidding_zone}: {getattr(df, 'shape', None)}")
    return df

# ------------- TRANSFORM ----------------

def df_to_records(df: pd.DataFrame, bidding_zone: str):
    print(f"[INFO] Converting DataFrame to records for {bidding_zone}...")

    if df is None or df.empty:
        print(f"[WARN] No data returned from ENTSO-E for {bidding_zone}.")
        return []

    df = df.copy()

    if df.index.tz is None:
        df.index = df.index.tz_localize("Europe/Berlin").tz_convert("UTC")
    else:
        df.index = df.index.tz_convert("UTC")

    cols = list(df.columns)
    print(f"[INFO] {len(df)} timestamps, {len(cols)} PSR columns for {bidding_zone}")

    records = []
    for ts, row in df.iterrows():
        for psr in cols:
            val = row[psr]
            if pd.isna(val):
                continue

            if isinstance(psr, tuple):
                fuel_type_raw   = psr[0] if len(psr) >= 1 else None
                fuel_detail_raw = psr[1] if len(psr) >= 2 else None
            else:
                fuel_type_raw   = psr
                fuel_detail_raw = DEFAULT_FUEL_DETAIL

            records.append(
                (
                    ts.to_pydatetime(),
                    bidding_zone,
                    normalize_fuel_type(fuel_type_raw),
                    normalize_fuel_detail(fuel_detail_raw),
                    float(val),
                    SOURCE_NAME,
                )
            )

    print(f"[INFO] Converted {len(records)} records for {bidding_zone}.")
    return records

# ------------- UPSERT ----------------

def upsert_generation(records):
    if not records:
        print("[WARN] Nothing to upsert.")
        return

    print(f"[INFO] Connecting to TimescaleDB at {PG_HOST}:{PG_PORT}/{PG_DB}")

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        sslmode=PG_SSLMODE,
    )

    try:
        with conn.cursor() as cur:
            sql = """
                INSERT INTO generation_ts
                    (time_utc, bidding_zone, fuel_type, fuel_detail, value_mw, source)
                VALUES %s
                ON CONFLICT (time_utc, bidding_zone, fuel_type, fuel_detail, source)
                DO UPDATE SET
                    value_mw   = EXCLUDED.value_mw,
                    created_at = now();
            """
            execute_values(cur, sql, records, page_size=10_000)
        conn.commit()
        print(f"[INFO] Upserted {len(records)} rows.")
    finally:
        conn.close()

# ------------- MAIN ----------------

def main():
    print("=== ENTSO-E → Timescale import starting ===")

    now_utc = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0, tzinfo=dt.timezone.utc)

    if ROLLING_WINDOW_DAYS > 0:
        start_utc = now_utc - dt.timedelta(days=ROLLING_WINDOW_DAYS)
    else:
        start_utc = now_utc - dt.timedelta(hours=ROLLING_WINDOW_HOURS)

    print(f"[INFO] Import window: {start_utc} → {now_utc}")
    print(f"[INFO] Zones: {len(ZONES)} | Rolling hours: {ROLLING_WINDOW_HOURS} | Rolling days: {ROLLING_WINDOW_DAYS} | Chunk days: {CHUNK_DAYS}")

    failures = []

    for country_code, bidding_zone in ZONES:
        print(f"\n[ZONE] Processing {bidding_zone} ({country_code})")

        try:
            for chunk_start, chunk_end in iter_chunks(start_utc, now_utc, CHUNK_DAYS):
                print(f"[INFO] Chunk window: {chunk_start} → {chunk_end}")
                df = fetch_generation_df(country_code, bidding_zone, chunk_start, chunk_end)
                records = df_to_records(df, bidding_zone)
                upsert_generation(records)

        except Exception as e:
            print(f"[ERROR] Zone failed: {bidding_zone} ({country_code}) -> {e}")
            traceback.print_exc()
            failures.append((country_code, bidding_zone, str(e)))

        if SLEEP_BETWEEN_ZONES_SEC > 0:
            time.sleep(SLEEP_BETWEEN_ZONES_SEC)

    if failures:
        print("\n[SUMMARY] Some zones failed:")
        for cc, bz, msg in failures:
            print(f" - {bz} ({cc}): {msg}")
        raise RuntimeError(f"{len(failures)} zones failed")

    print("=== DONE ===")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n[UNCAUGHT ERROR]")
        print(e)
        print("\n[TRACEBACK]")
        traceback.print_exc()
        raise  # ensure GitHub Actions marks it as failed

