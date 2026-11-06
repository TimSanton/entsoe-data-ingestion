print(">>> TOP OF SCRIPT REACHED")

import os
import re
import time
import threading
import datetime as dt
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from dotenv import load_dotenv
load_dotenv()

from entsoe import EntsoePandasClient
import psycopg2
from psycopg2.extras import execute_values

# -------------------- CONFIG --------------------

ENTSOE_API_TOKEN = os.getenv("ENTSOE_API_TOKEN")
SOURCE_NAME      = "ENTSOE"

PG_HOST    = os.getenv("PG_HOST")
PG_PORT    = os.getenv("PG_PORT", "5432")
PG_DB      = os.getenv("PG_DB")
PG_USER    = os.getenv("PG_USER")
PG_PASS    = os.getenv("PG_PASS")
PG_SSLMODE = os.getenv("PG_SSLMODE", "require")

ROLLING_WINDOW_DAYS = int(os.getenv("ROLLING_WINDOW_DAYS", "3"))
REQUEST_DELAY       = float(os.getenv("REQUEST_DELAY", "1.0"))
RATE_LIMIT_WAIT     = int(os.getenv("RATE_LIMIT_WAIT", "60"))
MAX_WORKERS         = int(os.getenv("MAX_WORKERS", "6"))
# Optional fixed start (ISO date/datetime, UTC) for one-off backfills; overrides ROLLING_WINDOW_DAYS
BACKFILL_START      = os.getenv("BACKFILL_START")

# -------------------- CONTRACT TYPES --------------------

CONTRACT_TYPES = [
    ("DAY_AHEAD", True),
    ("TOTAL",     False),
]

# -------------------- ZONE FORMATTING --------------------

NORDIC_RE = re.compile(r'^(NO|SE|DK)_(\w+)$')


def fmt(zone: str) -> str:
    m = NORDIC_RE.match(zone)
    if m:
        rest = m.group(2).replace('_', '-')
        return f"{m.group(1)}{rest}"
    return zone.replace('_', '-')


# -------------------- EXCLUDED PAIRS --------------------

EXCLUDED_ZONES = {'IT_ROSN', 'IT_FOGN'}
EXCLUDED_PAIRS = {
    frozenset(['AL', 'RS']),
}


def is_excluded(frm: str, to: str) -> bool:
    if frm in EXCLUDED_ZONES or to in EXCLUDED_ZONES:
        return True
    if frozenset([frm, to]) in EXCLUDED_PAIRS:
        return True
    return False


# -------------------- NEIGHBOURS --------------------

NEIGHBOURS = {
    'CH':     ['AT', 'DE_LU', 'FR', 'IT_NORD'],
    'AT':     ['CH', 'CZ', 'DE_LU', 'HU', 'IT_NORD', 'SI'],
    'CZ':     ['AT', 'DE_LU', 'PL', 'SK'],
    'GB':     ['BE', 'FR', 'IE_SEM', 'NL', 'NO_2', 'DK_1'],
    'NO_2':   ['DE_LU', 'DK_1', 'NL', 'NO_1', 'NO_5', 'GB'],
    'HU':     ['AT', 'HR', 'RO', 'RS', 'SI', 'SK', 'UA'],
    'IT_NORD':['CH', 'FR', 'SI', 'AT', 'IT_CNOR'],
    'ES':     ['FR', 'PT'],
    'SI':     ['AT', 'HR', 'IT_NORD', 'HU'],
    'RS':     ['AL', 'BA', 'BG', 'HR', 'HU', 'ME', 'MK', 'RO'],
    'PL':     ['CZ', 'DE_LU', 'LT', 'SE_4', 'SK', 'UA'],
    'ME':     ['AL', 'BA', 'RS'],
    'DK_1':   ['DE_LU', 'DK_2', 'NO_2', 'SE_3', 'NL', 'GB'],
    'RO':     ['BG', 'HU', 'RS', 'UA'],
    'LT':     ['BY', 'LV', 'PL', 'RU_KGD', 'SE_4'],
    'BG':     ['GR', 'MK', 'RO', 'RS', 'TR'],
    'SE_3':   ['DK_1', 'FI', 'NO_1', 'SE_2', 'SE_4'],
    'LV':     ['EE', 'LT', 'RU'],
    'IE_SEM': ['GB'],
    'IE':     ['GB', 'NIE'],
    'NIE':    ['GB', 'IE'],
    'BA':     ['HR', 'ME', 'RS'],
    'NO_1':   ['NO_2', 'NO_3', 'NO_5', 'SE_3'],
    'SE_4':   ['DE_LU', 'DK_2', 'LT', 'PL', 'SE_3'],
    'NO_5':   ['NO_1', 'NO_2', 'NO_3'],
    'SK':     ['CZ', 'HU', 'PL', 'UA'],
    'EE':     ['FI', 'LV', 'RU'],
    'DK_2':   ['DE_LU', 'DK_1', 'SE_4'],
    'FI':     ['EE', 'NO_4', 'RU', 'SE_1', 'SE_3'],
    'NO_4':   ['SE_2', 'FI', 'NO_3', 'SE_1'],
    'SE_1':   ['FI', 'NO_4', 'SE_2'],
    'SE_2':   ['NO_3', 'NO_4', 'SE_1', 'SE_3'],
    'DE_LU':  ['AT', 'BE', 'CH', 'CZ', 'DK_1', 'DK_2', 'FR', 'NO_2', 'NL', 'PL', 'SE_4'],
    'MK':     ['BG', 'GR', 'RS'],
    'PT':     ['ES'],
    'GR':     ['AL', 'BG', 'MK', 'TR'],
    'NO_3':   ['NO_1', 'NO_4', 'NO_5', 'SE_2'],
    'IT_SUD': ['IT_CSUD', 'IT_CALA'],
    'IT_CSUD':['IT_CNOR', 'IT_SARD', 'IT_SUD'],
    'IT_CNOR':['IT_NORD', 'IT_CSUD'],
    'IT_SARD':['IT_CSUD'],
    'IT_SICI':['IT_CALA', 'MT'],
    'IT_CALA':['IT_SICI', 'IT_SUD'],
    'MT':     ['IT_SICI'],
    'HR':     ['BA', 'HU', 'RS', 'SI'],
}


def get_directed_pairs():
    seen = set()
    pairs = []
    for frm, neighbours in NEIGHBOURS.items():
        for to in neighbours:
            if is_excluded(frm, to):
                continue
            key = tuple(sorted([frm, to]))
            if key not in seen:
                seen.add(key)
                pairs.append((frm, to))
    return pairs


# -------------------- CLIENT --------------------

_local = threading.local()


def get_client():
    # one client (and HTTP session) per worker thread
    client = getattr(_local, "client", None)
    if client is None:
        if not ENTSOE_API_TOKEN:
            raise RuntimeError("ENTSOE_API_TOKEN is not set")
        client = EntsoePandasClient(api_key=ENTSOE_API_TOKEN)
        _local.client = client
    return client


# -------------------- FETCH WITH RETRY --------------------

def fetch_flows(from_zone: str, to_zone: str,
                start: pd.Timestamp, end: pd.Timestamp,
                dayahead: bool):
    client = get_client()
    for attempt in range(3):
        try:
            series = client.query_scheduled_exchanges(
                country_code_from=from_zone,
                country_code_to=to_zone,
                start=start,
                end=end,
                dayahead=dayahead,
            )
            time.sleep(REQUEST_DELAY)
            if series is None or (hasattr(series, 'empty') and series.empty):
                return None
            return series.tz_convert("UTC")
        except Exception as e:
            msg = str(e)
            if any(x in msg for x in ["No matching data found", "204", "No data"]):
                return None
            if "429" in msg:
                wait = RATE_LIMIT_WAIT * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s before retry "
                      f"{attempt + 1}/3...")
                time.sleep(wait)
                continue
            raise
    print(f"    Giving up after 3 retries.")
    return None


# -------------------- DB --------------------

def get_conn():
    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB,
        user=PG_USER, password=PG_PASS, sslmode=PG_SSLMODE,
    )


def upsert_rows(conn, rows: list):
    if not rows:
        return
    sql = """
        INSERT INTO scheduled_flows_ts
            (time_utc, from_zone, to_zone, flow_mw, contract_type, source)
        VALUES %s
        ON CONFLICT (time_utc, from_zone, to_zone, contract_type, source)
        DO UPDATE SET
            flow_mw    = EXCLUDED.flow_mw,
            created_at = NOW()
    """
    with conn.cursor() as cur:
        execute_values(cur, sql, rows)
    conn.commit()


# -------------------- CORE --------------------

def fetch_task(from_zone: str, to_zone: str,
               contract_label: str, dayahead: bool,
               start_utc: dt.datetime, end_utc: dt.datetime) -> list:
    """Runs in a worker thread: fetch one direction/contract and return DB rows."""
    ts_start = pd.Timestamp(start_utc).tz_convert("Europe/Brussels")
    ts_end   = pd.Timestamp(end_utc).tz_convert("Europe/Brussels")

    series = fetch_flows(from_zone, to_zone, ts_start, ts_end, dayahead)
    if series is None:
        return []

    return [
        (ts.to_pydatetime(), fmt(from_zone), fmt(to_zone),
         float(val), contract_label, SOURCE_NAME)
        for ts, val in series.items()
        if pd.notna(val)
    ]


def main():
    now_utc   = dt.datetime.now(dt.timezone.utc).replace(minute=0, second=0, microsecond=0)
    if BACKFILL_START:
        start_utc = pd.Timestamp(BACKFILL_START, tz="UTC").to_pydatetime()
    else:
        start_utc = now_utc - dt.timedelta(days=ROLLING_WINDOW_DAYS)
    end_utc   = now_utc + dt.timedelta(days=2)

    pairs = get_directed_pairs()
    tasks = [
        (from_zone, to_zone, contract_label, dayahead)
        for frm, to in pairs
        for from_zone, to_zone in [(frm, to), (to, frm)]
        for contract_label, dayahead in CONTRACT_TYPES
    ]
    print(f"=== ENTSO-E Scheduled Flows | window {start_utc} → {end_utc} ===")
    print(f"[INFO] {len(pairs)} border pairs, {len(tasks)} requests, {MAX_WORKERS} workers")

    conn = get_conn()
    grand_total = 0
    failures = []
    t0 = time.time()

    # fetch in parallel, upsert serially on the main thread's connection
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(fetch_task, f, t, c, d, start_utc, end_utc): (f, t, c)
            for f, t, c, d in tasks
        }
        for fut in as_completed(futures):
            f, t, c = futures[fut]
            label = f"{fmt(f)}->{fmt(t)} [{c}]"
            try:
                rows = fut.result()
                upsert_rows(conn, rows)
                grand_total += len(rows)
                print(f"    {label}: {len(rows)} rows")
            except Exception as e:
                conn.rollback()
                failures.append(label)
                print(f"    ERROR {label}: {e}")
                traceback.print_exc()

    conn.close()
    print(f"\n=== COMPLETE in {time.time() - t0:.0f}s. Total rows upserted: {grand_total} ===")
    if failures:
        print(f"[WARN] {len(failures)} failed: {', '.join(failures)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n[UNCAUGHT ERROR]")
        print(e)
        traceback.print_exc()

print(">>> BOTTOM OF SCRIPT REACHED")
