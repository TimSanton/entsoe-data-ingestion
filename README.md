# ENTSO-E Data Ingestion Pipeline

Automated data pipelines for fetching European electricity market data from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) into a TimescaleDB database, visualised in Grafana.

Built to support fundamental analysis of European power markets across generation mix, cross-border flows, day-ahead prices and capture rates by bidding zone.

---

## Architecture

```
ENTSO-E Transparency Platform API
        |
        v
Python ingestion scripts (entsoe-py)
        |
        v
TimescaleDB (PostgreSQL time-series database)
        |
        v
Grafana dashboard
```

GitHub Actions runs each script on a schedule, keeping the database current without manual intervention.

---

## Data Coverage

| Script | Data type | Lookback | Bidding zones |
|---|---|---|---|
| `import_day_ahead_prices.py` | Day-ahead prices (EUR/MWh) | 3 days + D+1 | 43 zones |
| `import_generation_recent_entsoepy.py` | Generation by fuel type (MW) | 48 hours | Major European zones |
| `import_scheduled_flows.py` | Cross-border scheduled flows (MW) | Rolling window | Key interconnectors |
| `update_load.py` | Actual and forecast load (MW) | Rolling window | Major European zones |

**Bidding zone coverage** includes all major European markets: AT, BE, BG, HR, CZ, DE-LU, DK1/2, EE, FI, FR, GR, HU, IE-SEM, IT-NORD/CNOR/CSUD/SUD/CALA/SICI/SARD, LV, LT, ME, NL, NO1-5, PL, PT, RO, RS, SK, SI, ES, SE1-4, CH, MK.

UA, TR and MD are excluded as they publish prices in local currency.

---

## Key Design Decisions

**Upsert on conflict** — all scripts use `INSERT ... ON CONFLICT DO UPDATE`, so re-runs are safe and backfilling does not create duplicates.

**Recursive window splitting** — the day-ahead price script automatically splits the fetch window and retries on HTTP errors, handling partial data availability across zones without failing the entire run.

**TimescaleDB** — time-series optimised storage with automatic partitioning by time, enabling fast range queries across the full dataset.

**All timestamps in UTC** — normalised at ingestion for consistent cross-zone comparison.

---

## Setup

### Prerequisites

- Python 3.9+
- PostgreSQL with TimescaleDB extension
- ENTSO-E API token (register at [transparency.entsoe.eu](https://transparency.entsoe.eu/))

### Install dependencies

```bash
pip install -r requirements.txt
```

### Environment variables

Set the following, either in a `.env` file locally or as GitHub Actions secrets for automated runs:

```
ENTSOE_API_TOKEN=
PG_HOST=
PG_PORT=5432
PG_DB=
PG_USER=
PG_PASS=
PG_SSLMODE=require
ROLLING_WINDOW_DAYS=3
```

### Run manually

```bash
python import_day_ahead_prices.py
python import_generation_recent_entsoepy.py
python import_scheduled_flows.py
python update_load.py
```

---

## GitHub Actions

Workflows run automatically on a schedule. Configure secrets in **Settings > Secrets > Actions** using the variable names above.

---

## Grafana Dashboard

The database feeds a Grafana dashboard covering:

- Day-ahead prices by bidding zone (EUR/MWh)
- Generation by fuel type (MW)
- Scheduled cross-border flows (MW)
- Capture rates by bidding zone and fuel type

Available on request.
