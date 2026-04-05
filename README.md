# entsoe-data-ingestion

Automated pipelines for fetching European electricity market data from the [ENTSO-E Transparency Platform](https://transparency.entsoe.eu/) into a TimescaleDB database.

## Scripts

| Script | Description |
|---|---|
| `import_generation_recent_entsoepy.py` | Fetches last 48 hours of generation by fuel type |
| `import_day_ahead_prices.py` | Fetches last 3 days of day-ahead prices (EUR/MWh) |

Coverage: all major European bidding zones.

---

## GitHub Actions

Workflows run automatically on a schedule. Set the following secrets in **Settings → Secrets → Actions**:

`ENTSOE_API_TOKEN`, `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASS`, `PG_SSLMODE`

---

## Notes

- All timestamps stored in UTC
- UA, TR, MD excluded (publish prices in local currency)
