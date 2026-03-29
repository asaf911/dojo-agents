# AppsFlyer ingestion (local cache)

Small Python utilities to pull AppsFlyer **aggregate** (Pull API / CSV) reports into SQLite and run simple offline reports.

## Layout

- `fetcher/fetch_appsflyer.py` — HTTP fetch, CSV parse, `daily_performance` table (dimensions include optional **ad set** and **ad** when the export provides them)
- `queries/query_appsflyer.py` — summaries and breakdowns over the cache
- `data/appsflyer.db` — default SQLite path (directory is created on first run)

## Setup

```bash
cd /root/dojo-agents/appsflyer
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Create `appsflyer/.env` (do not commit) with:

- `APPSFLYER_APP_ID` — your app identifier in AppsFlyer
- `APPSFLYER_API_TOKEN` — API V2 token from the dashboard

Optional environment variables:

- `APPSFLYER_SQLITE_PATH` — override DB path (default: `data/appsflyer.db` under this folder)
- `APPSFLYER_AGG_BASE` — aggregate export base URL if your tenant uses another host
- `APPSFLYER_REPORT_SEGMENT` — default report path segment (see fetcher help)

## Fetch

The fetcher loads `.env` from this directory, requests a CSV aggregate report for `--from` / `--to`, and **upserts** normalized rows keyed by date, media source, campaign, ad set, and ad (NULLs for dimensions not present in the file). Use an AppsFlyer aggregate export that includes ad-level breakdown when you need **Ad** / **Ad set** columns; availability varies by ad network and report type.

```bash
python fetcher/fetch_appsflyer.py --from 2026-01-01 --to 2026-01-07
```

Use `--dry-run` to validate credentials resolution and schema without calling the API or inserting data.

**Important:** confirm the correct **report segment** and **auth style** in AppsFlyer’s docs for your account; adjust `REPORT_PATH_TEMPLATE`, `build_report_url()`, `fetch_report_csv()`, and `FIELD_ALIASES` in `fetch_appsflyer.py` if your export differs.

## Query

After pulling a fetcher upgrade, run `python fetcher/fetch_appsflyer.py --from YYYY-MM-DD --to YYYY-MM-DD --dry-run` once so the local DB migrates (adds `ad` / `adset` and the widened unique key) before using new query modes.

```bash
python queries/query_appsflyer.py summary --from 2026-01-01 --to 2026-01-07
python queries/query_appsflyer.py media-sources --from 2026-01-01 --to 2026-01-07
python queries/query_appsflyer.py campaigns --from 2026-01-01 --to 2026-01-07
python queries/query_appsflyer.py adsets --from 2026-01-01 --to 2026-01-07
python queries/query_appsflyer.py ads --from 2026-01-01 --to 2026-01-07
```

Breakdowns reflect whatever dimensions were in the ingested CSV. If **adsets** / **ads** show `(unknown)` only, your Pull export likely does not include those columns—confirm the report segment and field mapping in `fetch_appsflyer.py` (`FIELD_ALIASES`).
