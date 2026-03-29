# AppsFlyer ingestion (local cache)

Small Python utilities to pull AppsFlyer **aggregate** (Pull API / CSV) reports into SQLite, optionally pull **Master API** pivots as raw files, and run simple offline reports.

## Layout

- `common.py` — shared env loading, date validation, Bearer GET with retries
- `fetcher/fetch_appsflyer.py` — Pull aggregate: HTTP fetch, CSV parse, `daily_performance` table (optional **ad set** / **ad** when the CSV has them)
- `fetcher/fetch_appsflyer_master.py` — Master API: raw JSON/CSV to `data/master_raw/` (no SQLite normalization yet)
- `queries/query_appsflyer.py` — summaries and breakdowns over the Pull cache
- `data/appsflyer.db` — default SQLite path (directory is created on first run)

### Pull aggregate API vs Master API

| | **Pull aggregate** (`fetch_appsflyer.py`) | **Master API** (`fetch_appsflyer_master.py`) |
|---|-------------------------------------------|---------------------------------------------|
| **What it is** | Fixed-path CSV export per report segment | Parameterized pivot: `groupings` + `kpis` ([overview](https://dev.appsflyer.com/hc/reference/overview-9)) |
| **Output** | Normalized rows in SQLite | Raw file under `data/master_raw/` for inspection |
| **Ad / ad set** | Only if that CSV export includes columns | Request dimensions in `--groupings` if your account/network supports them ([Get Master Report](https://dev.appsflyer.com/hc/reference/master_api_get)) |
| **Freshness** | Per export rules | Often daily, with typical lag (see Help Center Master API article) |

Use Pull for a stable pipeline into `daily_performance`; use Master when you need richer breakdowns and are still validating field names and JSON/CSV shape.

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
- `APPSFLYER_MASTER_BASE` — Master API base URL (default: `https://hq1.appsflyer.com/api/master-agg-data/v4/app`)
- `APPSFLYER_MASTER_GROUPINGS` / `APPSFLYER_MASTER_KPIS` / `APPSFLYER_MASTER_FORMAT` — defaults for the Master fetcher CLI

## Fetch

The fetcher loads `.env` from this directory, requests a CSV aggregate report for `--from` / `--to`, and **upserts** normalized rows keyed by date, media source, campaign, ad set, and ad (NULLs for dimensions not present in the file). Use an AppsFlyer aggregate export that includes ad-level breakdown when you need **Ad** / **Ad set** columns; availability varies by ad network and report type.

```bash
python fetcher/fetch_appsflyer.py --from 2026-01-01 --to 2026-01-07
```

Use `--dry-run` to validate credentials resolution and schema without calling the API or inserting data.

**Important:** confirm the correct **report segment** and **auth style** in AppsFlyer’s docs for your account; adjust `REPORT_PATH_TEMPLATE`, `build_report_url()`, `fetch_report_csv()`, and `FIELD_ALIASES` in `fetch_appsflyer.py` if your export differs.

## Master API (raw)

Confirm query parameter names and allowed `groupings` / `kpis` values in the [developer reference](https://dev.appsflyer.com/hc/reference/master_api_get). Ad- and ad-set–level groupings are not available for every network.

```bash
# Example shape only — replace groupings/kpis with values from your docs
python fetcher/fetch_appsflyer_master.py \
  --from 2026-01-01 --to 2026-01-07 \
  --groupings "date,pid,c,af_adset,af_ad" \
  --kpis "impressions,clicks,installs,cost,revenue" \
  --format json --pretty

python fetcher/fetch_appsflyer_master.py --from 2026-01-01 --to 2026-01-02 --dry-run
```

Extra query keys: `--param timezone=...` (repeat as needed). Raw files are written under `data/master_raw/` (gitignored by default).

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

## MCP KPI fetcher (Phase 1)

Use AppsFlyer MCP (`fetch_aggregated_data`) to fetch KPI snapshots with ad/adset dimensions into table `mcp_kpi_performance`.

Required in `appsflyer/.env`:

- `APPSFLYER_APP_ID`
- `APPSFLYER_MCP_TOKEN`
- optional: `APPSFLYER_MCP_URL` (default: `https://mcp.appsflyer.com/auth/mcp`)

Run:

```bash
python fetcher/fetch_appsflyer_mcp.py --from 2026-02-28 --to 2026-03-28
python queries/query_appsflyer_mcp.py top-ads --from 2026-02-28 --to 2026-03-28 --kpi af_start_trial_unique_users
```

This fetcher pulls:
- `af_start_trial` unique users (primary)
- `af_subscribe` unique users
- `af_tutorial_completion` unique users
- `rc_trial_converted_event` unique users
- `ARPU` period `ltv`
