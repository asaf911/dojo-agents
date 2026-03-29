# AppsFlyer ingestion (two-layer model)

AppsFlyer is now modeled in two layers:

1. **AppsFlyer source of truth** — data as pulled from AppsFlyer MCP
2. **Normalized marketing facts** — daily analytics table for CAC / trends / rollups

This is the foundation for later blending with Meta Ads, Apple Search Ads, Mixpanel, OneSignal, and other systems.

## Layout

- `common.py` — shared env loading and date validation
- `fetcher/fetch_appsflyer_mcp.py` — fetch from AppsFlyer MCP into both layers
- `queries/query_appsflyer_mcp.py` — query normalized daily facts (CAC, daily, weekly, top entities)
- `fetcher/fetch_appsflyer_master.py` — optional raw Master API inspection
- `data/appsflyer.db` — SQLite database

## Layer 1 — AppsFlyer source of truth

Table: `appsflyer_mcp_source_rows`

Purpose:
- preserve AppsFlyer-shaped fetched rows
- keep provenance and fetch windows
- enable debugging / reprocessing / auditability

Main columns:
- `fetch_window_from`, `fetch_window_to`
- `fact_date`
- `media_source`, `campaign`, `adset`, `ad`
- `kpi_name`, `metric_column`, `metric_value`
- `installs`, `cost`
- `timezone`, `currency`, `fetched_at`
- `raw_row_json`, `raw_metadata_json`

## Layer 2 — normalized analytics

Table: `marketing_fact_daily`

Grain:
- one row per `fact_date x media_source x campaign x adset x ad`

Measures:
- `spend`
- `installs`
- `af_start_trial`
- `af_subscribe`
- `af_tutorial_completion`
- `rc_trial_converted_event`
- `arpu_ltv`

This is the table to use for:
- CAC = spend / af_subscribe
- day-over-day analysis
- week-over-week analysis
- month-over-month analysis
- top campaigns/adsets/ads by CAC or conversions

## Setup

Create `appsflyer/.env` (do not commit) with:

- `APPSFLYER_APP_ID`
- `APPSFLYER_MCP_TOKEN`
- optional: `APPSFLYER_MCP_URL`

## Fetch

Dry-run:

```bash
python fetcher/fetch_appsflyer_mcp.py --from 2026-03-01 --to 2026-03-28 --dry-run
```

Real fetch:

```bash
python fetcher/fetch_appsflyer_mcp.py --from 2026-03-01 --to 2026-03-28
```

The fetcher requests:
- dimensions: `Date`, `Media source`, `Campaign`, `Adset`, `Ad`
- metrics per KPI fetch:
  - target KPI
  - `Installs`
  - `Cost`

Current KPI set:
- `af_start_trial_unique_users`
- `af_subscribe_unique_users`
- `af_tutorial_completion_unique_users`
- `rc_trial_converted_event_unique_users`
- `arpu_ltv`

## Query

Summary CAC:

```bash
python queries/query_appsflyer_mcp.py summary --from 2026-01-01 --to 2026-03-28
```

Daily CAC:

```bash
python queries/query_appsflyer_mcp.py daily-cac --from 2026-01-01 --to 2026-03-28
```

Weekly CAC:

```bash
python queries/query_appsflyer_mcp.py weekly-cac --from 2026-01-01 --to 2026-03-28
```

Top campaigns:

```bash
python queries/query_appsflyer_mcp.py top-campaigns --from 2026-01-01 --to 2026-03-28 --limit 20
```

Top adsets:

```bash
python queries/query_appsflyer_mcp.py top-adsets --from 2026-01-01 --to 2026-03-28 --limit 20
```

Top ads:

```bash
python queries/query_appsflyer_mcp.py top-ads --from 2026-01-01 --to 2026-03-28 --limit 20
```

## Notes

- AppsFlyer MCP remains the ingestion/source layer, not the final business-truth layer.
- CAC and trend analysis should use `marketing_fact_daily`.
- Later, additional systems can enrich or reconcile normalized facts without mutating the raw AppsFlyer source layer.
