# Data layers (AppsFlyer)

## Business calendar contract

Gold and silver layers use a single meaning for **`fact_date`**: the **business calendar date** in the timezone named by **`APPSFLYER_BUSINESS_TIMEZONE`** (default `America/Los_Angeles`). Helpers live in [`common.py`](../common.py): `business_timezone_name()`, `utc_calendar_date_to_business_date()`, `is_utc_like_report_tz()`.

**Consumers (OpenClaw, Slack, scripts):** Use `freshness.yesterday_la` from [`data/artifacts/run_manifest.json`](../data/artifacts/run_manifest.json) for “yesterday.” Do **not** use SQLite `date('now', '-1 day')` or the server host timezone to infer the business day. The manifest also includes `business_timezone` and `gold_fact_date_semantics` (`Los_Angeles_calendar_date`) so agents do not guess.

| Layer | Objects | `fact_date` meaning | `timezone` column |
| ----- | ------- | ------------------- | ----------------- |
| Bronze | `appsflyer_mcp_source_rows` | Vendor calendar day (often UTC) | As returned by MCP |
| Bronze | `appsflyer_pull_daily_truth` | Business day (Pull request uses business TZ) | `APPSFLYER_BUSINESS_TIMEZONE` |
| Silver | `marketing_fact_daily` where `source_system = 'appsflyer_mcp'` | **Business calendar day** (UTC MCP rows remapped at rebuild) | Business TZ after normalization |
| Gold | `growth_daily_totals_la`, `growth_weekly_totals_la`, `growth_fact_daily` (Pull side) | **Business calendar day** | Literal / constant LA (or Pull-filtered LA) |

## Source of truth for agents and Slack

- **Engine:** SQLite file `data/appsflyer.db` under this package.
- **Raw / normalized tables:** e.g. `appsflyer_pull_daily_truth`, `appsflyer_mcp_source_rows`, `marketing_fact_daily` (populated by fetchers). MCP bronze rows may stay UTC-dated; `fetch_appsflyer_mcp.rebuild_marketing_fact_daily` maps UTC-like report days to the business calendar via `utc_calendar_date_to_business_date()` so in-app events align with Pull and “yesterday (LA)” reports.
- **Pull truth (`appsflyer_pull_daily_truth`):** Each fetch **replaces** rows for the requested date range (same report segment + timezone), then **dedupes** identical CSV duplicates. Multiple rows with empty dimensions but **different metrics** are kept so `growth_daily_totals_la` can `SUM` to dashboard totals. Legacy tables used a dimension-only `UNIQUE` that (with SQLite NULL rules) duplicated rows or, after normalization, overwrote sibling slices — migrated away on next `init_schema`. CSV column names such as `Media Source (pid)` are normalized to the same fields as `Media Source` so paid + organic rows all load.
- **Gold views (refreshed by pipeline):** `growth_fact_daily`, `growth_daily_totals_la`, `growth_breakdowns_experimental`, `growth_weekly_totals_la` — applied from `sql/50_*.sql` through `sql/53_*.sql` by `python3 -m pipeline.apply_sqlite_views` or `python3 -m pipeline.run_incremental`.
- **`growth_daily_totals_la` (canonical day totals):** Sums **installs, spend, and revenue** from Pull only (business timezone). For in-app event columns (`af_start_trial`, `af_subscribe`, `af_tutorial_completion`, `rc_trial_converted_event`), each day uses the **larger** of the Pull daily sum and the MCP daily sum so events are not dropped when MCP grain differs from Pull while avoiding double-counting installs/spend from overlapping MCP slices. `primary_source` is `merged_pull_mcp`.

Incremental ingest: `pipeline/run_incremental.sh` (see also `pipeline/crontab.example`).

## Experimental: DuckDB

Scripts `scripts/init_duckdb.py` and `scripts/build_marts.py` maintain an optional DuckDB file at `data/warehouse/dojo_marketing.duckdb` for staging/marts SQL (`00_`–`40_` only). This is **not** the same database the OpenClaw agents use today. Prefer SQLite + the pipeline for production reporting until DuckDB is explicitly wired to the same Gold semantics.

## Artifacts

- **Run manifest:** `data/artifacts/run_manifest.json` — last pipeline status, date window, freshness summary, `business_timezone`, `gold_fact_date_semantics`, and **`ingest`** (`skip_pull` / `skip_mcp`, `pull_attempted` / `mcp_attempted`) so consumers know whether AppsFlyer fetches ran.
- **OpenClaw marketing / appsflyer-ops workspaces** symlink this file and `data/appsflyer.db` as `data/artifacts/run_manifest.json` and `data/appsflyer.db` so workspace-scoped tools can read them.
- **Rolling-window helper:** `scripts/growth_totals_summary.py` prints JSON aggregates from `growth_daily_totals_la` (read-only).
