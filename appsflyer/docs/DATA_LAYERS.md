# Data layers (AppsFlyer)

## Source of truth for agents and Slack

- **Engine:** SQLite file `data/appsflyer.db` under this package.
- **Raw / normalized tables:** e.g. `appsflyer_pull_daily_truth`, `appsflyer_mcp_source_rows`, `marketing_fact_daily` (populated by fetchers).
- **Gold views (refreshed by pipeline):** `growth_fact_daily`, `growth_daily_totals_la`, `growth_breakdowns_experimental`, `growth_weekly_totals_la` — applied from `sql/50_*.sql` through `sql/53_*.sql` by `python3 -m pipeline.apply_sqlite_views` or `python3 -m pipeline.run_incremental`.

Incremental ingest: `pipeline/run_incremental.sh` (see also `pipeline/crontab.example`).

## Experimental: DuckDB

Scripts `scripts/init_duckdb.py` and `scripts/build_marts.py` maintain an optional DuckDB file at `data/warehouse/dojo_marketing.duckdb` for staging/marts SQL (`00_`–`40_` only). This is **not** the same database the OpenClaw agents use today. Prefer SQLite + the pipeline for production reporting until DuckDB is explicitly wired to the same Gold semantics.

## Artifacts

- **Run manifest:** `data/artifacts/run_manifest.json` — last pipeline status, date window, freshness summary.
