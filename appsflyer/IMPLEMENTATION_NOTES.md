# Implementation Notes

## What is done
- Concrete Phase 1 ingestion/storage architecture is defined
- Repo scaffold exists under `dojo-agents/appsflyer/`
- SQL contracts exist for:
  - schema initialization
  - raw tables
  - staging views
  - intermediate models
  - daily/weekly/monthly marts
  - validation queries
- Starter Python scripts exist for:
  - directory bootstrap
  - DuckDB init
  - raw JSON loading
  - mart rebuilds
- Example payloads exist for performance + in-app events

## What is not done yet
- actual scheduled job orchestration
- dependency installation for the DuckDB branch of the scaffold if we want to use that path
- formal validation against AppsFlyer dashboard exports for reconciliation signoff
- final consolidation between the existing SQLite production path and the newer warehouse-style scaffold

## What changed after deeper inspection
- A working AppsFlyer MCP fetcher already exists in this repo
- A populated SQLite store already exists at `appsflyer/data/appsflyer.db`
- 2026 YTD coverage is present in the current normalized store
- The immediate task is therefore migration/formalization, not greenfield ingestion from zero

## Immediate next coding steps
1. Add an AppsFlyer MCP extraction script that writes JSON snapshots into `data/appsflyer/raw/...`
2. Install `duckdb` and run:
   - `python3 dojo-agents/appsflyer/scripts/bootstrap_dirs.py`
   - `python3 dojo-agents/appsflyer/scripts/init_duckdb.py`
3. Load sample or live raw files:
   - `python3 dojo-agents/appsflyer/scripts/load_raw_json.py`
4. Build marts:
   - `python3 dojo-agents/appsflyer/scripts/build_marts.py`
5. Run validation queries from `sql/90_validation_queries.sql`

## Design stance
This is intentionally local-first and warehouse-portable. If the target later becomes BigQuery/Snowflake, the schema contract and layer boundaries can move without changing the basic data model.
