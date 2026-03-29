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
- MCP extraction client for AppsFlyer
- actual scheduled job orchestration
- dependency installation (`duckdb` Python package missing in current environment)
- real source validation against live AppsFlyer exports

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
