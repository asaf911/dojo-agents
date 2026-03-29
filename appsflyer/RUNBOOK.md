# AppsFlyer Agent Runbook

## Phase 1 deliverable
Stand up a local-first AppsFlyer ingestion stack with:
- immutable raw JSON snapshots
- DuckDB raw/staging/intermediate/mart schemas
- 2026 YTD backfill support
- D-1 + rolling 7-day incremental refresh policy

## Directory contract
- `../../data/appsflyer/raw/<report>/dt=YYYY-MM-DD/<extract_timestamp>.json`
- `../../data/warehouse/dojo_marketing.duckdb`
- `./sql/*.sql`
- `./scripts/*.py`

## Run order
1. Ensure directories exist
2. Create DuckDB schemas/tables
3. Write raw snapshot files from AppsFlyer MCP extracts
4. Register/load raw data into DuckDB
5. Build staging models
6. Build intermediate conformed daily models
7. Build marts
8. Run validation queries

## Backfill policy
- Initial: 2026-01-01 through current date
- Incremental: D-1 daily
- Correction window: reprocess last 7 days each run

## Validation checklist
- Daily spend total reconciles to source export/dashboard within acceptable tolerance
- Daily `af_start_trial` total reconciles to source report
- No duplicate natural keys in daily mart
- Null spikes on campaign/media_source/country investigated
- Top campaigns by spend and trial starts look sane

## Notes
- Raw is append-only/source-faithful
- Business logic belongs in normalized/mart layers
- Weekly/monthly are derived from daily marts only
