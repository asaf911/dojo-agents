# AppsFlyer Agent

Phase 1 implementation scaffold for source-faithful AppsFlyer ingestion.

## Goals
- Land immutable raw AppsFlyer extracts to partitioned JSON
- Load/query raw data in DuckDB
- Build normalized daily marketing marts
- Support 2026 YTD backfill + daily incrementals

## Paths
- Raw snapshots: `../../data/appsflyer/raw/`
- DuckDB warehouse: `../../data/warehouse/dojo_marketing.duckdb`
- SQL models: `./sql/`
- Scripts: `./scripts/`
- Runbook: `./RUNBOOK.md`

## Initial reports
- aggregated_performance
- in_app_events
- cohort_revenue (Phase 1.1 if MCP support is missing)
