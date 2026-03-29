# Reconciliation Summary

## Status
Completed for the current SQLite-backed AppsFlyer export path.

## What was checked
- Existing AppsFlyer MCP fetch path executes successfully
- Current SQLite fact store contains 2026 YTD coverage
- SQLite-backed source/fact data was exported into warehouse-style raw + normalized artifacts
- Exported normalized artifact was validated for parity against `marketing_fact_daily`

## Validation result
Parity check passed.

### Counts
- SQLite rows: `1078`
- Exported normalized rows: `1078`
- SQLite unique business keys: `1063`
- Export unique business keys: `1063`
- Keys only in SQLite: `0`
- Keys only in export: `0`
- Metric mismatches sampled: `0`

## Output artifacts
- Raw snapshots: `appsflyer/data/raw/<report>/dt=YYYY-MM-DD/sqlite_export.json`
- Normalized export: `appsflyer/data/warehouse_exports/marketing_fact_daily.json`
- Parity summary JSON: `appsflyer/data/warehouse_exports/parity_summary.json`
- Export summary JSON: `appsflyer/data/warehouse_exports/export_summary.json`

## Practical conclusion
The existing AppsFlyer backfill is present and verified, and the warehouse-style normalized export matches the live SQLite reporting table for the checked fields.

## What remains outside this task
- Dashboard/UI reconciliation signoff against AppsFlyer external views
- Optional cutover from SQLite-native reporting path to a new DuckDB/warehouse-native reporting path
- Scheduling / automation polish
