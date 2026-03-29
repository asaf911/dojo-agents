CREATE TABLE IF NOT EXISTS raw_appsflyer.aggregated_performance (
  _batch_id VARCHAR,
  _ingested_at TIMESTAMP,
  _extract_date DATE,
  _source_system VARCHAR,
  _report_name VARCHAR,
  _request_params JSON,
  _row_hash VARCHAR,
  _payload JSON
);

CREATE TABLE IF NOT EXISTS raw_appsflyer.in_app_events (
  _batch_id VARCHAR,
  _ingested_at TIMESTAMP,
  _extract_date DATE,
  _source_system VARCHAR,
  _report_name VARCHAR,
  _request_params JSON,
  _row_hash VARCHAR,
  _payload JSON
);

CREATE TABLE IF NOT EXISTS raw_appsflyer.cohort_revenue (
  _batch_id VARCHAR,
  _ingested_at TIMESTAMP,
  _extract_date DATE,
  _source_system VARCHAR,
  _report_name VARCHAR,
  _request_params JSON,
  _row_hash VARCHAR,
  _payload JSON
);
