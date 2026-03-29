CREATE SCHEMA IF NOT EXISTS raw_appsflyer;
CREATE SCHEMA IF NOT EXISTS stg_appsflyer;
CREATE SCHEMA IF NOT EXISTS int_appsflyer;
CREATE SCHEMA IF NOT EXISTS mart_marketing;

CREATE TABLE IF NOT EXISTS raw_appsflyer.extract_batches (
  batch_id VARCHAR PRIMARY KEY,
  report_name VARCHAR NOT NULL,
  extract_date DATE NOT NULL,
  extract_started_at TIMESTAMP,
  ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  source_system VARCHAR DEFAULT 'appsflyer',
  request_params_json JSON,
  file_path VARCHAR NOT NULL,
  row_count BIGINT,
  status VARCHAR DEFAULT 'loaded'
);
