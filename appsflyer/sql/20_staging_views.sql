CREATE OR REPLACE VIEW stg_appsflyer.aggregated_performance AS
SELECT
  _batch_id,
  _ingested_at,
  _extract_date AS report_date,
  json_extract_string(_payload, '$.app_id') AS app_id,
  json_extract_string(_payload, '$.app_name') AS app_name,
  json_extract_string(_payload, '$.platform') AS platform,
  json_extract_string(_payload, '$.media_source') AS media_source,
  json_extract_string(_payload, '$.campaign') AS campaign,
  json_extract_string(_payload, '$.campaign_id') AS campaign_id,
  json_extract_string(_payload, '$.adset') AS adset,
  json_extract_string(_payload, '$.adset_id') AS adset_id,
  json_extract_string(_payload, '$.ad') AS ad,
  json_extract_string(_payload, '$.ad_id') AS ad_id,
  json_extract_string(_payload, '$.country') AS country,
  TRY_CAST(json_extract_string(_payload, '$.impressions') AS DOUBLE) AS impressions,
  TRY_CAST(json_extract_string(_payload, '$.clicks') AS DOUBLE) AS clicks,
  TRY_CAST(json_extract_string(_payload, '$.installs') AS DOUBLE) AS installs,
  TRY_CAST(json_extract_string(_payload, '$.cost') AS DOUBLE) AS cost,
  _row_hash
FROM raw_appsflyer.aggregated_performance;

CREATE OR REPLACE VIEW stg_appsflyer.in_app_events AS
SELECT
  _batch_id,
  _ingested_at,
  _extract_date AS report_date,
  json_extract_string(_payload, '$.app_id') AS app_id,
  json_extract_string(_payload, '$.app_name') AS app_name,
  json_extract_string(_payload, '$.platform') AS platform,
  json_extract_string(_payload, '$.media_source') AS media_source,
  json_extract_string(_payload, '$.campaign') AS campaign,
  json_extract_string(_payload, '$.campaign_id') AS campaign_id,
  json_extract_string(_payload, '$.adset') AS adset,
  json_extract_string(_payload, '$.adset_id') AS adset_id,
  json_extract_string(_payload, '$.ad') AS ad,
  json_extract_string(_payload, '$.ad_id') AS ad_id,
  json_extract_string(_payload, '$.country') AS country,
  json_extract_string(_payload, '$.event_name') AS event_name,
  TRY_CAST(json_extract_string(_payload, '$.event_count') AS DOUBLE) AS event_count,
  TRY_CAST(json_extract_string(_payload, '$.revenue') AS DOUBLE) AS revenue,
  _row_hash
FROM raw_appsflyer.in_app_events;
