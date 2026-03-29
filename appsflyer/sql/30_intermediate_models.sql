CREATE OR REPLACE TABLE int_appsflyer.daily_event_metrics AS
SELECT
  report_date,
  COALESCE(app_id, app_name) AS app_key,
  platform,
  media_source,
  campaign,
  campaign_id,
  adset,
  adset_id,
  ad,
  ad_id,
  country,
  event_name,
  SUM(COALESCE(event_count, 0)) AS event_count,
  SUM(COALESCE(revenue, 0)) AS revenue
FROM stg_appsflyer.in_app_events
GROUP BY ALL;

CREATE OR REPLACE TABLE int_appsflyer.daily_performance_metrics AS
SELECT
  report_date,
  COALESCE(app_id, app_name) AS app_key,
  platform,
  media_source,
  campaign,
  campaign_id,
  adset,
  adset_id,
  ad,
  ad_id,
  country,
  SUM(COALESCE(impressions, 0)) AS impressions,
  SUM(COALESCE(clicks, 0)) AS clicks,
  SUM(COALESCE(installs, 0)) AS installs,
  SUM(COALESCE(cost, 0)) AS cost
FROM stg_appsflyer.aggregated_performance
GROUP BY ALL;
