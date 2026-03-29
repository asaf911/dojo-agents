CREATE OR REPLACE TABLE mart_marketing.daily_acquisition AS
WITH perf AS (
  SELECT * FROM int_appsflyer.daily_performance_metrics
),
events AS (
  SELECT
    report_date,
    app_key,
    platform,
    media_source,
    campaign,
    campaign_id,
    adset,
    adset_id,
    ad,
    ad_id,
    country,
    SUM(CASE WHEN event_name = 'af_start_trial' THEN event_count ELSE 0 END) AS af_start_trial,
    SUM(CASE WHEN event_name = 'af_subscribe' THEN event_count ELSE 0 END) AS af_subscribe,
    SUM(CASE WHEN event_name = 'af_tutorial_completion' THEN event_count ELSE 0 END) AS af_tutorial_completion,
    SUM(CASE WHEN event_name = 'rc_trial_converted_event' THEN event_count ELSE 0 END) AS rc_trial_converted_event,
    SUM(COALESCE(revenue, 0)) AS revenue
  FROM int_appsflyer.daily_event_metrics
  GROUP BY ALL
)
SELECT
  COALESCE(perf.report_date, events.report_date) AS report_date,
  COALESCE(perf.app_key, events.app_key) AS app_key,
  COALESCE(perf.platform, events.platform) AS platform,
  COALESCE(perf.media_source, events.media_source) AS media_source,
  COALESCE(perf.campaign, events.campaign) AS campaign,
  COALESCE(perf.campaign_id, events.campaign_id) AS campaign_id,
  COALESCE(perf.adset, events.adset) AS adset,
  COALESCE(perf.adset_id, events.adset_id) AS adset_id,
  COALESCE(perf.ad, events.ad) AS ad,
  COALESCE(perf.ad_id, events.ad_id) AS ad_id,
  COALESCE(perf.country, events.country) AS country,
  COALESCE(perf.impressions, 0) AS impressions,
  COALESCE(perf.clicks, 0) AS clicks,
  COALESCE(perf.installs, 0) AS installs,
  COALESCE(perf.cost, 0) AS cost,
  COALESCE(events.af_start_trial, 0) AS af_start_trial,
  COALESCE(events.af_subscribe, 0) AS af_subscribe,
  COALESCE(events.af_tutorial_completion, 0) AS af_tutorial_completion,
  COALESCE(events.rc_trial_converted_event, 0) AS rc_trial_converted_event,
  COALESCE(events.revenue, 0) AS revenue,
  CASE WHEN COALESCE(perf.installs, 0) = 0 THEN NULL ELSE perf.cost / NULLIF(perf.installs, 0) END AS cost_per_install,
  CASE WHEN COALESCE(events.af_start_trial, 0) = 0 THEN NULL ELSE perf.cost / NULLIF(events.af_start_trial, 0) END AS cost_per_af_start_trial,
  CASE WHEN COALESCE(events.af_subscribe, 0) = 0 THEN NULL ELSE perf.cost / NULLIF(events.af_subscribe, 0) END AS cost_per_af_subscribe
FROM perf
FULL OUTER JOIN events
  ON perf.report_date = events.report_date
 AND perf.app_key = events.app_key
 AND COALESCE(perf.platform, '') = COALESCE(events.platform, '')
 AND COALESCE(perf.media_source, '') = COALESCE(events.media_source, '')
 AND COALESCE(perf.campaign_id, perf.campaign, '') = COALESCE(events.campaign_id, events.campaign, '')
 AND COALESCE(perf.adset_id, perf.adset, '') = COALESCE(events.adset_id, events.adset, '')
 AND COALESCE(perf.ad_id, perf.ad, '') = COALESCE(events.ad_id, events.ad, '')
 AND COALESCE(perf.country, '') = COALESCE(events.country, '');

CREATE OR REPLACE TABLE mart_marketing.weekly_acquisition AS
SELECT
  DATE_TRUNC('week', report_date) AS week_start,
  app_key,
  platform,
  media_source,
  campaign,
  campaign_id,
  adset,
  adset_id,
  ad,
  ad_id,
  country,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(installs) AS installs,
  SUM(cost) AS cost,
  SUM(af_start_trial) AS af_start_trial,
  SUM(af_subscribe) AS af_subscribe,
  SUM(af_tutorial_completion) AS af_tutorial_completion,
  SUM(rc_trial_converted_event) AS rc_trial_converted_event,
  SUM(revenue) AS revenue,
  CASE WHEN SUM(installs) = 0 THEN NULL ELSE SUM(cost) / NULLIF(SUM(installs), 0) END AS cost_per_install,
  CASE WHEN SUM(af_start_trial) = 0 THEN NULL ELSE SUM(cost) / NULLIF(SUM(af_start_trial), 0) END AS cost_per_af_start_trial,
  CASE WHEN SUM(af_subscribe) = 0 THEN NULL ELSE SUM(cost) / NULLIF(SUM(af_subscribe), 0) END AS cost_per_af_subscribe
FROM mart_marketing.daily_acquisition
GROUP BY ALL;

CREATE OR REPLACE TABLE mart_marketing.monthly_acquisition AS
SELECT
  DATE_TRUNC('month', report_date) AS month_start,
  app_key,
  platform,
  media_source,
  campaign,
  campaign_id,
  adset,
  adset_id,
  ad,
  ad_id,
  country,
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(installs) AS installs,
  SUM(cost) AS cost,
  SUM(af_start_trial) AS af_start_trial,
  SUM(af_subscribe) AS af_subscribe,
  SUM(af_tutorial_completion) AS af_tutorial_completion,
  SUM(rc_trial_converted_event) AS rc_trial_converted_event,
  SUM(revenue) AS revenue,
  CASE WHEN SUM(installs) = 0 THEN NULL ELSE SUM(cost) / NULLIF(SUM(installs), 0) END AS cost_per_install,
  CASE WHEN SUM(af_start_trial) = 0 THEN NULL ELSE SUM(cost) / NULLIF(SUM(af_start_trial), 0) END AS cost_per_af_start_trial,
  CASE WHEN SUM(af_subscribe) = 0 THEN NULL ELSE SUM(cost) / NULLIF(SUM(af_subscribe), 0) END AS cost_per_af_subscribe
FROM mart_marketing.daily_acquisition
GROUP BY ALL;
