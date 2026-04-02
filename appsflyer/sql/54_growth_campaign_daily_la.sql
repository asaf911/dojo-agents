-- Campaign-level daily performance (LTV) from Pull API (authoritative, LA timezone).

DROP VIEW IF EXISTS growth_campaign_daily_la;

CREATE VIEW growth_campaign_daily_la AS
SELECT
  fact_date,
  'America/Los_Angeles' AS timezone,
  'ltv' AS attribution_model,
  media_source,
  campaign,
  SUM(COALESCE(installs, 0)) AS installs,
  SUM(COALESCE(cost, 0)) AS spend,
  SUM(COALESCE(revenue, 0)) AS revenue,
  SUM(COALESCE(clicks, 0)) AS clicks,
  SUM(COALESCE(impressions, 0)) AS impressions,
  SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
  SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
  SUM(COALESCE(rc_trial_converted_event, 0)) AS rc_trial_converted_event,
  SUM(COALESCE(af_tutorial_completion, 0)) AS af_tutorial_completion,
  CASE WHEN SUM(COALESCE(installs, 0)) > 0
       THEN SUM(COALESCE(cost, 0)) * 1.0 / SUM(installs)
       ELSE NULL END AS cost_per_install,
  CASE WHEN SUM(COALESCE(af_start_trial, 0)) > 0
       THEN SUM(COALESCE(cost, 0)) * 1.0 / SUM(af_start_trial)
       ELSE NULL END AS cost_per_trial,
  CASE WHEN SUM(COALESCE(af_subscribe, 0)) > 0
       THEN SUM(COALESCE(cost, 0)) * 1.0 / SUM(af_subscribe)
       ELSE NULL END AS cost_per_subscriber,
  'appsflyer_pull' AS primary_source,
  'high' AS source_confidence,
  MAX(fetched_at) AS fetched_at
FROM appsflyer_pull_daily_truth
WHERE timezone = 'America/Los_Angeles'
GROUP BY fact_date, media_source, campaign;
