-- Canonical LA-aligned daily totals for business reporting.
-- Pull API is the source of truth here when available.

DROP VIEW IF EXISTS growth_daily_totals_la;

CREATE VIEW growth_daily_totals_la AS
SELECT
  fact_date,
  'America/Los_Angeles' AS timezone,
  SUM(COALESCE(installs, 0)) AS installs,
  SUM(COALESCE(cost, 0)) AS spend,
  SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
  SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
  SUM(COALESCE(revenue, 0)) AS revenue,
  SUM(COALESCE(rc_trial_converted_event, 0)) AS rc_trial_converted_event,
  SUM(COALESCE(af_tutorial_completion, 0)) AS af_tutorial_completion,
  CASE WHEN SUM(COALESCE(af_subscribe, 0)) > 0
       THEN SUM(COALESCE(cost, 0)) * 1.0 / SUM(COALESCE(af_subscribe, 0))
       ELSE NULL END AS cac,
  'appsflyer_pull' AS primary_source,
  'high' AS source_confidence,
  MAX(fetched_at) AS fetched_at
FROM appsflyer_pull_daily_truth
WHERE timezone = 'America/Los_Angeles'
GROUP BY fact_date;
