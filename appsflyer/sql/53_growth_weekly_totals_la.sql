-- Weekly LA-aligned totals (LTV) derived from the validated daily totals layer.

DROP VIEW IF EXISTS growth_weekly_totals_la;

CREATE VIEW growth_weekly_totals_la AS
SELECT
  strftime('%Y-W%W', fact_date) AS year_week,
  MIN(fact_date) AS week_start,
  MAX(fact_date) AS week_end,
  'ltv' AS attribution_model,
  SUM(COALESCE(installs, 0)) AS installs,
  SUM(COALESCE(spend, 0)) AS spend,
  SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
  SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
  SUM(COALESCE(revenue, 0)) AS revenue,
  CASE WHEN SUM(COALESCE(af_subscribe, 0)) > 0
       THEN SUM(COALESCE(spend, 0)) * 1.0 / SUM(COALESCE(af_subscribe, 0))
       ELSE NULL END AS cac,
  CASE WHEN SUM(COALESCE(af_start_trial, 0)) > 0
       THEN SUM(COALESCE(spend, 0)) * 1.0 / SUM(COALESCE(af_start_trial, 0))
       ELSE NULL END AS cost_per_af_start_trial,
  'growth_daily_totals_la' AS source_layer,
  'high' AS source_confidence
FROM growth_daily_totals_la
GROUP BY 1
ORDER BY week_start;
