-- Canonical LA daily totals:
-- Installs, spend, revenue: sum from Pull only (dashboard grain, LA timezone).
-- In-app events (af_start_trial, af_subscribe, af_tutorial_completion, rc_trial_converted_event):
-- per calendar day, use the higher of Pull daily sum vs MCP daily sum so MCP (often UTC-
-- bucketed, finer adset rows) does not drop events when partners_by_date shows 0.

DROP VIEW IF EXISTS growth_daily_totals_la;

CREATE VIEW growth_daily_totals_la AS
WITH
pull_day AS (
  SELECT
    fact_date,
    SUM(COALESCE(installs, 0)) AS installs,
    SUM(COALESCE(cost, 0)) AS spend,
    SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
    SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
    SUM(COALESCE(revenue, 0)) AS revenue,
    SUM(COALESCE(rc_trial_converted_event, 0)) AS rc_trial_converted_event,
    SUM(COALESCE(af_tutorial_completion, 0)) AS af_tutorial_completion,
    MAX(fetched_at) AS fetched_at
  FROM appsflyer_pull_daily_truth
  WHERE timezone = 'America/Los_Angeles'
  GROUP BY fact_date
),
mcp_day AS (
  SELECT
    fact_date,
    SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
    SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
    SUM(COALESCE(rc_trial_converted_event, 0)) AS rc_trial_converted_event,
    SUM(COALESCE(af_tutorial_completion, 0)) AS af_tutorial_completion
  FROM marketing_fact_daily
  WHERE source_system = 'appsflyer_mcp'
  GROUP BY fact_date
),
days AS (
  SELECT fact_date FROM pull_day
  UNION
  SELECT fact_date FROM mcp_day
)
SELECT
  d.fact_date,
  'America/Los_Angeles' AS timezone,
  COALESCE(p.installs, 0) AS installs,
  COALESCE(p.spend, 0) AS spend,
  CASE
    WHEN COALESCE(p.af_start_trial, 0) >= COALESCE(m.af_start_trial, 0) THEN COALESCE(p.af_start_trial, 0)
    ELSE COALESCE(m.af_start_trial, 0)
  END AS af_start_trial,
  CASE
    WHEN COALESCE(p.af_subscribe, 0) >= COALESCE(m.af_subscribe, 0) THEN COALESCE(p.af_subscribe, 0)
    ELSE COALESCE(m.af_subscribe, 0)
  END AS af_subscribe,
  COALESCE(p.revenue, 0) AS revenue,
  CASE
    WHEN COALESCE(p.rc_trial_converted_event, 0) >= COALESCE(m.rc_trial_converted_event, 0)
      THEN COALESCE(p.rc_trial_converted_event, 0)
    ELSE COALESCE(m.rc_trial_converted_event, 0)
  END AS rc_trial_converted_event,
  CASE
    WHEN COALESCE(p.af_tutorial_completion, 0) >= COALESCE(m.af_tutorial_completion, 0)
      THEN COALESCE(p.af_tutorial_completion, 0)
    ELSE COALESCE(m.af_tutorial_completion, 0)
  END AS af_tutorial_completion,
  CASE
    WHEN CASE
           WHEN COALESCE(p.af_subscribe, 0) >= COALESCE(m.af_subscribe, 0) THEN COALESCE(p.af_subscribe, 0)
           ELSE COALESCE(m.af_subscribe, 0)
         END > 0
    THEN COALESCE(p.spend, 0) * 1.0 / CASE
           WHEN COALESCE(p.af_subscribe, 0) >= COALESCE(m.af_subscribe, 0) THEN COALESCE(p.af_subscribe, 0)
           ELSE COALESCE(m.af_subscribe, 0)
         END
    ELSE NULL
  END AS cac,
  'merged_pull_mcp' AS primary_source,
  'high' AS source_confidence,
  p.fetched_at AS fetched_at
FROM days d
LEFT JOIN pull_day p ON p.fact_date = d.fact_date
LEFT JOIN mcp_day m ON m.fact_date = d.fact_date;
