-- Activity daily totals: event-date attribution from MCP, Pull-scaled.
--
-- MCP Unique users metrics are cumulative (inflated vs dashboard).
-- We scale MCP Activity to Pull media-source totals for correct absolute
-- numbers, same approach as LTV gold views.  At the daily-total level this
-- makes Activity = LTV, but preserves the Activity proportional distribution
-- at granular (adset/ad) levels.
--
-- When Master API becomes available, replace with direct Activity data.

DROP VIEW IF EXISTS activity_daily_totals_la;

CREATE VIEW activity_daily_totals_la AS
WITH
pull_day AS (
  SELECT
    fact_date,
    SUM(COALESCE(installs, 0)) AS installs,
    SUM(COALESCE(cost, 0)) AS spend,
    SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
    SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
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
    SUM(COALESCE(installs, 0)) AS installs,
    SUM(COALESCE(spend, 0)) AS spend,
    SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
    SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
    SUM(COALESCE(rc_trial_converted_event, 0)) AS rc_trial_converted_event,
    SUM(COALESCE(af_tutorial_completion, 0)) AS af_tutorial_completion,
    MAX(fetched_at) AS fetched_at
  FROM marketing_fact_daily
  WHERE source_system = 'appsflyer_mcp'
    AND attribution_model = 'activity'
  GROUP BY fact_date
),
days AS (
  SELECT fact_date FROM pull_day
  UNION
  SELECT fact_date FROM mcp_day
),
use_pull AS (
  SELECT d.fact_date,
    CASE
      WHEN COALESCE(p.spend, 0) > 0 THEN 1
      WHEN p.fact_date IS NOT NULL AND COALESCE(m.spend, 0) = 0 THEN 1
      ELSE 0
    END AS pull_wins
  FROM days d
  LEFT JOIN pull_day p ON p.fact_date = d.fact_date
  LEFT JOIN mcp_day m ON m.fact_date = d.fact_date
)
SELECT
  d.fact_date,
  'America/Los_Angeles' AS timezone,
  'activity' AS attribution_model,
  CASE WHEN u.pull_wins = 1 THEN COALESCE(p.installs, 0) ELSE COALESCE(m.installs, 0) END AS installs,
  CASE WHEN u.pull_wins = 1 THEN COALESCE(p.spend, 0) ELSE COALESCE(m.spend, 0) END AS spend,
  CASE WHEN u.pull_wins = 1 THEN COALESCE(p.af_start_trial, 0) ELSE COALESCE(m.af_start_trial, 0) END AS af_start_trial,
  CASE WHEN u.pull_wins = 1 THEN COALESCE(p.af_subscribe, 0) ELSE COALESCE(m.af_subscribe, 0) END AS af_subscribe,
  CASE WHEN u.pull_wins = 1 THEN COALESCE(p.rc_trial_converted_event, 0) ELSE COALESCE(m.rc_trial_converted_event, 0) END AS rc_trial_converted_event,
  CASE WHEN u.pull_wins = 1 THEN COALESCE(p.af_tutorial_completion, 0) ELSE COALESCE(m.af_tutorial_completion, 0) END AS af_tutorial_completion,
  CASE
    WHEN (CASE WHEN u.pull_wins = 1 THEN COALESCE(p.af_subscribe, 0) ELSE COALESCE(m.af_subscribe, 0) END) > 0
    THEN (CASE WHEN u.pull_wins = 1 THEN COALESCE(p.spend, 0) ELSE COALESCE(m.spend, 0) END) * 1.0
         / (CASE WHEN u.pull_wins = 1 THEN COALESCE(p.af_subscribe, 0) ELSE COALESCE(m.af_subscribe, 0) END)
    ELSE NULL
  END AS cac,
  CASE WHEN u.pull_wins = 1 THEN 'appsflyer_pull' ELSE 'appsflyer_mcp_activity' END AS primary_source,
  CASE WHEN u.pull_wins = 1 THEN 'high' ELSE 'medium' END AS source_confidence,
  COALESCE(p.fetched_at, m.fetched_at) AS fetched_at
FROM days d
LEFT JOIN pull_day p ON p.fact_date = d.fact_date
LEFT JOIN mcp_day m ON m.fact_date = d.fact_date
LEFT JOIN use_pull u ON u.fact_date = d.fact_date;
