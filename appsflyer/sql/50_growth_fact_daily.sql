DROP VIEW IF EXISTS growth_merged_slice_la;

-- Row-level facts from both Pull and MCP.  Both sources are included with a
-- source_system label so consumers can filter or compare.  The canonical
-- day-level totals live in growth_daily_totals_la (MAX of Pull vs MCP sums).

DROP VIEW IF EXISTS growth_fact_daily;

CREATE VIEW growth_fact_daily AS
SELECT
  fact_date,
  'appsflyer_pull' AS source_system,
  'ltv' AS attribution_model,
  media_source,
  campaign,
  adset,
  ad,
  installs,
  cost AS spend,
  af_start_trial,
  af_subscribe,
  revenue,
  rc_trial_converted_event,
  af_tutorial_completion,
  currency,
  timezone,
  fetched_at,
  'high' AS source_confidence
FROM appsflyer_pull_daily_truth
WHERE timezone = 'America/Los_Angeles'
UNION ALL
SELECT
  fact_date,
  'appsflyer_mcp' AS source_system,
  attribution_model,
  media_source,
  campaign,
  adset,
  ad,
  installs,
  spend,
  af_start_trial,
  af_subscribe,
  NULL AS revenue,
  rc_trial_converted_event,
  af_tutorial_completion,
  currency,
  timezone,
  fetched_at,
  'experimental' AS source_confidence
FROM marketing_fact_daily
WHERE source_system = 'appsflyer_mcp';
