-- Experimental breakdown layer for richer analysis.
-- Uses MCP because it has richer campaign/adset/ad dimensions.
-- This is NOT the canonical total layer.

DROP VIEW IF EXISTS growth_breakdowns_experimental;

CREATE VIEW growth_breakdowns_experimental AS
SELECT
  fact_date,
  media_source,
  campaign,
  adset,
  ad,
  installs,
  spend,
  af_start_trial,
  af_subscribe,
  rc_trial_converted_event,
  af_tutorial_completion,
  arpu_ltv,
  currency,
  timezone,
  fetched_at,
  CASE WHEN COALESCE(af_subscribe, 0) > 0
       THEN COALESCE(spend, 0) * 1.0 / af_subscribe
       ELSE NULL END AS cac,
  'appsflyer_mcp' AS primary_source,
  'experimental' AS source_confidence,
  'MCP breakdowns are richer but may not exactly match LA dashboard day buckets yet.' AS caveat
FROM marketing_fact_daily;
