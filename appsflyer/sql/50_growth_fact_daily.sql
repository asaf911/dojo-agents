-- Canonical LA-aligned growth facts (initial hybrid model)
-- Pull API truth is preferred for daily totals / business-day buckets.
-- MCP remains available for richer exploratory dimensions and provenance.

DROP VIEW IF EXISTS growth_fact_daily;

CREATE VIEW growth_fact_daily AS
WITH pull AS (
  SELECT
    fact_date,
    'appsflyer_pull' AS source_system,
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
),
mcp AS (
  SELECT
    fact_date,
    'appsflyer_mcp' AS source_system,
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
),
combined AS (
  SELECT * FROM pull
  UNION ALL
  SELECT * FROM mcp
  WHERE NOT EXISTS (
    SELECT 1
    FROM pull p
    WHERE p.fact_date = mcp.fact_date
      AND COALESCE(p.media_source,'') = COALESCE(mcp.media_source,'')
      AND COALESCE(p.campaign,'') = COALESCE(mcp.campaign,'')
      AND COALESCE(p.adset,'') = COALESCE(mcp.adset,'')
      AND COALESCE(p.ad,'') = COALESCE(mcp.ad,'')
  )
)
SELECT *
FROM combined;
