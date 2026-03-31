DROP VIEW IF EXISTS growth_merged_slice_la;

-- Hybrid facts: Pull rows plus MCP rows where no Pull slice exists (same date + dims).
-- In-app counts on Pull rows may be 0 while MCP has data; see growth_daily_totals_la
-- for day-level reconciliation (MAX of Pull vs MCP sums).

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
  SELECT * FROM mcp m
  WHERE NOT EXISTS (
    SELECT 1
    FROM pull p
    WHERE p.fact_date = m.fact_date
      AND COALESCE(p.media_source,'') = COALESCE(m.media_source,'')
      AND COALESCE(p.campaign,'') = COALESCE(m.campaign,'')
      AND COALESCE(p.adset,'') = COALESCE(m.adset,'')
      AND COALESCE(p.ad,'') = COALESCE(m.ad,'')
  )
)
SELECT *
FROM combined;
