-- Adset-level daily performance (LTV) from MCP, proportionally scaled to Pull
-- media-source totals (authoritative).  When Pull has no data for a
-- media_source on a given day, MCP adset values pass through unscaled.

DROP VIEW IF EXISTS growth_adset_daily_la;

CREATE VIEW growth_adset_daily_la AS
WITH
pull_ms AS (
  SELECT
    fact_date, media_source,
    SUM(COALESCE(cost, 0)) AS spend,
    SUM(COALESCE(installs, 0)) AS installs,
    SUM(COALESCE(af_start_trial, 0)) AS trials,
    SUM(COALESCE(af_subscribe, 0)) AS subs
  FROM appsflyer_pull_daily_truth
  WHERE timezone = 'America/Los_Angeles'
  GROUP BY fact_date, media_source
),
mcp_adset AS (
  SELECT
    fact_date, media_source, campaign, adset,
    SUM(COALESCE(spend, 0)) AS spend,
    SUM(COALESCE(installs, 0)) AS installs,
    SUM(COALESCE(af_start_trial, 0)) AS af_start_trial,
    SUM(COALESCE(af_subscribe, 0)) AS af_subscribe,
    SUM(COALESCE(rc_trial_converted_event, 0)) AS rc_trial_converted_event,
    SUM(COALESCE(af_tutorial_completion, 0)) AS af_tutorial_completion,
    MAX(currency) AS currency,
    MAX(fetched_at) AS fetched_at
  FROM marketing_fact_daily
  WHERE source_system = 'appsflyer_mcp'
    AND attribution_model = 'ltv'
  GROUP BY fact_date, media_source, campaign, adset
),
mcp_adset_ms AS (
  SELECT fact_date, media_source,
    SUM(spend) AS m_spend,
    SUM(installs) AS m_installs,
    SUM(af_start_trial) AS m_trials,
    SUM(af_subscribe) AS m_subs
  FROM mcp_adset
  GROUP BY fact_date, media_source
)
SELECT
  a.fact_date,
  'America/Los_Angeles' AS timezone,
  'ltv' AS attribution_model,
  a.media_source,
  a.campaign,
  a.adset,
  a.spend    * COALESCE(p.spend    / NULLIF(t.m_spend, 0),    1.0) AS spend,
  a.installs * COALESCE(p.installs / NULLIF(t.m_installs, 0), 1.0) AS installs,
  a.af_start_trial * COALESCE(p.trials / NULLIF(t.m_trials, 0), 1.0) AS af_start_trial,
  a.af_subscribe   * COALESCE(p.subs   / NULLIF(t.m_subs, 0),   1.0) AS af_subscribe,
  a.rc_trial_converted_event AS rc_trial_converted_event,
  a.af_tutorial_completion AS af_tutorial_completion,
  CASE WHEN a.installs * COALESCE(p.installs / NULLIF(t.m_installs, 0), 1.0) > 0
       THEN a.spend * COALESCE(p.spend / NULLIF(t.m_spend, 0), 1.0) * 1.0
            / (a.installs * COALESCE(p.installs / NULLIF(t.m_installs, 0), 1.0))
       ELSE NULL END AS cost_per_install,
  CASE WHEN a.af_start_trial * COALESCE(p.trials / NULLIF(t.m_trials, 0), 1.0) > 0
       THEN a.spend * COALESCE(p.spend / NULLIF(t.m_spend, 0), 1.0) * 1.0
            / (a.af_start_trial * COALESCE(p.trials / NULLIF(t.m_trials, 0), 1.0))
       ELSE NULL END AS cost_per_trial,
  CASE WHEN a.af_subscribe * COALESCE(p.subs / NULLIF(t.m_subs, 0), 1.0) > 0
       THEN a.spend * COALESCE(p.spend / NULLIF(t.m_spend, 0), 1.0) * 1.0
            / (a.af_subscribe * COALESCE(p.subs / NULLIF(t.m_subs, 0), 1.0))
       ELSE NULL END AS cost_per_subscriber,
  a.currency,
  CASE WHEN p.spend IS NOT NULL THEN 'appsflyer_mcp_scaled' ELSE 'appsflyer_mcp_raw' END AS primary_source,
  CASE WHEN p.spend IS NOT NULL THEN 'high' ELSE 'medium' END AS source_confidence,
  a.fetched_at
FROM mcp_adset a
LEFT JOIN mcp_adset_ms t
  ON t.fact_date = a.fact_date AND t.media_source = a.media_source
LEFT JOIN pull_ms p
  ON p.fact_date = a.fact_date AND p.media_source = a.media_source;
