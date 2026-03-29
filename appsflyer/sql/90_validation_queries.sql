-- Freshness / volume
SELECT report_date, SUM(cost) AS cost, SUM(installs) AS installs, SUM(af_start_trial) AS af_start_trial
FROM mart_marketing.daily_acquisition
GROUP BY 1
ORDER BY 1 DESC;

-- Duplicate natural keys in daily mart
SELECT
  report_date, app_key, platform, media_source, campaign_id, adset_id, ad_id, country,
  COUNT(*) AS row_count
FROM mart_marketing.daily_acquisition
GROUP BY 1,2,3,4,5,6,7,8
HAVING COUNT(*) > 1
ORDER BY row_count DESC;

-- Top campaigns by spend
SELECT report_date, media_source, campaign, SUM(cost) AS spend, SUM(af_start_trial) AS start_trials
FROM mart_marketing.daily_acquisition
GROUP BY 1,2,3
ORDER BY spend DESC
LIMIT 50;
