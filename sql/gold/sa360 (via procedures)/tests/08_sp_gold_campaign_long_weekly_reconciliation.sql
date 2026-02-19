/*
===============================================================================
FILE: 08_sp_gold_campaign_long_weekly_reconciliation.sql
PROC:  sp_gold_sa360_campaign_long_weekly_reconciliation_tests
RECON: Gold long weekly vs Gold wide weekly (qgp_week)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE sample_weeks INT64 DEFAULT 4;
  DECLARE tolerance   FLOAT64 DEFAULT 0.000001;

  -- ---------------------------------------------------------------------------
  -- TEST 1: impressions (Gold long weekly vs Gold wide weekly) per qgp_week
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week
    FROM (
      SELECT DISTINCT qgp_week
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
      WHERE qgp_week IS NOT NULL
    )
    QUALIFY ROW_NUMBER() OVER (ORDER BY qgp_week DESC) <= sample_weeks
  ),
  wide AS (
    SELECT
      qgp_week,
      SUM(COALESCE(impressions, 0)) AS impressions_wide
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week IS NOT NULL
    GROUP BY 1
  ),
  long AS (
    SELECT
      qgp_week,
      SUM(COALESCE(metric_value, 0)) AS impressions_long
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IS NOT NULL
      AND metric_name = 'impressions'
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.qgp_week,
      w.impressions_wide AS expected_value,
      g.impressions_long AS actual_value
    FROM qgp_list l
    LEFT JOIN wide w USING (qgp_week)
    LEFT JOIN long g USING (qgp_week)
  )
  SELECT
    CURRENT_TIMESTAMP() AS test_run_timestamp,
    CURRENT_DATE()      AS test_date,
    'sdi_gold_sa360_campaign_weekly_long' AS table_name,
    'reconciliation'    AS test_layer,
    CONCAT(
      'Impressions Weekly Reconciliation (Gold long vs Gold wide) | qgp_week=',
      CAST(qgp_week AS STRING)
    ) AS test_name,
    'HIGH' AS severity_level,
    CAST(expected_value AS FLOAT64) AS expected_value,
    CAST(actual_value   AS FLOAT64) AS actual_value,
    CAST(actual_value - expected_value AS FLOAT64) AS variance_value,
    CASE
      WHEN expected_value IS NULL THEN 'FAIL'
      WHEN actual_value   IS NULL THEN 'FAIL'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'PASS'
      ELSE 'FAIL'
    END AS status,
    CASE
      WHEN expected_value IS NULL THEN '🔴'
      WHEN actual_value   IS NULL THEN '🔴'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN '🟢'
      ELSE '🔴'
    END AS status_emoji,
    CASE
      WHEN expected_value IS NULL THEN 'Gold wide weekly missing for this qgp_week (unexpected).'
      WHEN actual_value   IS NULL THEN 'Gold long weekly missing for this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END AS failure_reason,
    CASE
      WHEN expected_value IS NULL THEN 'Check Gold weekly build coverage.'
      WHEN actual_value   IS NULL THEN 'Check Gold long weekly build/unpivot coverage.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix long weekly unpivot logic; check duplicates and metric_name mapping.'
    END AS next_step,
    CASE
      WHEN expected_value IS NULL THEN TRUE
      WHEN actual_value   IS NULL THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END AS is_critical_failure,
    CASE
      WHEN expected_value IS NOT NULL
       AND actual_value IS NOT NULL
       AND ABS(actual_value - expected_value) <= tolerance THEN TRUE
      ELSE FALSE
    END AS is_pass,
    CASE
      WHEN expected_value IS NULL THEN TRUE
      WHEN actual_value   IS NULL THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END AS is_fail
  FROM aligned;

END;
