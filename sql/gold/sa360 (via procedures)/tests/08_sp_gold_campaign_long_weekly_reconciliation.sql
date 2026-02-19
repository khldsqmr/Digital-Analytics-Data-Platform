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
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

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
      SUM(COALESCE(impressions,0)) AS impressions_wide
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    GROUP BY 1
  ),

  long AS (
    SELECT
      qgp_week,
      SUM(COALESCE(metric_value,0)) AS impressions_long
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE metric_name = 'impressions'
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

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  SELECT
    CURRENT_TIMESTAMP(),
    CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'reconciliation',
    CONCAT('Impressions Weekly Reconciliation (Gold long vs Gold wide) | qgp_week=', CAST(qgp_week AS STRING)),
    'HIGH',
    CAST(expected_value AS FLOAT64),
    CAST(actual_value AS FLOAT64),
    CAST(actual_value - expected_value AS FLOAT64),
    CASE
      WHEN expected_value IS NULL THEN 'FAIL'
      WHEN actual_value IS NULL THEN 'FAIL'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'PASS'
      ELSE 'FAIL'
    END,
    CASE
      WHEN expected_value IS NULL THEN '🔴'
      WHEN actual_value IS NULL THEN '🔴'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN '🟢'
      ELSE '🔴'
    END,
    CASE
      WHEN expected_value IS NULL THEN 'Gold wide weekly missing for this qgp_week (unexpected).'
      WHEN actual_value IS NULL THEN 'Gold long weekly missing for this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END,
    CASE
      WHEN expected_value IS NULL THEN 'Check Gold weekly build coverage.'
      WHEN actual_value IS NULL THEN 'Check Gold long weekly build/unpivot coverage.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix long weekly build/unpivot logic; ensure no duplicate metric rows.'
    END,
    CASE
      WHEN expected_value IS NULL THEN TRUE
      WHEN actual_value IS NULL THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END,
    CASE
      WHEN expected_value IS NOT NULL
       AND actual_value IS NOT NULL
       AND ABS(actual_value - expected_value) <= tolerance THEN TRUE
      ELSE FALSE
    END,
    CASE
      WHEN expected_value IS NULL THEN TRUE
      WHEN actual_value IS NULL THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END
  FROM aligned;

END;
