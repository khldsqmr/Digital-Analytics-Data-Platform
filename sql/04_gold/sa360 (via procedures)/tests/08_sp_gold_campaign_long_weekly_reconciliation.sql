/*
===============================================================================
FILE: 08_sp_gold_campaign_long_weekly_reconciliation.sql
PROC: sp_gold_sa360_campaign_long_weekly_reconciliation_tests

RECON (basic data flow):
  Gold long weekly vs Gold wide weekly for ONLY:
    - cart_start
    - postpaid_pspv
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE sample_weeks INT64 DEFAULT 8;
  DECLARE tolerance   FLOAT64 DEFAULT 0.000001;

  DECLARE metric_focus ARRAY<STRING> DEFAULT ['cart_start','postpaid_pspv'];

  DECLARE sampled_qgp_weeks ARRAY<DATE>;
  DECLARE qgp_cnt INT64;

  SET sampled_qgp_weeks = (
    SELECT ARRAY_AGG(qgp_week ORDER BY qgp_week DESC)
    FROM (
      SELECT qgp_week
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
      WHERE qgp_week IS NOT NULL
      GROUP BY qgp_week
      QUALIFY ROW_NUMBER() OVER (ORDER BY qgp_week DESC) <= sample_weeks
    )
  );

  SET qgp_cnt = ARRAY_LENGTH(sampled_qgp_weeks);

  IF qgp_cnt IS NULL OR qgp_cnt = 0 THEN
    INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
    SELECT
      CURRENT_TIMESTAMP(), CURRENT_DATE(),
      'sdi_gold_sa360_campaign_weekly_long',
      'reconciliation',
      'Weekly Long vs Wide Reconciliation (no qgp_week available)',
      'HIGH',
      0.0, 0.0, 0.0,
      'FAIL',
      '🔴',
      'Gold wide weekly has no qgp_week values to sample (cannot run reconciliation).',
      'Check Gold weekly build/backfill; ensure qgp_week is populated.',
      TRUE, FALSE, TRUE;
    RETURN;
  END IF;

  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week FROM UNNEST(sampled_qgp_weeks) AS qgp_week
  ),
  wide_agg AS (
    SELECT
      w.qgp_week,
      m.metric_name,
      SUM(COALESCE(m.metric_value,0)) AS wide_val,
      COUNT(1) AS wide_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly` w
    JOIN qgp_list q USING (qgp_week)
    CROSS JOIN UNNEST([
      STRUCT('cart_start'    AS metric_name, CAST(w.cart_start    AS FLOAT64) AS metric_value),
      STRUCT('postpaid_pspv' AS metric_name, CAST(w.postpaid_pspv AS FLOAT64) AS metric_value)
    ]) m
    WHERE m.metric_name IN UNNEST(metric_focus)
    GROUP BY 1,2
  ),
  long_agg AS (
    SELECT
      qgp_week,
      metric_name,
      SUM(COALESCE(metric_value,0)) AS long_val,
      COUNT(1) AS long_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
      AND metric_name IN UNNEST(metric_focus)
    GROUP BY 1,2
  ),
  aligned AS (
    SELECT
      q.qgp_week,
      metric_name,
      COALESCE(w.wide_val, 0) AS expected_value,
      COALESCE(l.long_val, 0) AS actual_value,
      COALESCE(w.wide_rows, 0) AS expected_rows,
      COALESCE(l.long_rows, 0) AS actual_rows
    FROM qgp_list q
    CROSS JOIN UNNEST(metric_focus) AS metric_name
    LEFT JOIN wide_agg w
      ON w.qgp_week = q.qgp_week AND w.metric_name = metric_name
    LEFT JOIN long_agg l
      ON l.qgp_week = q.qgp_week AND l.metric_name = metric_name
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'reconciliation',
    CONCAT('Weekly Long == Weekly Wide | metric=', metric_name, ' | qgp_week=', CAST(qgp_week AS STRING)),
    'HIGH',
    CAST(expected_value AS FLOAT64),
    CAST(actual_value   AS FLOAT64),
    CAST(actual_value - expected_value AS FLOAT64),

    CASE
      WHEN expected_rows = 0 THEN 'FAIL'
      WHEN actual_rows   = 0 THEN 'FAIL'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'PASS'
      ELSE 'FAIL'
    END,

    CASE
      WHEN expected_rows = 0 THEN '🔴'
      WHEN actual_rows   = 0 THEN '🔴'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN '🟢'
      ELSE '🔴'
    END,

    CASE
      WHEN expected_rows = 0 THEN 'Gold wide weekly missing this metric/qgp_week (unexpected).'
      WHEN actual_rows   = 0 THEN 'Gold long weekly missing this metric/qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END,

    CASE
      WHEN expected_rows = 0 THEN 'Check Gold weekly build/backfill for this qgp_week.'
      WHEN actual_rows   = 0 THEN 'Run/verify weekly_long build; confirm unpivot for this metric.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix weekly_long unpivot/merge logic; verify duplicates + mapping.'
    END,

    CASE
      WHEN expected_rows = 0 THEN TRUE
      WHEN actual_rows   = 0 THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END,

    CASE
      WHEN expected_rows > 0 AND actual_rows > 0 AND ABS(actual_value - expected_value) <= tolerance THEN TRUE
      ELSE FALSE
    END,

    CASE
      WHEN expected_rows = 0 THEN TRUE
      WHEN actual_rows   = 0 THEN TRUE
      WHEN ABS(actual_value - expected_value) > tolerance THEN TRUE
      ELSE FALSE
    END
  FROM aligned;

END;