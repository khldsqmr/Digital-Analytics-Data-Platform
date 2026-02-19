/*
===============================================================================
FILE: 08_sp_gold_campaign_long_weekly_reconciliation.sql
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests
RECON: Gold long weekly vs Gold wide weekly (qgp_week)

FIXES:
  - Removed TEMP table usage (not supported reliably inside BQ procedures)
  - Uses sampled_qgp_weeks ARRAY<DATE> for scan efficiency
  - Row-count aware FAIL (wide missing / long missing per qgp_week)
  - Correct is_critical_failure logic (no constant TRUE)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE sample_weeks INT64 DEFAULT 4;
  DECLARE tolerance   FLOAT64 DEFAULT 0.000001;

  -- Sample the most recent qgp_weeks from WIDE weekly once (efficient)
  DECLARE sampled_qgp_weeks ARRAY<DATE>;

  SET sampled_qgp_weeks = (
    SELECT ARRAY_AGG(qgp_week ORDER BY qgp_week DESC LIMIT sample_weeks)
    FROM (
      SELECT DISTINCT qgp_week
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
      WHERE qgp_week IS NOT NULL
    )
  );

  DECLARE qgp_cnt INT64 DEFAULT ARRAY_LENGTH(sampled_qgp_weeks);

  -- If no qgp_weeks available, write one FAIL row and exit
  IF qgp_cnt = 0 THEN
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

  -- ===========================================================================
  -- TEST 1: impressions
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week FROM UNNEST(sampled_qgp_weeks) AS qgp_week
  ),
  wide AS (
    SELECT
      qgp_week,
      SUM(COALESCE(impressions,0)) AS wide_val,
      COUNT(1) AS wide_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  long AS (
    SELECT
      qgp_week,
      SUM(COALESCE(metric_value,0)) AS long_val,
      COUNT(1) AS long_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
      AND metric_name = 'impressions'
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.qgp_week,
      w.wide_val AS expected_value,
      g.long_val AS actual_value,
      COALESCE(w.wide_rows, 0) AS expected_rows,
      COALESCE(g.long_rows, 0) AS actual_rows
    FROM qgp_list l
    LEFT JOIN wide w USING (qgp_week)
    LEFT JOIN long g USING (qgp_week)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'reconciliation',
    CONCAT('Impressions Weekly Reconciliation (long vs wide) | qgp_week=', CAST(qgp_week AS STRING)),
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
      WHEN expected_rows = 0 THEN 'Gold wide weekly has 0 rows for this qgp_week (unexpected).'
      WHEN actual_rows   = 0 THEN 'Gold long weekly has 0 rows for metric=impressions for this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END,
    CASE
      WHEN expected_rows = 0 THEN 'Check Gold weekly build coverage/window.'
      WHEN actual_rows   = 0 THEN 'Check Gold long weekly merge coverage/window; verify metric_name mapping.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix long weekly unpivot/merge logic; verify duplicates + metric_name mapping.'
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

  -- ===========================================================================
  -- TEST 2: clicks
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week FROM UNNEST(sampled_qgp_weeks) AS qgp_week
  ),
  wide AS (
    SELECT
      qgp_week,
      SUM(COALESCE(clicks,0)) AS wide_val,
      COUNT(1) AS wide_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  long AS (
    SELECT
      qgp_week,
      SUM(COALESCE(metric_value,0)) AS long_val,
      COUNT(1) AS long_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
      AND metric_name = 'clicks'
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.qgp_week,
      w.wide_val AS expected_value,
      g.long_val AS actual_value,
      COALESCE(w.wide_rows, 0) AS expected_rows,
      COALESCE(g.long_rows, 0) AS actual_rows
    FROM qgp_list l
    LEFT JOIN wide w USING (qgp_week)
    LEFT JOIN long g USING (qgp_week)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'reconciliation',
    CONCAT('Clicks Weekly Reconciliation (long vs wide) | qgp_week=', CAST(qgp_week AS STRING)),
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
      WHEN expected_rows = 0 THEN 'Gold wide weekly has 0 rows for this qgp_week (unexpected).'
      WHEN actual_rows   = 0 THEN 'Gold long weekly has 0 rows for metric=clicks for this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END,
    CASE
      WHEN expected_rows = 0 THEN 'Check Gold weekly build coverage/window.'
      WHEN actual_rows   = 0 THEN 'Check Gold long weekly merge coverage/window; verify metric_name mapping.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix long weekly unpivot/merge logic; verify duplicates + metric_name mapping.'
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

  -- ===========================================================================
  -- TEST 3: cost
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week FROM UNNEST(sampled_qgp_weeks) AS qgp_week
  ),
  wide AS (
    SELECT
      qgp_week,
      SUM(COALESCE(cost,0)) AS wide_val,
      COUNT(1) AS wide_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  long AS (
    SELECT
      qgp_week,
      SUM(COALESCE(metric_value,0)) AS long_val,
      COUNT(1) AS long_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
      AND metric_name = 'cost'
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.qgp_week,
      w.wide_val AS expected_value,
      g.long_val AS actual_value,
      COALESCE(w.wide_rows, 0) AS expected_rows,
      COALESCE(g.long_rows, 0) AS actual_rows
    FROM qgp_list l
    LEFT JOIN wide w USING (qgp_week)
    LEFT JOIN long g USING (qgp_week)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'reconciliation',
    CONCAT('Cost Weekly Reconciliation (long vs wide) | qgp_week=', CAST(qgp_week AS STRING)),
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
      WHEN expected_rows = 0 THEN 'Gold wide weekly has 0 rows for this qgp_week (unexpected).'
      WHEN actual_rows   = 0 THEN 'Gold long weekly has 0 rows for metric=cost for this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END,
    CASE
      WHEN expected_rows = 0 THEN 'Check Gold weekly build coverage/window.'
      WHEN actual_rows   = 0 THEN 'Check Gold long weekly merge coverage/window; verify metric_name mapping.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix long weekly unpivot/merge logic; verify duplicates + metric_name mapping.'
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

  -- ===========================================================================
  -- TEST 4: all_conversions
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week FROM UNNEST(sampled_qgp_weeks) AS qgp_week
  ),
  wide AS (
    SELECT
      qgp_week,
      SUM(COALESCE(all_conversions,0)) AS wide_val,
      COUNT(1) AS wide_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  long AS (
    SELECT
      qgp_week,
      SUM(COALESCE(metric_value,0)) AS long_val,
      COUNT(1) AS long_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
      AND metric_name = 'all_conversions'
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.qgp_week,
      w.wide_val AS expected_value,
      g.long_val AS actual_value,
      COALESCE(w.wide_rows, 0) AS expected_rows,
      COALESCE(g.long_rows, 0) AS actual_rows
    FROM qgp_list l
    LEFT JOIN wide w USING (qgp_week)
    LEFT JOIN long g USING (qgp_week)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'reconciliation',
    CONCAT('All Conversions Weekly Reconciliation (long vs wide) | qgp_week=', CAST(qgp_week AS STRING)),
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
      WHEN expected_rows = 0 THEN 'Gold wide weekly has 0 rows for this qgp_week (unexpected).'
      WHEN actual_rows   = 0 THEN 'Gold long weekly has 0 rows for metric=all_conversions for this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END,
    CASE
      WHEN expected_rows = 0 THEN 'Check Gold weekly build coverage/window.'
      WHEN actual_rows   = 0 THEN 'Check Gold long weekly merge coverage/window; verify metric_name mapping.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix long weekly unpivot/merge logic; verify duplicates + metric_name mapping.'
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

  -- ===========================================================================
  -- TEST 5: cart_start
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week FROM UNNEST(sampled_qgp_weeks) AS qgp_week
  ),
  wide AS (
    SELECT
      qgp_week,
      SUM(COALESCE(cart_start,0)) AS wide_val,
      COUNT(1) AS wide_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  long AS (
    SELECT
      qgp_week,
      SUM(COALESCE(metric_value,0)) AS long_val,
      COUNT(1) AS long_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
      AND metric_name = 'cart_start'
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.qgp_week,
      w.wide_val AS expected_value,
      g.long_val AS actual_value,
      COALESCE(w.wide_rows, 0) AS expected_rows,
      COALESCE(g.long_rows, 0) AS actual_rows
    FROM qgp_list l
    LEFT JOIN wide w USING (qgp_week)
    LEFT JOIN long g USING (qgp_week)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'reconciliation',
    CONCAT('Cart Start Weekly Reconciliation (long vs wide) | qgp_week=', CAST(qgp_week AS STRING)),
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
      WHEN expected_rows = 0 THEN 'Gold wide weekly has 0 rows for this qgp_week (unexpected).'
      WHEN actual_rows   = 0 THEN 'Gold long weekly has 0 rows for metric=cart_start for this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END,
    CASE
      WHEN expected_rows = 0 THEN 'Check Gold weekly build coverage/window.'
      WHEN actual_rows   = 0 THEN 'Check Gold long weekly merge coverage/window; verify metric_name mapping.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix long weekly unpivot/merge logic; verify duplicates + metric_name mapping.'
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

  -- ===========================================================================
  -- TEST 6: postpaid_pspv
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week FROM UNNEST(sampled_qgp_weeks) AS qgp_week
  ),
  wide AS (
    SELECT
      qgp_week,
      SUM(COALESCE(postpaid_pspv,0)) AS wide_val,
      COUNT(1) AS wide_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
    GROUP BY 1
  ),
  long AS (
    SELECT
      qgp_week,
      SUM(COALESCE(metric_value,0)) AS long_val,
      COUNT(1) AS long_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
      AND metric_name = 'postpaid_pspv'
    GROUP BY 1
  ),
  aligned AS (
    SELECT
      l.qgp_week,
      w.wide_val AS expected_value,
      g.long_val AS actual_value,
      COALESCE(w.wide_rows, 0) AS expected_rows,
      COALESCE(g.long_rows, 0) AS actual_rows
    FROM qgp_list l
    LEFT JOIN wide w USING (qgp_week)
    LEFT JOIN long g USING (qgp_week)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'reconciliation',
    CONCAT('Postpaid PSPV Weekly Reconciliation (long vs wide) | qgp_week=', CAST(qgp_week AS STRING)),
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
      WHEN expected_rows = 0 THEN 'Gold wide weekly has 0 rows for this qgp_week (unexpected).'
      WHEN actual_rows   = 0 THEN 'Gold long weekly has 0 rows for metric=postpaid_pspv for this qgp_week.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END,
    CASE
      WHEN expected_rows = 0 THEN 'Check Gold weekly build coverage/window.'
      WHEN actual_rows   = 0 THEN 'Check Gold long weekly merge coverage/window; verify metric_name mapping.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix long weekly unpivot/merge logic; verify duplicates + metric_name mapping.'
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
