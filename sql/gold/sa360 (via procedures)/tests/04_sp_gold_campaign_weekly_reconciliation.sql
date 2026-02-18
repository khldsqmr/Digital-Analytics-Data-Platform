/*
===============================================================================
FILE: 04_sp_gold_campaign_weekly_reconciliation.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests

RECONCILIATION:
  Gold Weekly must equal SUM(Gold Daily) for same weekend_date window.
  This ensures week rollups are mathematically correct.

METRICS:
  - cart_start
  - postpaid_pspv
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_weeks INT64 DEFAULT 12;
  DECLARE tolerance FLOAT64 DEFAULT 0.000001;

  -- cart_start reconcile (weekly vs sum(daily))
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH weekly_sum AS (
    SELECT
      SUM(COALESCE(cart_start,0)) AS weekly_total
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
  ),
  daily_sum AS (
    SELECT
      SUM(COALESCE(cart_start,0)) AS daily_total
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE DATE_TRUNC(date, WEEK(SATURDAY)) >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'reconciliation',
    'Cart Start Reconciliation (Weekly vs SUM(Daily), 12w)',
    'HIGH',
    (SELECT daily_total FROM daily_sum),
    (SELECT weekly_total FROM weekly_sum),
    (SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum),
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) <= tolerance, '🟢', '🔴'),
    'Weekly should equal SUM(Daily) for the same week window.',
    'If FAIL: verify weekend_date logic (WEEK(SATURDAY)) + ensure weekly built only from Gold Daily.',
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) > tolerance, TRUE, FALSE),
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) <= tolerance, TRUE, FALSE),
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) > tolerance, TRUE, FALSE);

  -- postpaid_pspv reconcile (weekly vs sum(daily))
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH weekly_sum AS (
    SELECT
      SUM(COALESCE(postpaid_pspv,0)) AS weekly_total
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE weekend_date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
  ),
  daily_sum AS (
    SELECT
      SUM(COALESCE(postpaid_pspv,0)) AS daily_total
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE DATE_TRUNC(date, WEEK(SATURDAY)) >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly',
    'reconciliation',
    'Postpaid PSPV Reconciliation (Weekly vs SUM(Daily), 12w)',
    'HIGH',
    (SELECT daily_total FROM daily_sum),
    (SELECT weekly_total FROM weekly_sum),
    (SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum),
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) <= tolerance, 'PASS', 'FAIL'),
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) <= tolerance, '🟢', '🔴'),
    'Weekly should equal SUM(Daily) for the same week window.',
    'If FAIL: verify weekend_date logic (WEEK(SATURDAY)) + ensure weekly built only from Gold Daily.',
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) > tolerance, TRUE, FALSE),
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) <= tolerance, TRUE, FALSE),
    IF(ABS((SELECT weekly_total FROM weekly_sum) - (SELECT daily_total FROM daily_sum)) > tolerance, TRUE, FALSE);

END;
