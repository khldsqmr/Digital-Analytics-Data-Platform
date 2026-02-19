/*
===============================================================================
FILE: 07_sp_gold_campaign_long_weekly_critical.sql
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_critical_tests
TABLE: sdi_gold_sa360_campaign_weekly_long
GRAIN: (account_id, campaign_id, qgp_week, metric_name)

UPDATES:
  - Added QGP week validity test (Saturday OR quarter-end)
  - Added metric_name allowlist test (aligned to UNPIVOT list)
  - Added coverage test (wide weekly recent qgp_weeks must exist in long weekly)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_critical_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_weeks INT64 DEFAULT 12;

  DECLARE cutoff_anchor DATE DEFAULT DATE_TRUNC(
    DATE_SUB(CURRENT_DATE(), INTERVAL lookback_weeks WEEK),
    WEEK(SATURDAY)
  );

  -- Must match your weekly_long UNPIVOT list (keep in sync with merge proc)
  DECLARE metric_allowlist ARRAY<STRING> DEFAULT [
    'impressions','clicks','cost','all_conversions',
    'bi','buying_intent','bts_quality_traffic','digital_gross_add','magenta_pqt',
    'cart_start','postpaid_cart_start','postpaid_pspv','aal','add_a_line',
    'connect_low_funnel_prospect','connect_low_funnel_visit','connect_qt',
    'hint_ec','hint_sec','hint_web_orders','hint_invoca_calls','hint_offline_invoca_calls',
    'hint_offline_invoca_eligibility','hint_offline_invoca_order','hint_offline_invoca_order_rt',
    'hint_offline_invoca_sales_opp','ma_hint_ec_eligibility_check',
    'fiber_activations','fiber_pre_order','fiber_waitlist_sign_up','fiber_web_orders',
    'fiber_ec','fiber_ec_dda','fiber_sec','fiber_sec_dda',
    'metro_low_funnel_cs','metro_mid_funnel_prospect','metro_top_funnel_prospect',
    'metro_upper_funnel_prospect','metro_hint_qt','metro_qt',
    'tmo_prepaid_low_funnel_prospect','tmo_top_funnel_prospect','tmo_upper_funnel_prospect',
    'tfb_low_funnel','tfb_lead_form_submit','tfb_invoca_sales_intent_dda','tfb_invoca_order_dda',
    'tfb_credit_check','tfb_hint_ec','tfb_invoca_sales_calls','tfb_leads','tfb_quality_traffic',
    'total_tfb_conversions'
  ];

  -- ===========================================================================
  -- TEST 1: Duplicate grain
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH dup AS (
    SELECT COUNT(1) AS duplicate_groups
    FROM (
      SELECT account_id, campaign_id, qgp_week, metric_name, COUNT(*) c
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
      WHERE qgp_week >= cutoff_anchor
      GROUP BY 1,2,3,4
      HAVING COUNT(*) > 1
    )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'Duplicate Grain Check (acct,campaign,qgp_week,metric_name)',
    'HIGH',
    0.0,
    CAST(duplicate_groups AS FLOAT64),
    CAST(duplicate_groups AS FLOAT64),
    IF(duplicate_groups = 0, 'PASS', 'FAIL'),
    IF(duplicate_groups = 0, '🟢', '🔴'),
    IF(duplicate_groups = 0, 'No duplicate long-weekly grain detected.',
       'Duplicate keys found in Gold long weekly.'),
    IF(duplicate_groups = 0, 'No action required.',
       'Fix long weekly build/merge; ensure uniqueness on merge key.'),
    IF(duplicate_groups > 0, TRUE, FALSE),
    IF(duplicate_groups = 0, TRUE, FALSE),
    IF(duplicate_groups > 0, TRUE, FALSE)
  FROM dup;

  -- ===========================================================================
  -- TEST 2: Null identifiers
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week >= cutoff_anchor
      AND (
        account_id IS NULL OR
        campaign_id IS NULL OR
        qgp_week IS NULL OR
        metric_name IS NULL
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'Null Identifier Check (acct,campaign,qgp_week,metric_name)',
    'HIGH',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0, 'All long-weekly identifiers are valid.',
       'Null identifier(s) found in Gold long weekly.'),
    IF(bad_rows = 0, 'No action required.',
       'Fix upstream mapping/build; identifiers must be populated.'),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

  -- ===========================================================================
  -- TEST 3: QGP week validity (Saturday OR quarter-end)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH mis AS (
    SELECT COUNT(1) AS misaligned_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week >= cutoff_anchor
      AND (
        qgp_week != DATE_TRUNC(qgp_week, WEEK(SATURDAY))
        AND qgp_week != DATE_SUB(
          DATE_ADD(DATE_TRUNC(qgp_week, QUARTER), INTERVAL 3 MONTH),
          INTERVAL 1 DAY
        )
      )
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'QGP Week Validity (Saturday OR quarter-end) - long weekly',
    'HIGH',
    0.0,
    CAST(misaligned_rows AS FLOAT64),
    CAST(misaligned_rows AS FLOAT64),
    IF(misaligned_rows = 0, 'PASS', 'FAIL'),
    IF(misaligned_rows = 0, '🟢', '🔴'),
    IF(misaligned_rows = 0,
      'All qgp_week values are valid (Saturday-aligned or quarter-end).',
      'One or more qgp_week values are neither Saturday nor quarter-end.'
    ),
    IF(misaligned_rows = 0,
      'No action required.',
      'Fix qgp_week derivation upstream (wide weekly) and/or long merge filters.'
    ),
    IF(misaligned_rows > 0, TRUE, FALSE),
    IF(misaligned_rows = 0, TRUE, FALSE),
    IF(misaligned_rows > 0, TRUE, FALSE)
  FROM mis;

  -- ===========================================================================
  -- TEST 4: metric_name allowlist (keep in sync with UNPIVOT list)
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH bad AS (
    SELECT COUNT(1) AS bad_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week >= cutoff_anchor
      AND metric_name NOT IN UNNEST(metric_allowlist)
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'Metric Name Allowlist Check (long weekly)',
    'MEDIUM',
    0.0,
    CAST(bad_rows AS FLOAT64),
    CAST(bad_rows AS FLOAT64),
    IF(bad_rows = 0, 'PASS', 'FAIL'),
    IF(bad_rows = 0, '🟢', '🔴'),
    IF(bad_rows = 0, 'All metric_name values are expected.',
       'Unexpected metric_name values found (drift / naming mismatch).'),
    IF(bad_rows = 0, 'No action required.',
       'Update allowlist or fix unpivot metric naming in merge procedure.'),
    IF(bad_rows > 0, TRUE, FALSE),
    IF(bad_rows = 0, TRUE, FALSE),
    IF(bad_rows > 0, TRUE, FALSE)
  FROM bad;

  -- ===========================================================================
  -- TEST 5: Coverage (wide weekly recent qgp_week must exist in long weekly)
  --   - Last N qgp_weeks from wide weekly should have at least 1 row in long
  -- ===========================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week
    FROM (
      SELECT DISTINCT qgp_week
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
      WHERE qgp_week IS NOT NULL
        AND qgp_week >= cutoff_anchor
    )
    QUALIFY ROW_NUMBER() OVER (ORDER BY qgp_week DESC) <= 4
  ),
  long_present AS (
    SELECT DISTINCT qgp_week
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
  ),
  missing AS (
    SELECT l.qgp_week
    FROM qgp_list l
    LEFT JOIN long_present p USING (qgp_week)
    WHERE p.qgp_week IS NULL
  )
  SELECT
    CURRENT_TIMESTAMP(), CURRENT_DATE(),
    'sdi_gold_sa360_campaign_weekly_long',
    'critical',
    'Coverage Check (wide weekly qgp_week exists, long weekly missing)',
    'HIGH',
    0.0,
    CAST(COUNT(*) AS FLOAT64),
    CAST(COUNT(*) AS FLOAT64),
    IF(COUNT(*) = 0, 'PASS', 'FAIL'),
    IF(COUNT(*) = 0, '🟢', '🔴'),
    IF(COUNT(*) = 0,
      'All recent qgp_weeks from wide weekly exist in long weekly.',
      CONCAT('Long weekly missing qgp_week example: ', CAST(ANY_VALUE(qgp_week) AS STRING))
    ),
    IF(COUNT(*) = 0,
      'No action required.',
      'Run/verify long weekly merge job; check lookback window and filters.'
    ),
    IF(COUNT(*) > 0, TRUE, FALSE),
    IF(COUNT(*) = 0, TRUE, FALSE),
    IF(COUNT(*) > 0, TRUE, FALSE)
  FROM missing;

END;
