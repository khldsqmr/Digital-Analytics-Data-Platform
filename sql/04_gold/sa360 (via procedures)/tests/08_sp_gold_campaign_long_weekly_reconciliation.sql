/*
===============================================================================
FILE: 08_sp_gold_campaign_long_weekly_reconciliation.sql (FIXED)
PROC: sp_gold_sa360_campaign_long_weekly_reconciliation_tests

RECON:
  Gold long weekly vs Gold wide weekly (qgp_week)

FIXES:
  - Proper UNNEST aliasing
  - Proper STRUCT field naming
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE sample_weeks INT64 DEFAULT 8;
  DECLARE tolerance   FLOAT64 DEFAULT 0.000001;

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

  wide_metric_rows AS (
    SELECT
      w.qgp_week,
      m.metric_name,
      m.metric_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly` w
    JOIN qgp_list q USING (qgp_week)
    CROSS JOIN UNNEST([
      STRUCT('impressions' AS metric_name, CAST(w.impressions AS FLOAT64) AS metric_value),
      STRUCT('clicks' AS metric_name, CAST(w.clicks AS FLOAT64) AS metric_value),
      STRUCT('cost' AS metric_name, CAST(w.cost AS FLOAT64) AS metric_value),
      STRUCT('all_conversions' AS metric_name, CAST(w.all_conversions AS FLOAT64) AS metric_value),

      STRUCT('bi' AS metric_name, CAST(w.bi AS FLOAT64) AS metric_value),
      STRUCT('buying_intent' AS metric_name, CAST(w.buying_intent AS FLOAT64) AS metric_value),
      STRUCT('bts_quality_traffic' AS metric_name, CAST(w.bts_quality_traffic AS FLOAT64) AS metric_value),
      STRUCT('digital_gross_add' AS metric_name, CAST(w.digital_gross_add AS FLOAT64) AS metric_value),
      STRUCT('magenta_pqt' AS metric_name, CAST(w.magenta_pqt AS FLOAT64) AS metric_value),

      STRUCT('cart_start' AS metric_name, CAST(w.cart_start AS FLOAT64) AS metric_value),
      STRUCT('postpaid_cart_start' AS metric_name, CAST(w.postpaid_cart_start AS FLOAT64) AS metric_value),
      STRUCT('postpaid_pspv' AS metric_name, CAST(w.postpaid_pspv AS FLOAT64) AS metric_value),
      STRUCT('aal' AS metric_name, CAST(w.aal AS FLOAT64) AS metric_value),
      STRUCT('add_a_line' AS metric_name, CAST(w.add_a_line AS FLOAT64) AS metric_value),

      STRUCT('connect_low_funnel_prospect' AS metric_name, CAST(w.connect_low_funnel_prospect AS FLOAT64) AS metric_value),
      STRUCT('connect_low_funnel_visit' AS metric_name, CAST(w.connect_low_funnel_visit AS FLOAT64) AS metric_value),
      STRUCT('connect_qt' AS metric_name, CAST(w.connect_qt AS FLOAT64) AS metric_value),

      STRUCT('hint_ec' AS metric_name, CAST(w.hint_ec AS FLOAT64) AS metric_value),
      STRUCT('hint_sec' AS metric_name, CAST(w.hint_sec AS FLOAT64) AS metric_value),
      STRUCT('hint_web_orders' AS metric_name, CAST(w.hint_web_orders AS FLOAT64) AS metric_value),
      STRUCT('hint_invoca_calls' AS metric_name, CAST(w.hint_invoca_calls AS FLOAT64) AS metric_value),
      STRUCT('hint_offline_invoca_calls' AS metric_name, CAST(w.hint_offline_invoca_calls AS FLOAT64) AS metric_value),
      STRUCT('hint_offline_invoca_eligibility' AS metric_name, CAST(w.hint_offline_invoca_eligibility AS FLOAT64) AS metric_value),
      STRUCT('hint_offline_invoca_order' AS metric_name, CAST(w.hint_offline_invoca_order AS FLOAT64) AS metric_value),
      STRUCT('hint_offline_invoca_order_rt' AS metric_name, CAST(w.hint_offline_invoca_order_rt AS FLOAT64) AS metric_value),
      STRUCT('hint_offline_invoca_sales_opp' AS metric_name, CAST(w.hint_offline_invoca_sales_opp AS FLOAT64) AS metric_value),
      STRUCT('ma_hint_ec_eligibility_check' AS metric_name, CAST(w.ma_hint_ec_eligibility_check AS FLOAT64) AS metric_value),

      STRUCT('fiber_activations' AS metric_name, CAST(w.fiber_activations AS FLOAT64) AS metric_value),
      STRUCT('fiber_pre_order' AS metric_name, CAST(w.fiber_pre_order AS FLOAT64) AS metric_value),
      STRUCT('fiber_waitlist_sign_up' AS metric_name, CAST(w.fiber_waitlist_sign_up AS FLOAT64) AS metric_value),
      STRUCT('fiber_web_orders' AS metric_name, CAST(w.fiber_web_orders AS FLOAT64) AS metric_value),
      STRUCT('fiber_ec' AS metric_name, CAST(w.fiber_ec AS FLOAT64) AS metric_value),
      STRUCT('fiber_ec_dda' AS metric_name, CAST(w.fiber_ec_dda AS FLOAT64) AS metric_value),
      STRUCT('fiber_sec' AS metric_name, CAST(w.fiber_sec AS FLOAT64) AS metric_value),
      STRUCT('fiber_sec_dda' AS metric_name, CAST(w.fiber_sec_dda AS FLOAT64) AS metric_value),

      STRUCT('metro_low_funnel_cs' AS metric_name, CAST(w.metro_low_funnel_cs AS FLOAT64) AS metric_value),
      STRUCT('metro_mid_funnel_prospect' AS metric_name, CAST(w.metro_mid_funnel_prospect AS FLOAT64) AS metric_value),
      STRUCT('metro_top_funnel_prospect' AS metric_name, CAST(w.metro_top_funnel_prospect AS FLOAT64) AS metric_value),
      STRUCT('metro_upper_funnel_prospect' AS metric_name, CAST(w.metro_upper_funnel_prospect AS FLOAT64) AS metric_value),
      STRUCT('metro_hint_qt' AS metric_name, CAST(w.metro_hint_qt AS FLOAT64) AS metric_value),
      STRUCT('metro_qt' AS metric_name, CAST(w.metro_qt AS FLOAT64) AS metric_value),

      STRUCT('tmo_prepaid_low_funnel_prospect' AS metric_name, CAST(w.tmo_prepaid_low_funnel_prospect AS FLOAT64) AS metric_value),
      STRUCT('tmo_top_funnel_prospect' AS metric_name, CAST(w.tmo_top_funnel_prospect AS FLOAT64) AS metric_value),
      STRUCT('tmo_upper_funnel_prospect' AS metric_name, CAST(w.tmo_upper_funnel_prospect AS FLOAT64) AS metric_value),

      STRUCT('tfb_low_funnel' AS metric_name, CAST(w.tfb_low_funnel AS FLOAT64) AS metric_value),
      STRUCT('tfb_lead_form_submit' AS metric_name, CAST(w.tfb_lead_form_submit AS FLOAT64) AS metric_value),
      STRUCT('tfb_invoca_sales_intent_dda' AS metric_name, CAST(w.tfb_invoca_sales_intent_dda AS FLOAT64) AS metric_value),
      STRUCT('tfb_invoca_order_dda' AS metric_name, CAST(w.tfb_invoca_order_dda AS FLOAT64) AS metric_value),
      STRUCT('tfb_credit_check' AS metric_name, CAST(w.tfb_credit_check AS FLOAT64) AS metric_value),
      STRUCT('tfb_hint_ec' AS metric_name, CAST(w.tfb_hint_ec AS FLOAT64) AS metric_value),
      STRUCT('tfb_invoca_sales_calls' AS metric_name, CAST(w.tfb_invoca_sales_calls AS FLOAT64) AS metric_value),
      STRUCT('tfb_leads' AS metric_name, CAST(w.tfb_leads AS FLOAT64) AS metric_value),
      STRUCT('tfb_quality_traffic' AS metric_name, CAST(w.tfb_quality_traffic AS FLOAT64) AS metric_value),
      STRUCT('total_tfb_conversions' AS metric_name, CAST(w.total_tfb_conversions AS FLOAT64) AS metric_value)
    ]) m
    WHERE m.metric_name IN UNNEST(metric_allowlist)
  ),

  wide_agg AS (
    SELECT
      qgp_week,
      metric_name,
      SUM(COALESCE(metric_value, 0)) AS wide_val,
      COUNT(1) AS wide_rows
    FROM wide_metric_rows
    GROUP BY 1,2
  ),

  long_agg AS (
    SELECT
      qgp_week,
      metric_name,
      SUM(COALESCE(metric_value, 0)) AS long_val,
      COUNT(1) AS long_rows
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long`
    WHERE qgp_week IN (SELECT qgp_week FROM qgp_list)
      AND metric_name IN UNNEST(metric_allowlist)
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
    CROSS JOIN UNNEST(metric_allowlist) AS metric_name
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
    CASE
      WHEN metric_name IN ('impressions','clicks','cost','all_conversions') THEN 'HIGH'
      ELSE 'MEDIUM'
    END,
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
      WHEN expected_rows = 0 THEN 'Gold wide weekly has 0 rows for this qgp_week+metric (unexpected).'
      WHEN actual_rows   = 0 THEN 'Gold long weekly has 0 rows for this qgp_week+metric.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'Gold long weekly matches Gold wide weekly.'
      ELSE 'Gold long weekly does NOT match Gold wide weekly.'
    END,

    CASE
      WHEN expected_rows = 0 THEN 'Check Gold weekly build coverage/window.'
      WHEN actual_rows   = 0 THEN 'Run/verify weekly_long merge; confirm metric_name mapping.'
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