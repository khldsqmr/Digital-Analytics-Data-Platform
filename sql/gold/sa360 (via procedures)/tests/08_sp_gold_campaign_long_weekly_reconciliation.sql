/*
===============================================================================
FILE: 08_sp_gold_campaign_long_weekly_reconciliation.sql  (UPDATED)
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests
RECON: Gold long weekly vs Gold wide weekly (qgp_week)

UPDATES:
  - FIX: qgp_cnt computed after sampled_qgp_weeks is set
  - Simplified: one reconciliation insert covers ALL metrics (vs repeating blocks)
  - Row-count aware FAIL (wide missing / long missing per qgp_week + metric)
  - Metric allowlist aligned to weekly_long UNPIVOT list
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE sample_weeks INT64 DEFAULT 4;
  DECLARE tolerance   FLOAT64 DEFAULT 0.000001;

  -- Must match weekly_long UNPIVOT list (keep in sync with merge proc)
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

  -- ---------------------------------------------------------------------------
  -- Sample recent qgp_weeks from WIDE weekly
  -- ---------------------------------------------------------------------------
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

  -- If no qgp_weeks available, write one FAIL row and exit
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

  -- ---------------------------------------------------------------------------
  -- Reconcile ALL metrics in one pass: wide weekly (authoritative) vs long weekly
  -- ---------------------------------------------------------------------------
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH qgp_list AS (
    SELECT qgp_week FROM UNNEST(sampled_qgp_weeks) AS qgp_week
  ),

  -- WIDE weekly -> (qgp_week, metric_name, metric_value)
  wide_metric_rows AS (
    SELECT
      w.qgp_week,
      m.metric_name,
      m.metric_value
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly` w
    JOIN qgp_list q USING (qgp_week)
    CROSS JOIN UNNEST([
      STRUCT('impressions' AS metric_name, CAST(w.impressions AS FLOAT64) AS metric_value),
      STRUCT('clicks', CAST(w.clicks AS FLOAT64)),
      STRUCT('cost', CAST(w.cost AS FLOAT64)),
      STRUCT('all_conversions', CAST(w.all_conversions AS FLOAT64)),

      STRUCT('bi', CAST(w.bi AS FLOAT64)),
      STRUCT('buying_intent', CAST(w.buying_intent AS FLOAT64)),
      STRUCT('bts_quality_traffic', CAST(w.bts_quality_traffic AS FLOAT64)),
      STRUCT('digital_gross_add', CAST(w.digital_gross_add AS FLOAT64)),
      STRUCT('magenta_pqt', CAST(w.magenta_pqt AS FLOAT64)),

      STRUCT('cart_start', CAST(w.cart_start AS FLOAT64)),
      STRUCT('postpaid_cart_start', CAST(w.postpaid_cart_start AS FLOAT64)),
      STRUCT('postpaid_pspv', CAST(w.postpaid_pspv AS FLOAT64)),
      STRUCT('aal', CAST(w.aal AS FLOAT64)),
      STRUCT('add_a_line', CAST(w.add_a_line AS FLOAT64)),

      STRUCT('connect_low_funnel_prospect', CAST(w.connect_low_funnel_prospect AS FLOAT64)),
      STRUCT('connect_low_funnel_visit', CAST(w.connect_low_funnel_visit AS FLOAT64)),
      STRUCT('connect_qt', CAST(w.connect_qt AS FLOAT64)),

      STRUCT('hint_ec', CAST(w.hint_ec AS FLOAT64)),
      STRUCT('hint_sec', CAST(w.hint_sec AS FLOAT64)),
      STRUCT('hint_web_orders', CAST(w.hint_web_orders AS FLOAT64)),
      STRUCT('hint_invoca_calls', CAST(w.hint_invoca_calls AS FLOAT64)),
      STRUCT('hint_offline_invoca_calls', CAST(w.hint_offline_invoca_calls AS FLOAT64)),
      STRUCT('hint_offline_invoca_eligibility', CAST(w.hint_offline_invoca_eligibility AS FLOAT64)),
      STRUCT('hint_offline_invoca_order', CAST(w.hint_offline_invoca_order AS FLOAT64)),
      STRUCT('hint_offline_invoca_order_rt', CAST(w.hint_offline_invoca_order_rt AS FLOAT64)),
      STRUCT('hint_offline_invoca_sales_opp', CAST(w.hint_offline_invoca_sales_opp AS FLOAT64)),
      STRUCT('ma_hint_ec_eligibility_check', CAST(w.ma_hint_ec_eligibility_check AS FLOAT64)),

      STRUCT('fiber_activations', CAST(w.fiber_activations AS FLOAT64)),
      STRUCT('fiber_pre_order', CAST(w.fiber_pre_order AS FLOAT64)),
      STRUCT('fiber_waitlist_sign_up', CAST(w.fiber_waitlist_sign_up AS FLOAT64)),
      STRUCT('fiber_web_orders', CAST(w.fiber_web_orders AS FLOAT64)),
      STRUCT('fiber_ec', CAST(w.fiber_ec AS FLOAT64)),
      STRUCT('fiber_ec_dda', CAST(w.fiber_ec_dda AS FLOAT64)),
      STRUCT('fiber_sec', CAST(w.fiber_sec AS FLOAT64)),
      STRUCT('fiber_sec_dda', CAST(w.fiber_sec_dda AS FLOAT64)),

      STRUCT('metro_low_funnel_cs', CAST(w.metro_low_funnel_cs AS FLOAT64)),
      STRUCT('metro_mid_funnel_prospect', CAST(w.metro_mid_funnel_prospect AS FLOAT64)),
      STRUCT('metro_top_funnel_prospect', CAST(w.metro_top_funnel_prospect AS FLOAT64)),
      STRUCT('metro_upper_funnel_prospect', CAST(w.metro_upper_funnel_prospect AS FLOAT64)),
      STRUCT('metro_hint_qt', CAST(w.metro_hint_qt AS FLOAT64)),
      STRUCT('metro_qt', CAST(w.metro_qt AS FLOAT64)),

      STRUCT('tmo_prepaid_low_funnel_prospect', CAST(w.tmo_prepaid_low_funnel_prospect AS FLOAT64)),
      STRUCT('tmo_top_funnel_prospect', CAST(w.tmo_top_funnel_prospect AS FLOAT64)),
      STRUCT('tmo_upper_funnel_prospect', CAST(w.tmo_upper_funnel_prospect AS FLOAT64)),

      STRUCT('tfb_low_funnel', CAST(w.tfb_low_funnel AS FLOAT64)),
      STRUCT('tfb_lead_form_submit', CAST(w.tfb_lead_form_submit AS FLOAT64)),
      STRUCT('tfb_invoca_sales_intent_dda', CAST(w.tfb_invoca_sales_intent_dda AS FLOAT64)),
      STRUCT('tfb_invoca_order_dda', CAST(w.tfb_invoca_order_dda AS FLOAT64)),
      STRUCT('tfb_credit_check', CAST(w.tfb_credit_check AS FLOAT64)),
      STRUCT('tfb_hint_ec', CAST(w.tfb_hint_ec AS FLOAT64)),
      STRUCT('tfb_invoca_sales_calls', CAST(w.tfb_invoca_sales_calls AS FLOAT64)),
      STRUCT('tfb_leads', CAST(w.tfb_leads AS FLOAT64)),
      STRUCT('tfb_quality_traffic', CAST(w.tfb_quality_traffic AS FLOAT64)),
      STRUCT('total_tfb_conversions', CAST(w.total_tfb_conversions AS FLOAT64))
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
      w.wide_val AS expected_value,
      l.long_val AS actual_value,
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
    CONCAT(
      'Weekly Long == Weekly Wide | metric=', metric_name,
      ' | qgp_week=', CAST(qgp_week AS STRING)
    ),
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
      WHEN actual_rows   = 0 THEN 'Run/verify long weekly merge coverage/window; confirm metric_name mapping.'
      WHEN ABS(actual_value - expected_value) <= tolerance THEN 'No action required.'
      ELSE 'Fix long weekly unpivot/merge logic; verify duplicates + metric mapping.'
    END,

    -- critical failure: missing either side OR mismatch beyond tolerance
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
