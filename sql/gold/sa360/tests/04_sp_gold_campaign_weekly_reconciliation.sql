/*
===============================================================================
FILE: 04_sp_gold_campaign_weekly_reconciliation.sql
LAYER: Gold QA (Reconciliation)

PURPOSE:
  Reconcile Gold Weekly against a "recomputed weekly rollup" derived from Gold Daily
  using the SAME QGP week bucketing logic.

TABLES:
  Gold Daily:   prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily
  Gold Weekly:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly

GRAIN:
  account_id + campaign_id + qgp_week

WINDOW:
  Lookback N days (default 60) to safely cover quarter-end partial-week buckets.

TESTS:
  1) Rowcount match (HIGH)
  2) Missing keys in Weekly vs recomputed (HIGH)
  3) Core metric sum match (HIGH): impressions, clicks, cost, all_conversions

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_reconciliation`()
BEGIN

  DECLARE v_table_name STRING DEFAULT 'sdi-gold-sa360-campaign-weekly';
  DECLARE v_now TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

  DECLARE v_lookback_days INT64 DEFAULT 60;
  DECLARE v_window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL v_lookback_days DAY);

  -- =====================================================================
  -- Helper: recompute weekly from Gold Daily using SAME QGP week logic
  -- =====================================================================
  -- NOTE: This must mirror your weekly build logic.
  WITH recomputed_weekly AS (
    WITH base AS (
      SELECT
        d.*,

        -- Calendar week end date (Saturday) derived from date (Sun->Sat week)
        DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_date,

        -- Quarter end date
        DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,

        -- Last Saturday on/before quarter end
        DATE_SUB(
          DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
          INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
        ) AS last_saturday_before_qe

      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily` d
      WHERE d.date >= v_window_start
    ),
    bucketed AS (
      SELECT
        *,

        -- QGP week end date (Saturday OR quarter end)
        CASE
          WHEN date > last_saturday_before_qe AND date <= quarter_end_date
          THEN quarter_end_date
          ELSE week_end_date
        END AS qgp_week
      FROM base
    )
    SELECT
      account_id,
      campaign_id,
      qgp_week,

      -- Dimensions: keep stable using ANY_VALUE (same as your weekly table approach)
      ANY_VALUE(account_name) AS account_name,
      ANY_VALUE(campaign_name) AS campaign_name,
      ANY_VALUE(lob) AS lob,
      ANY_VALUE(ad_platform) AS ad_platform,
      ANY_VALUE(campaign_type) AS campaign_type,
      ANY_VALUE(advertising_channel_type) AS advertising_channel_type,
      ANY_VALUE(advertising_channel_sub_type) AS advertising_channel_sub_type,
      ANY_VALUE(bidding_strategy_type) AS bidding_strategy_type,
      ANY_VALUE(serving_status) AS serving_status,

      ANY_VALUE(customer_id) AS customer_id,
      ANY_VALUE(customer_name) AS customer_name,
      ANY_VALUE(resource_name) AS resource_name,
      ANY_VALUE(client_manager_id) AS client_manager_id,
      ANY_VALUE(client_manager_name) AS client_manager_name,

      -- Core metrics
      SUM(impressions) AS impressions,
      SUM(clicks) AS clicks,
      SUM(cost) AS cost,
      SUM(all_conversions) AS all_conversions,

      -- Intent/quality/generic
      SUM(bi) AS bi,
      SUM(buying_intent) AS buying_intent,
      SUM(bts_quality_traffic) AS bts_quality_traffic,
      SUM(digital_gross_add) AS digital_gross_add,
      SUM(magenta_pqt) AS magenta_pqt,

      -- Cart/Postpaid/PSPV/AAL
      SUM(cart_start) AS cart_start,
      SUM(postpaid_cart_start) AS postpaid_cart_start,
      SUM(postpaid_pspv) AS postpaid_pspv,
      SUM(aal) AS aal,
      SUM(add_a_line) AS add_a_line,

      -- Connect
      SUM(connect_low_funnel_prospect) AS connect_low_funnel_prospect,
      SUM(connect_low_funnel_visit) AS connect_low_funnel_visit,
      SUM(connect_qt) AS connect_qt,

      -- HINT/HSI
      SUM(hint_ec) AS hint_ec,
      SUM(hint_sec) AS hint_sec,
      SUM(hint_web_orders) AS hint_web_orders,
      SUM(hint_invoca_calls) AS hint_invoca_calls,
      SUM(hint_offline_invoca_calls) AS hint_offline_invoca_calls,
      SUM(hint_offline_invoca_eligibility) AS hint_offline_invoca_eligibility,
      SUM(hint_offline_invoca_order) AS hint_offline_invoca_order,
      SUM(hint_offline_invoca_order_rt) AS hint_offline_invoca_order_rt,
      SUM(hint_offline_invoca_sales_opp) AS hint_offline_invoca_sales_opp,
      SUM(ma_hint_ec_eligibility_check) AS ma_hint_ec_eligibility_check,

      -- Fiber
      SUM(fiber_activations) AS fiber_activations,
      SUM(fiber_pre_order) AS fiber_pre_order,
      SUM(fiber_waitlist_sign_up) AS fiber_waitlist_sign_up,
      SUM(fiber_web_orders) AS fiber_web_orders,
      SUM(fiber_ec) AS fiber_ec,
      SUM(fiber_ec_dda) AS fiber_ec_dda,
      SUM(fiber_sec) AS fiber_sec,
      SUM(fiber_sec_dda) AS fiber_sec_dda,

      -- Metro
      SUM(metro_top_funnel_prospect) AS metro_top_funnel_prospect,
      SUM(metro_upper_funnel_prospect) AS metro_upper_funnel_prospect,
      SUM(metro_mid_funnel_prospect) AS metro_mid_funnel_prospect,
      SUM(metro_low_funnel_cs) AS metro_low_funnel_cs,
      SUM(metro_qt) AS metro_qt,
      SUM(metro_hint_qt) AS metro_hint_qt,

      -- TMO
      SUM(tmo_top_funnel_prospect) AS tmo_top_funnel_prospect,
      SUM(tmo_upper_funnel_prospect) AS tmo_upper_funnel_prospect,
      SUM(t_mobile_prepaid_low_funnel_prospect) AS t_mobile_prepaid_low_funnel_prospect,

      -- TFB
      SUM(tfb_credit_check) AS tfb_credit_check,
      SUM(tfb_invoca_sales_calls) AS tfb_invoca_sales_calls,
      SUM(tfb_leads) AS tfb_leads,
      SUM(tfb_quality_traffic) AS tfb_quality_traffic,
      SUM(tfb_hint_ec) AS tfb_hint_ec,
      SUM(total_tfb_conversions) AS total_tfb_conversions,
      SUM(tfb_low_funnel) AS tfb_low_funnel,
      SUM(tfb_lead_form_submit) AS tfb_lead_form_submit,
      SUM(tfb_invoca_sales_intent_dda) AS tfb_invoca_sales_intent_dda,
      SUM(tfb_invoca_order_dda) AS tfb_invoca_order_dda,

      MAX(file_load_datetime) AS file_load_datetime
    FROM bucketed
    GROUP BY account_id, campaign_id, qgp_week
  ),

  weekly_table AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly`
    WHERE qgp_week >= v_window_start
  )

  -- =====================================================================
  -- TEST 1: Rowcount Match (HIGH)
  -- =====================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH counts AS (
    SELECT
      (SELECT COUNT(*) FROM recomputed_weekly) AS expected_rows,
      (SELECT COUNT(*) FROM weekly_table) AS actual_rows
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'reconciliation',
    'Rowcount Match vs Recomputed from Gold Daily (60-day)',
    'HIGH',
    CAST(expected_rows AS FLOAT64),
    CAST(actual_rows AS FLOAT64),
    CAST(actual_rows - expected_rows AS FLOAT64),
    IF(actual_rows = expected_rows, 'PASS', 'FAIL'),
    IF(actual_rows = expected_rows, '🟢', '🔴'),
    IF(actual_rows = expected_rows,
      'Rowcount matches recomputed weekly rollup from Gold Daily.',
      CONCAT('Rowcount differs (Weekly=', CAST(actual_rows AS STRING), ', Recomputed=', CAST(expected_rows AS STRING), '). Possible weekly MERGE gap or bucketing mismatch.')
    ),
    IF(actual_rows = expected_rows,
      'No action required.',
      'Compare weekly build logic vs recomputation; check lookback; verify qgp_week bucketing rules.'
    ),
    IF(actual_rows = expected_rows, FALSE, TRUE),
    IF(actual_rows = expected_rows, TRUE, FALSE),
    IF(actual_rows = expected_rows, FALSE, TRUE)
  FROM counts;

  -- =====================================================================
  -- TEST 2: Missing Keys in Weekly (HIGH)
  -- =====================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH expected_keys AS (
    SELECT DISTINCT account_id, campaign_id, qgp_week
    FROM recomputed_weekly
  ),
  actual_keys AS (
    SELECT DISTINCT account_id, campaign_id, qgp_week
    FROM weekly_table
  ),
  miss AS (
    SELECT COUNT(*) AS missing_cnt
    FROM expected_keys e
    LEFT JOIN actual_keys a
      ON e.account_id = a.account_id
     AND e.campaign_id = a.campaign_id
     AND e.qgp_week = a.qgp_week
    WHERE a.account_id IS NULL
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'reconciliation',
    'Missing Weekly Keys vs Recomputed (60-day)',
    'HIGH',
    0.0,
    CAST(missing_cnt AS FLOAT64),
    CAST(missing_cnt AS FLOAT64),
    IF(missing_cnt = 0, 'PASS', 'FAIL'),
    IF(missing_cnt = 0, '🟢', '🔴'),
    IF(missing_cnt = 0,
      'No missing weekly keys. Weekly fully covers recomputed rollup keys.',
      CONCAT('Missing weekly keys detected: ', CAST(missing_cnt AS STRING), ' expected keys absent in Weekly.')
    ),
    IF(missing_cnt = 0,
      'No action required.',
      'Investigate weekly MERGE insert logic; verify lookback covers quarter-end partial buckets.'
    ),
    IF(missing_cnt = 0, FALSE, TRUE),
    IF(missing_cnt = 0, TRUE, FALSE),
    IF(missing_cnt = 0, FALSE, TRUE)
  FROM miss;

  -- =====================================================================
  -- TEST 3: Core Metric Sum Match (HIGH)
  --   Compare totals across all rows in-window for:
  --   impressions, clicks, cost, all_conversions
  -- =====================================================================
  INSERT INTO `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WITH expected AS (
    SELECT
      IFNULL(SUM(impressions),0) AS impressions,
      IFNULL(SUM(clicks),0) AS clicks,
      IFNULL(SUM(cost),0) AS cost,
      IFNULL(SUM(all_conversions),0) AS all_conversions
    FROM recomputed_weekly
  ),
  actual AS (
    SELECT
      IFNULL(SUM(impressions),0) AS impressions,
      IFNULL(SUM(clicks),0) AS clicks,
      IFNULL(SUM(cost),0) AS cost,
      IFNULL(SUM(all_conversions),0) AS all_conversions
    FROM weekly_table
  ),
  j AS (
    SELECT
      e.impressions AS e_impr, a.impressions AS a_impr,
      e.clicks AS e_clk, a.clicks AS a_clk,
      e.cost AS e_cost, a.cost AS a_cost,
      e.all_conversions AS e_conv, a.all_conversions AS a_conv
    FROM expected e CROSS JOIN actual a
  ),
  calc AS (
    SELECT
      (IF(a_impr = e_impr, 0, 1) +
       IF(a_clk = e_clk, 0, 1) +
       IF(a_cost = e_cost, 0, 1) +
       IF(a_conv = e_conv, 0, 1)) AS failed_metric_cnt
    FROM j
  )
  SELECT
    v_now,
    CURRENT_DATE(),
    v_table_name,
    'reconciliation',
    'Core Weekly Metric Sums Match vs Recomputed (60-day)',
    'HIGH',
    0.0,
    CAST(failed_metric_cnt AS FLOAT64),
    CAST(failed_metric_cnt AS FLOAT64),
    IF(failed_metric_cnt = 0, 'PASS', 'FAIL'),
    IF(failed_metric_cnt = 0, '🟢', '🔴'),
    IF(failed_metric_cnt = 0,
      'Weekly core metric sums match recomputed rollup from Gold Daily.',
      'One or more weekly core metric sums differ vs recomputed rollup (bucketing/duplication/filtering issue).'
    ),
    IF(failed_metric_cnt = 0,
      'No action required.',
      'Verify qgp_week computation and GROUP BY grain; ensure MERGE key is (account_id,campaign_id,qgp_week).'
    ),
    IF(failed_metric_cnt = 0, FALSE, TRUE),
    IF(failed_metric_cnt = 0, TRUE, FALSE),
    IF(failed_metric_cnt = 0, FALSE, TRUE)
  FROM calc;

END;
