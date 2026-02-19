/*
===============================================================================
FILE: 01_backfill_gold_sa360_campaign_weekly_long.sql  (UPDATED)
LAYER: Gold (One-time Backfill)
TARGET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long
SOURCE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily

PURPOSE:
  Build WEEKLY_LONG using SAME qgp_week rule as weekly wide:
    - week_end_sat = DATE_TRUNC(date, WEEK(SATURDAY))
    - quarter_end = LAST_DAY(date, QUARTER)
    - if quarter_end < week_end_sat AND date <= quarter_end => qgp_week = quarter_end (partial)
      else qgp_week = week_end_sat

  Metrics = SUM(daily) to guarantee reconciliation.

===============================================================================
*/

DECLARE backfill_start_date DATE DEFAULT DATE('2024-01-01');
DECLARE backfill_end_date   DATE DEFAULT CURRENT_DATE();

-- Expand scan window (covers partial-week edges + quarter tail)
DECLARE expanded_start DATE DEFAULT DATE_SUB(backfill_start_date, INTERVAL 40 DAY);
DECLARE expanded_end   DATE DEFAULT DATE_ADD(backfill_end_date,   INTERVAL 40 DAY);

MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long` T
USING (
  WITH base AS (
    SELECT
      account_id,
      campaign_id,
      date,

      DATE_TRUNC(date, WEEK(SATURDAY)) AS week_end_sat,
      LAST_DAY(date, QUARTER)          AS quarter_end,

      lob, ad_platform, account_name, campaign_name, campaign_type,
      advertising_channel_type, advertising_channel_sub_type, bidding_strategy_type, serving_status,

      impressions, clicks, cost, all_conversions,
      bi, buying_intent, bts_quality_traffic, digital_gross_add, magenta_pqt,
      cart_start, postpaid_cart_start, postpaid_pspv, aal, add_a_line,
      connect_low_funnel_prospect, connect_low_funnel_visit, connect_qt,
      hint_ec, hint_sec, hint_web_orders, hint_invoca_calls, hint_offline_invoca_calls,
      hint_offline_invoca_eligibility, hint_offline_invoca_order, hint_offline_invoca_order_rt,
      hint_offline_invoca_sales_opp, ma_hint_ec_eligibility_check,
      fiber_activations, fiber_pre_order, fiber_waitlist_sign_up, fiber_web_orders,
      fiber_ec, fiber_ec_dda, fiber_sec, fiber_sec_dda,
      metro_low_funnel_cs, metro_mid_funnel_prospect, metro_top_funnel_prospect, metro_upper_funnel_prospect,
      metro_hint_qt, metro_qt,
      tmo_prepaid_low_funnel_prospect, tmo_top_funnel_prospect, tmo_upper_funnel_prospect,
      tfb_low_funnel, tfb_lead_form_submit, tfb_invoca_sales_intent_dda, tfb_invoca_order_dda,
      tfb_credit_check, tfb_hint_ec, tfb_invoca_sales_calls, tfb_leads, tfb_quality_traffic,
      total_tfb_conversions
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
    WHERE date BETWEEN expanded_start AND expanded_end
  ),

  bucketed AS (
    SELECT
      *,
      CASE
        WHEN quarter_end < week_end_sat AND date <= quarter_end THEN quarter_end
        ELSE week_end_sat
      END AS qgp_week,
      CASE
        WHEN quarter_end < week_end_sat AND date <= quarter_end THEN 'QUARTER_END_PARTIAL'
        ELSE 'WEEKLY'
      END AS period_type
    FROM base
  ),

  impacted_qgp_weeks AS (
    SELECT DISTINCT qgp_week
    FROM bucketed
    WHERE date BETWEEN backfill_start_date AND backfill_end_date
      AND qgp_week IS NOT NULL
  ),

  scoped AS (
    SELECT b.*
    FROM bucketed b
    JOIN impacted_qgp_weeks i USING (qgp_week)
  ),

  agg AS (
    SELECT
      account_id,
      campaign_id,
      qgp_week,
      FORMAT_DATE('%Y%m%d', qgp_week) AS qgp_week_yyyymmdd,
      period_type,

      ARRAY_AGG(lob IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS lob,
      ARRAY_AGG(ad_platform IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS ad_platform,
      ARRAY_AGG(account_name IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS account_name,
      ARRAY_AGG(campaign_name IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS campaign_name,
      ARRAY_AGG(campaign_type IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS campaign_type,
      ARRAY_AGG(advertising_channel_type IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS advertising_channel_type,
      ARRAY_AGG(advertising_channel_sub_type IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS advertising_channel_sub_type,
      ARRAY_AGG(bidding_strategy_type IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS bidding_strategy_type,
      ARRAY_AGG(serving_status IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS serving_status,

      SUM(COALESCE(impressions,0)) AS impressions,
      SUM(COALESCE(clicks,0)) AS clicks,
      SUM(COALESCE(cost,0)) AS cost,
      SUM(COALESCE(all_conversions,0)) AS all_conversions,

      SUM(COALESCE(bi,0)) AS bi,
      SUM(COALESCE(buying_intent,0)) AS buying_intent,
      SUM(COALESCE(bts_quality_traffic,0)) AS bts_quality_traffic,
      SUM(COALESCE(digital_gross_add,0)) AS digital_gross_add,
      SUM(COALESCE(magenta_pqt,0)) AS magenta_pqt,

      SUM(COALESCE(cart_start,0)) AS cart_start,
      SUM(COALESCE(postpaid_cart_start,0)) AS postpaid_cart_start,
      SUM(COALESCE(postpaid_pspv,0)) AS postpaid_pspv,
      SUM(COALESCE(aal,0)) AS aal,
      SUM(COALESCE(add_a_line,0)) AS add_a_line,

      SUM(COALESCE(connect_low_funnel_prospect,0)) AS connect_low_funnel_prospect,
      SUM(COALESCE(connect_low_funnel_visit,0)) AS connect_low_funnel_visit,
      SUM(COALESCE(connect_qt,0)) AS connect_qt,

      SUM(COALESCE(hint_ec,0)) AS hint_ec,
      SUM(COALESCE(hint_sec,0)) AS hint_sec,
      SUM(COALESCE(hint_web_orders,0)) AS hint_web_orders,
      SUM(COALESCE(hint_invoca_calls,0)) AS hint_invoca_calls,
      SUM(COALESCE(hint_offline_invoca_calls,0)) AS hint_offline_invoca_calls,
      SUM(COALESCE(hint_offline_invoca_eligibility,0)) AS hint_offline_invoca_eligibility,
      SUM(COALESCE(hint_offline_invoca_order,0)) AS hint_offline_invoca_order,
      SUM(COALESCE(hint_offline_invoca_order_rt,0)) AS hint_offline_invoca_order_rt,
      SUM(COALESCE(hint_offline_invoca_sales_opp,0)) AS hint_offline_invoca_sales_opp,
      SUM(COALESCE(ma_hint_ec_eligibility_check,0)) AS ma_hint_ec_eligibility_check,

      SUM(COALESCE(fiber_activations,0)) AS fiber_activations,
      SUM(COALESCE(fiber_pre_order,0)) AS fiber_pre_order,
      SUM(COALESCE(fiber_waitlist_sign_up,0)) AS fiber_waitlist_sign_up,
      SUM(COALESCE(fiber_web_orders,0)) AS fiber_web_orders,
      SUM(COALESCE(fiber_ec,0)) AS fiber_ec,
      SUM(COALESCE(fiber_ec_dda,0)) AS fiber_ec_dda,
      SUM(COALESCE(fiber_sec,0)) AS fiber_sec,
      SUM(COALESCE(fiber_sec_dda,0)) AS fiber_sec_dda,

      SUM(COALESCE(metro_low_funnel_cs,0)) AS metro_low_funnel_cs,
      SUM(COALESCE(metro_mid_funnel_prospect,0)) AS metro_mid_funnel_prospect,
      SUM(COALESCE(metro_top_funnel_prospect,0)) AS metro_top_funnel_prospect,
      SUM(COALESCE(metro_upper_funnel_prospect,0)) AS metro_upper_funnel_prospect,
      SUM(COALESCE(metro_hint_qt,0)) AS metro_hint_qt,
      SUM(COALESCE(metro_qt,0)) AS metro_qt,

      SUM(COALESCE(tmo_prepaid_low_funnel_prospect,0)) AS tmo_prepaid_low_funnel_prospect,
      SUM(COALESCE(tmo_top_funnel_prospect,0)) AS tmo_top_funnel_prospect,
      SUM(COALESCE(tmo_upper_funnel_prospect,0)) AS tmo_upper_funnel_prospect,

      SUM(COALESCE(tfb_low_funnel,0)) AS tfb_low_funnel,
      SUM(COALESCE(tfb_lead_form_submit,0)) AS tfb_lead_form_submit,
      SUM(COALESCE(tfb_invoca_sales_intent_dda,0)) AS tfb_invoca_sales_intent_dda,
      SUM(COALESCE(tfb_invoca_order_dda,0)) AS tfb_invoca_order_dda,

      SUM(COALESCE(tfb_credit_check,0)) AS tfb_credit_check,
      SUM(COALESCE(tfb_hint_ec,0)) AS tfb_hint_ec,
      SUM(COALESCE(tfb_invoca_sales_calls,0)) AS tfb_invoca_sales_calls,
      SUM(COALESCE(tfb_leads,0)) AS tfb_leads,
      SUM(COALESCE(tfb_quality_traffic,0)) AS tfb_quality_traffic,
      SUM(COALESCE(total_tfb_conversions,0)) AS total_tfb_conversions
    FROM scoped
    GROUP BY 1,2,3,4,5
  ),

  longified AS (
    SELECT
      account_id, campaign_id, qgp_week, qgp_week_yyyymmdd, period_type,
      lob, ad_platform, account_name, campaign_name, campaign_type,
      advertising_channel_type, advertising_channel_sub_type, bidding_strategy_type, serving_status,
      metric_name, metric_value,
      CURRENT_TIMESTAMP() AS gold_qgp_long_inserted_at
    FROM agg
    UNPIVOT (
      metric_value FOR metric_name IN (
        impressions, clicks, cost, all_conversions,
        bi, buying_intent, bts_quality_traffic, digital_gross_add, magenta_pqt,
        cart_start, postpaid_cart_start, postpaid_pspv, aal, add_a_line,
        connect_low_funnel_prospect, connect_low_funnel_visit, connect_qt,
        hint_ec, hint_sec, hint_web_orders, hint_invoca_calls, hint_offline_invoca_calls,
        hint_offline_invoca_eligibility, hint_offline_invoca_order, hint_offline_invoca_order_rt,
        hint_offline_invoca_sales_opp, ma_hint_ec_eligibility_check,
        fiber_activations, fiber_pre_order, fiber_waitlist_sign_up, fiber_web_orders,
        fiber_ec, fiber_ec_dda, fiber_sec, fiber_sec_dda,
        metro_low_funnel_cs, metro_mid_funnel_prospect, metro_top_funnel_prospect, metro_upper_funnel_prospect,
        metro_hint_qt, metro_qt,
        tmo_prepaid_low_funnel_prospect, tmo_top_funnel_prospect, tmo_upper_funnel_prospect,
        tfb_low_funnel, tfb_lead_form_submit, tfb_invoca_sales_intent_dda, tfb_invoca_order_dda,
        tfb_credit_check, tfb_hint_ec, tfb_invoca_sales_calls, tfb_leads, tfb_quality_traffic,
        total_tfb_conversions
      )
    )
  )

  SELECT * FROM longified
) S
ON  T.account_id  = S.account_id
AND T.campaign_id = S.campaign_id
AND T.qgp_week    = S.qgp_week
AND T.metric_name = S.metric_name

WHEN MATCHED THEN UPDATE SET
  qgp_week_yyyymmdd = S.qgp_week_yyyymmdd,
  period_type       = S.period_type,
  lob               = S.lob,
  ad_platform       = S.ad_platform,
  account_name      = S.account_name,
  campaign_name     = S.campaign_name,
  campaign_type     = S.campaign_type,
  advertising_channel_type = S.advertising_channel_type,
  advertising_channel_sub_type = S.advertising_channel_sub_type,
  bidding_strategy_type = S.bidding_strategy_type,
  serving_status    = S.serving_status,
  metric_value      = S.metric_value,
  gold_qgp_long_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT ROW;
