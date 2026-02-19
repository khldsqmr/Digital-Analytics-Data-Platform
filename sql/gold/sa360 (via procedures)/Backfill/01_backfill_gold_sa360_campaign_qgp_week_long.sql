/*
===============================================================================
FILE: 01_backfill_gold_sa360_campaign_qgp_week_long.sql
LAYER: Gold (One-time Backfill)
TARGET: sdi_gold_sa360_campaign_qgp_week_long
SOURCE: sdi_gold_sa360_campaign_weekly (qgp_week wide)
===============================================================================
*/

DECLARE backfill_start_date DATE DEFAULT DATE('2024-01-01'); -- change
DECLARE backfill_end_date   DATE DEFAULT CURRENT_DATE();     -- change

-- Expand so we rebuild full qgp_week buckets touched by the window
DECLARE expanded_start DATE DEFAULT DATE_SUB(backfill_start_date, INTERVAL 14 DAY);
DECLARE expanded_end   DATE DEFAULT DATE_ADD(backfill_end_date,   INTERVAL 14 DAY);

MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_qgp_week_long` T
USING (
  WITH src AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
    WHERE qgp_week BETWEEN expanded_start AND expanded_end
  ),
  impacted AS (
    SELECT DISTINCT qgp_week
    FROM src
    WHERE qgp_week BETWEEN backfill_start_date AND backfill_end_date
  ),
  scoped AS (
    SELECT s.*
    FROM src s
    JOIN impacted i USING (qgp_week)
  ),
  longified AS (
    SELECT
      account_id,
      campaign_id,
      qgp_week,
      qgp_week_yyyymmdd,
      period_type,

      lob,
      ad_platform,
      account_name,
      campaign_name,
      campaign_type,

      advertising_channel_type,
      advertising_channel_sub_type,
      bidding_strategy_type,
      serving_status,

      metric_name,
      metric_value,

      CURRENT_TIMESTAMP() AS gold_qgp_long_inserted_at
    FROM scoped
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
ON  T.account_id = S.account_id
AND T.campaign_id = S.campaign_id
AND T.qgp_week = S.qgp_week
AND T.metric_name = S.metric_name

WHEN MATCHED THEN UPDATE SET
  qgp_week_yyyymmdd = S.qgp_week_yyyymmdd,
  period_type = S.period_type,
  lob = S.lob,
  ad_platform = S.ad_platform,
  account_name = S.account_name,
  campaign_name = S.campaign_name,
  campaign_type = S.campaign_type,
  advertising_channel_type = S.advertising_channel_type,
  advertising_channel_sub_type = S.advertising_channel_sub_type,
  bidding_strategy_type = S.bidding_strategy_type,
  serving_status = S.serving_status,
  metric_value = S.metric_value,
  gold_qgp_long_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT ROW;
