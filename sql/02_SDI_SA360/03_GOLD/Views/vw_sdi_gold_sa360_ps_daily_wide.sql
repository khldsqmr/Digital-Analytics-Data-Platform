/*
===============================================================================
FILE NAME: vw_sdi_gold_sa360_ps_daily_wide.sql
VIEW: vw_gold_sa360_daily_wide_reporting
SOURCE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily

GRAIN (UNIQUE ROW):
  - account_name
  - campaign_type
  - date
  - lob
  - ad_platform

PURPOSE:
  Reporting-friendly daily wide view using only requested dimensions + all metrics.

NOTES:
  - Gold Daily is already deduped at (account_id, campaign_id, date)
  - Since we are dropping account_id/campaign_id, we must aggregate to preserve grain
  - SUM(COALESCE(...,0)) ensures reconciliation-safe totals
===============================================================================
*/

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_gold_sa360_daily_wide_reporting` AS
SELECT
  account_name AS `Account Name`,
  campaign_type AS `Campaign Type`,
  date AS `Date`,
  lob AS `Lob`,
  ad_platform AS `Ad Platform`,

  -- Core media metrics
  SUM(COALESCE(impressions, 0)) AS impressions,
  SUM(COALESCE(clicks, 0)) AS clicks,
  SUM(COALESCE(cost, 0)) AS cost,
  SUM(COALESCE(all_conversions, 0)) AS all_conversions,

  -- Upper / mid funnel
  SUM(COALESCE(bi, 0)) AS bi,
  SUM(COALESCE(buying_intent, 0)) AS buying_intent,
  SUM(COALESCE(bts_quality_traffic, 0)) AS bts_quality_traffic,
  SUM(COALESCE(digital_gross_add, 0)) AS digital_gross_add,
  SUM(COALESCE(magenta_pqt, 0)) AS magenta_pqt,

  -- Postpaid
  SUM(COALESCE(cart_start, 0)) AS cart_start,
  SUM(COALESCE(postpaid_cart_start, 0)) AS postpaid_cart_start,
  SUM(COALESCE(postpaid_pspv, 0)) AS postpaid_pspv,
  SUM(COALESCE(aal, 0)) AS aal,
  SUM(COALESCE(add_a_line, 0)) AS add_a_line,

  -- Connect
  SUM(COALESCE(connect_low_funnel_prospect, 0)) AS connect_low_funnel_prospect,
  SUM(COALESCE(connect_low_funnel_visit, 0)) AS connect_low_funnel_visit,
  SUM(COALESCE(connect_qt, 0)) AS connect_qt,

  -- HINT
  SUM(COALESCE(hint_ec, 0)) AS hint_ec,
  SUM(COALESCE(hint_sec, 0)) AS hint_sec,
  SUM(COALESCE(hint_web_orders, 0)) AS hint_web_orders,
  SUM(COALESCE(hint_invoca_calls, 0)) AS hint_invoca_calls,
  SUM(COALESCE(hint_offline_invoca_calls, 0)) AS hint_offline_invoca_calls,
  SUM(COALESCE(hint_offline_invoca_eligibility, 0)) AS hint_offline_invoca_eligibility,
  SUM(COALESCE(hint_offline_invoca_order, 0)) AS hint_offline_invoca_order,
  SUM(COALESCE(hint_offline_invoca_order_rt, 0)) AS hint_offline_invoca_order_rt,
  SUM(COALESCE(hint_offline_invoca_sales_opp, 0)) AS hint_offline_invoca_sales_opp,
  SUM(COALESCE(ma_hint_ec_eligibility_check, 0)) AS ma_hint_ec_eligibility_check,

  -- Fiber
  SUM(COALESCE(fiber_activations, 0)) AS fiber_activations,
  SUM(COALESCE(fiber_pre_order, 0)) AS fiber_pre_order,
  SUM(COALESCE(fiber_waitlist_sign_up, 0)) AS fiber_waitlist_sign_up,
  SUM(COALESCE(fiber_web_orders, 0)) AS fiber_web_orders,
  SUM(COALESCE(fiber_ec, 0)) AS fiber_ec,
  SUM(COALESCE(fiber_ec_dda, 0)) AS fiber_ec_dda,
  SUM(COALESCE(fiber_sec, 0)) AS fiber_sec,
  SUM(COALESCE(fiber_sec_dda, 0)) AS fiber_sec_dda,

  -- Metro
  SUM(COALESCE(metro_low_funnel_cs, 0)) AS metro_low_funnel_cs,
  SUM(COALESCE(metro_mid_funnel_prospect, 0)) AS metro_mid_funnel_prospect,
  SUM(COALESCE(metro_top_funnel_prospect, 0)) AS metro_top_funnel_prospect,
  SUM(COALESCE(metro_upper_funnel_prospect, 0)) AS metro_upper_funnel_prospect,
  SUM(COALESCE(metro_hint_qt, 0)) AS metro_hint_qt,
  SUM(COALESCE(metro_qt, 0)) AS metro_qt,

  -- Prepaid
  SUM(COALESCE(tmo_prepaid_low_funnel_prospect, 0)) AS tmo_prepaid_low_funnel_prospect,
  SUM(COALESCE(tmo_top_funnel_prospect, 0)) AS tmo_top_funnel_prospect,
  SUM(COALESCE(tmo_upper_funnel_prospect, 0)) AS tmo_upper_funnel_prospect,

  -- TFB
  SUM(COALESCE(tfb_low_funnel, 0)) AS tfb_low_funnel,
  SUM(COALESCE(tfb_lead_form_submit, 0)) AS tfb_lead_form_submit,
  SUM(COALESCE(tfb_invoca_sales_intent_dda, 0)) AS tfb_invoca_sales_intent_dda,
  SUM(COALESCE(tfb_invoca_order_dda, 0)) AS tfb_invoca_order_dda,
  SUM(COALESCE(tfb_credit_check, 0)) AS tfb_credit_check,
  SUM(COALESCE(tfb_hint_ec, 0)) AS tfb_hint_ec,
  SUM(COALESCE(tfb_invoca_sales_calls, 0)) AS tfb_invoca_sales_calls,
  SUM(COALESCE(tfb_leads, 0)) AS tfb_leads,
  SUM(COALESCE(tfb_quality_traffic, 0)) AS tfb_quality_traffic,
  SUM(COALESCE(total_tfb_conversions, 0)) AS total_tfb_conversions

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
WHERE date IS NOT NULL
GROUP BY 1,2,3,4,5;