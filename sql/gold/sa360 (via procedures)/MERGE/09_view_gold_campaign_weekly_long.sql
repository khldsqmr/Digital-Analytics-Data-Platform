/*
===============================================================================
FILE: 09_view_gold_campaign_weekly_long.sql
LAYER: Gold (Tableau semantic)
VIEW: vw_sdi_gold_sa360_campaign_weekly_long

PURPOSE:
  Convert Gold Weekly wide fact into LONG format:
    - One row per (account_id, campaign_id, weekend_date, metric_name)

GRAIN:
  (account_id, campaign_id, weekend_date, metric_name)
===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gold_sa360_campaign_weekly_long` AS
SELECT
  -- keys
  w.account_id,
  w.campaign_id,
  w.weekend_date AS date_key,
  w.week_yyyymmdd,

  -- reporting dims
  w.lob,
  w.ad_platform,
  w.account_name,
  w.campaign_name,
  w.campaign_type,

  w.advertising_channel_type,
  w.advertising_channel_sub_type,
  w.bidding_strategy_type,
  w.serving_status,

  -- lineage
  w.gold_weekly_inserted_at,

  -- long fields
  m.metric_group,
  m.metric_name,
  m.metric_value
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly` w
CROSS JOIN UNNEST([
  STRUCT('Core' AS metric_group, 'impressions' AS metric_name, w.impressions AS metric_value),
  STRUCT('Core', 'clicks', w.clicks),
  STRUCT('Core', 'cost', w.cost),
  STRUCT('Core', 'all_conversions', w.all_conversions),

  STRUCT('Upper Funnel', 'bi', w.bi),
  STRUCT('Upper Funnel', 'buying_intent', w.buying_intent),
  STRUCT('Upper Funnel', 'bts_quality_traffic', w.bts_quality_traffic),
  STRUCT('Upper Funnel', 'digital_gross_add', w.digital_gross_add),
  STRUCT('Upper Funnel', 'magenta_pqt', w.magenta_pqt),

  STRUCT('Postpaid', 'cart_start', w.cart_start),
  STRUCT('Postpaid', 'postpaid_cart_start', w.postpaid_cart_start),
  STRUCT('Postpaid', 'postpaid_pspv', w.postpaid_pspv),
  STRUCT('Postpaid', 'aal', w.aal),
  STRUCT('Postpaid', 'add_a_line', w.add_a_line),

  STRUCT('Connect', 'connect_low_funnel_prospect', w.connect_low_funnel_prospect),
  STRUCT('Connect', 'connect_low_funnel_visit', w.connect_low_funnel_visit),
  STRUCT('Connect', 'connect_qt', w.connect_qt),

  STRUCT('HINT', 'hint_ec', w.hint_ec),
  STRUCT('HINT', 'hint_sec', w.hint_sec),
  STRUCT('HINT', 'hint_web_orders', w.hint_web_orders),
  STRUCT('HINT', 'hint_invoca_calls', w.hint_invoca_calls),
  STRUCT('HINT', 'hint_offline_invoca_calls', w.hint_offline_invoca_calls),
  STRUCT('HINT', 'hint_offline_invoca_eligibility', w.hint_offline_invoca_eligibility),
  STRUCT('HINT', 'hint_offline_invoca_order', w.hint_offline_invoca_order),
  STRUCT('HINT', 'hint_offline_invoca_order_rt', w.hint_offline_invoca_order_rt),
  STRUCT('HINT', 'hint_offline_invoca_sales_opp', w.hint_offline_invoca_sales_opp),
  STRUCT('HINT', 'ma_hint_ec_eligibility_check', w.ma_hint_ec_eligibility_check),

  STRUCT('Fiber', 'fiber_activations', w.fiber_activations),
  STRUCT('Fiber', 'fiber_pre_order', w.fiber_pre_order),
  STRUCT('Fiber', 'fiber_waitlist_sign_up', w.fiber_waitlist_sign_up),
  STRUCT('Fiber', 'fiber_web_orders', w.fiber_web_orders),
  STRUCT('Fiber', 'fiber_ec', w.fiber_ec),
  STRUCT('Fiber', 'fiber_ec_dda', w.fiber_ec_dda),
  STRUCT('Fiber', 'fiber_sec', w.fiber_sec),
  STRUCT('Fiber', 'fiber_sec_dda', w.fiber_sec_dda),

  STRUCT('Metro', 'metro_low_funnel_cs', w.metro_low_funnel_cs),
  STRUCT('Metro', 'metro_mid_funnel_prospect', w.metro_mid_funnel_prospect),
  STRUCT('Metro', 'metro_top_funnel_prospect', w.metro_top_funnel_prospect),
  STRUCT('Metro', 'metro_upper_funnel_prospect', w.metro_upper_funnel_prospect),
  STRUCT('Metro', 'metro_hint_qt', w.metro_hint_qt),
  STRUCT('Metro', 'metro_qt', w.metro_qt),

  STRUCT('Prepaid', 'tmo_prepaid_low_funnel_prospect', w.tmo_prepaid_low_funnel_prospect),
  STRUCT('Prepaid', 'tmo_top_funnel_prospect', w.tmo_top_funnel_prospect),
  STRUCT('Prepaid', 'tmo_upper_funnel_prospect', w.tmo_upper_funnel_prospect),

  STRUCT('TFB', 'tfb_low_funnel', w.tfb_low_funnel),
  STRUCT('TFB', 'tfb_lead_form_submit', w.tfb_lead_form_submit),
  STRUCT('TFB', 'tfb_invoca_sales_intent_dda', w.tfb_invoca_sales_intent_dda),
  STRUCT('TFB', 'tfb_invoca_order_dda', w.tfb_invoca_order_dda),
  STRUCT('TFB', 'tfb_credit_check', w.tfb_credit_check),
  STRUCT('TFB', 'tfb_hint_ec', w.tfb_hint_ec),
  STRUCT('TFB', 'tfb_invoca_sales_calls', w.tfb_invoca_sales_calls),
  STRUCT('TFB', 'tfb_leads', w.tfb_leads),
  STRUCT('TFB', 'tfb_quality_traffic', w.tfb_quality_traffic),
  STRUCT('TFB', 'total_tfb_conversions', w.total_tfb_conversions)
]) AS m
WHERE m.metric_value IS NOT NULL;
