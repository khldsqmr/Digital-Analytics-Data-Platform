/*
===============================================================================
FILE: 08_view_gold_campaign_daily_long.sql
LAYER: Gold (Tableau semantic)
VIEW: vw_sdi_gold_sa360_campaign_daily_long

PURPOSE:
  Convert Gold Daily wide fact into LONG format:
    - One row per (account_id, campaign_id, date, metric_name)
    - metric_value holds the numeric value
  Tableau can use:
    - metric_name as Dimension
    - metric_value as Measure

GRAIN:
  (account_id, campaign_id, date, metric_name)
===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gold_sa360_campaign_daily_long` AS
SELECT
  -- keys
  d.account_id,
  d.campaign_id,
  d.date AS date_key,

  -- reporting dims
  d.lob,
  d.ad_platform,
  d.account_name,
  d.campaign_name,
  d.campaign_type,

  d.advertising_channel_type,
  d.advertising_channel_sub_type,
  d.bidding_strategy_type,
  d.serving_status,

  -- lineage
  d.file_load_datetime,
  d.gold_inserted_at,

  -- long fields
  m.metric_group,
  m.metric_name,
  m.metric_value
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily` d
CROSS JOIN UNNEST([
  -- -------------------------
  -- Core performance
  -- -------------------------
  STRUCT('Core' AS metric_group, 'impressions' AS metric_name, d.impressions AS metric_value),
  STRUCT('Core', 'clicks', d.clicks),
  STRUCT('Core', 'cost', d.cost),
  STRUCT('Core', 'all_conversions', d.all_conversions),

  -- -------------------------
  -- Brand / intent / quality
  -- -------------------------
  STRUCT('Upper Funnel', 'bi', d.bi),
  STRUCT('Upper Funnel', 'buying_intent', d.buying_intent),
  STRUCT('Upper Funnel', 'bts_quality_traffic', d.bts_quality_traffic),
  STRUCT('Upper Funnel', 'digital_gross_add', d.digital_gross_add),
  STRUCT('Upper Funnel', 'magenta_pqt', d.magenta_pqt),

  -- -------------------------
  -- Postpaid
  -- -------------------------
  STRUCT('Postpaid', 'cart_start', d.cart_start),
  STRUCT('Postpaid', 'postpaid_cart_start', d.postpaid_cart_start),
  STRUCT('Postpaid', 'postpaid_pspv', d.postpaid_pspv),
  STRUCT('Postpaid', 'aal', d.aal),
  STRUCT('Postpaid', 'add_a_line', d.add_a_line),

  -- -------------------------
  -- Connect
  -- -------------------------
  STRUCT('Connect', 'connect_low_funnel_prospect', d.connect_low_funnel_prospect),
  STRUCT('Connect', 'connect_low_funnel_visit', d.connect_low_funnel_visit),
  STRUCT('Connect', 'connect_qt', d.connect_qt),

  -- -------------------------
  -- HINT / Invoca
  -- -------------------------
  STRUCT('HINT', 'hint_ec', d.hint_ec),
  STRUCT('HINT', 'hint_sec', d.hint_sec),
  STRUCT('HINT', 'hint_web_orders', d.hint_web_orders),
  STRUCT('HINT', 'hint_invoca_calls', d.hint_invoca_calls),
  STRUCT('HINT', 'hint_offline_invoca_calls', d.hint_offline_invoca_calls),
  STRUCT('HINT', 'hint_offline_invoca_eligibility', d.hint_offline_invoca_eligibility),
  STRUCT('HINT', 'hint_offline_invoca_order', d.hint_offline_invoca_order),
  STRUCT('HINT', 'hint_offline_invoca_order_rt', d.hint_offline_invoca_order_rt),
  STRUCT('HINT', 'hint_offline_invoca_sales_opp', d.hint_offline_invoca_sales_opp),
  STRUCT('HINT', 'ma_hint_ec_eligibility_check', d.ma_hint_ec_eligibility_check),

  -- -------------------------
  -- Fiber
  -- -------------------------
  STRUCT('Fiber', 'fiber_activations', d.fiber_activations),
  STRUCT('Fiber', 'fiber_pre_order', d.fiber_pre_order),
  STRUCT('Fiber', 'fiber_waitlist_sign_up', d.fiber_waitlist_sign_up),
  STRUCT('Fiber', 'fiber_web_orders', d.fiber_web_orders),
  STRUCT('Fiber', 'fiber_ec', d.fiber_ec),
  STRUCT('Fiber', 'fiber_ec_dda', d.fiber_ec_dda),
  STRUCT('Fiber', 'fiber_sec', d.fiber_sec),
  STRUCT('Fiber', 'fiber_sec_dda', d.fiber_sec_dda),

  -- -------------------------
  -- Metro
  -- -------------------------
  STRUCT('Metro', 'metro_low_funnel_cs', d.metro_low_funnel_cs),
  STRUCT('Metro', 'metro_mid_funnel_prospect', d.metro_mid_funnel_prospect),
  STRUCT('Metro', 'metro_top_funnel_prospect', d.metro_top_funnel_prospect),
  STRUCT('Metro', 'metro_upper_funnel_prospect', d.metro_upper_funnel_prospect),
  STRUCT('Metro', 'metro_hint_qt', d.metro_hint_qt),
  STRUCT('Metro', 'metro_qt', d.metro_qt),

  -- -------------------------
  -- TMO Prepaid
  -- -------------------------
  STRUCT('Prepaid', 'tmo_prepaid_low_funnel_prospect', d.tmo_prepaid_low_funnel_prospect),
  STRUCT('Prepaid', 'tmo_top_funnel_prospect', d.tmo_top_funnel_prospect),
  STRUCT('Prepaid', 'tmo_upper_funnel_prospect', d.tmo_upper_funnel_prospect),

  -- -------------------------
  -- TFB
  -- -------------------------
  STRUCT('TFB', 'tfb_low_funnel', d.tfb_low_funnel),
  STRUCT('TFB', 'tfb_lead_form_submit', d.tfb_lead_form_submit),
  STRUCT('TFB', 'tfb_invoca_sales_intent_dda', d.tfb_invoca_sales_intent_dda),
  STRUCT('TFB', 'tfb_invoca_order_dda', d.tfb_invoca_order_dda),
  STRUCT('TFB', 'tfb_credit_check', d.tfb_credit_check),
  STRUCT('TFB', 'tfb_hint_ec', d.tfb_hint_ec),
  STRUCT('TFB', 'tfb_invoca_sales_calls', d.tfb_invoca_sales_calls),
  STRUCT('TFB', 'tfb_leads', d.tfb_leads),
  STRUCT('TFB', 'tfb_quality_traffic', d.tfb_quality_traffic),
  STRUCT('TFB', 'total_tfb_conversions', d.total_tfb_conversions)
]) AS m
WHERE m.metric_value IS NOT NULL;
