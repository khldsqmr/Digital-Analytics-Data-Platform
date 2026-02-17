/*
===============================================================================
FILE: 03_create_sdi_gold_sa360_campaign_weekly.sql
LAYER: Gold (Weekly)
TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily

QGP WEEK:
  qgp_week is the bucket end date:
    - Saturday for normal weeks (Sun..Sat)
    - Quarter end date for quarter-end partial weeks

GRAIN:
  account_id + campaign_id + qgp_week

COLUMNS:
  Only qgp_week as the time grain (no calendar week start/end).

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
PARTITION BY qgp_week
CLUSTER BY lob, ad_platform, account_id, campaign_id
OPTIONS(
  description = "Gold SA360 WEEKLY dashboard table. qgp_week = Saturday week-ending OR quarter-end partial bucket end date."
)
AS
WITH base AS (
  SELECT
    d.*,

    -- week ending Saturday (Sun..Sat)
    DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_saturday,

    -- quarter end date for the date's quarter
    DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,

    -- last Saturday on/before quarter end (within same quarter)
    DATE_SUB(
      DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
      INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
    ) AS last_saturday_before_qe
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily` d
),
bucketed AS (
  SELECT
    *,

    CASE
      -- after the last Saturday and up to quarter end => bucket to quarter_end_date
      WHEN date > last_saturday_before_qe AND date <= quarter_end_date
      THEN quarter_end_date
      ELSE week_end_saturday
    END AS qgp_week
  FROM base
)
SELECT
  account_id,
  ANY_VALUE(account_name) AS account_name,
  campaign_id,
  ANY_VALUE(campaign_name) AS campaign_name,

  qgp_week,

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

  -- core sums
  SUM(impressions) AS impressions,
  SUM(clicks) AS clicks,
  SUM(cost) AS cost,
  SUM(all_conversions) AS all_conversions,

  -- intent/quality
  SUM(bi) AS bi,
  SUM(buying_intent) AS buying_intent,
  SUM(bts_quality_traffic) AS bts_quality_traffic,
  SUM(digital_gross_add) AS digital_gross_add,
  SUM(magenta_pqt) AS magenta_pqt,

  -- postpaid/cart
  SUM(cart_start) AS cart_start,
  SUM(postpaid_cart_start) AS postpaid_cart_start,
  SUM(postpaid_pspv) AS postpaid_pspv,
  SUM(aal) AS aal,
  SUM(add_a_line) AS add_a_line,

  -- connect
  SUM(connect_low_funnel_prospect) AS connect_low_funnel_prospect,
  SUM(connect_low_funnel_visit) AS connect_low_funnel_visit,
  SUM(connect_qt) AS connect_qt,

  -- hint/hsi
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

  -- fiber
  SUM(fiber_activations) AS fiber_activations,
  SUM(fiber_pre_order) AS fiber_pre_order,
  SUM(fiber_waitlist_sign_up) AS fiber_waitlist_sign_up,
  SUM(fiber_web_orders) AS fiber_web_orders,
  SUM(fiber_ec) AS fiber_ec,
  SUM(fiber_ec_dda) AS fiber_ec_dda,
  SUM(fiber_sec) AS fiber_sec,
  SUM(fiber_sec_dda) AS fiber_sec_dda,

  -- metro
  SUM(metro_top_funnel_prospect) AS metro_top_funnel_prospect,
  SUM(metro_upper_funnel_prospect) AS metro_upper_funnel_prospect,
  SUM(metro_mid_funnel_prospect) AS metro_mid_funnel_prospect,
  SUM(metro_low_funnel_cs) AS metro_low_funnel_cs,
  SUM(metro_qt) AS metro_qt,
  SUM(metro_hint_qt) AS metro_hint_qt,

  -- tmo
  SUM(tmo_top_funnel_prospect) AS tmo_top_funnel_prospect,
  SUM(tmo_upper_funnel_prospect) AS tmo_upper_funnel_prospect,
  SUM(tmo_prepaid_low_funnel_prospect) AS tmo_prepaid_low_funnel_prospect,

  -- tfb
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

  MAX(file_load_datetime) AS file_load_datetime,
  CURRENT_TIMESTAMP() AS gold_inserted_at

FROM bucketed
GROUP BY account_id, campaign_id, qgp_week;
