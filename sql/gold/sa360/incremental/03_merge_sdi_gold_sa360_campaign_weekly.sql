/*
===============================================================================
FILE: 03_merge_sdi_gold_sa360_campaign_weekly.sql
LAYER: Gold Weekly
TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily

INCREMENTAL STRATEGY:
  Recompute recent history (N days) then MERGE by:
    account_id + campaign_id + qgp_week

NOTE:
  We use a day-based lookback to safely cover quarter-end partial weeks.
===============================================================================
*/
BEGIN

DECLARE lookback_days INT64 DEFAULT 60;

MERGE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-weekly` T
USING (
  WITH base AS (
    SELECT
      d.*,

      DATE_TRUNC(d.date, WEEK(SUNDAY)) AS week_start_date,
      DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_date,

      DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,

      DATE_SUB(
        DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
        INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
      ) AS last_saturday_before_qe

    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi-gold-sa360-campaign-daily` d
    WHERE d.date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),
  bucketed AS (
    SELECT
      *,

      CASE
        WHEN date > last_saturday_before_qe AND date <= quarter_end_date
        THEN quarter_end_date
        ELSE week_end_date
      END AS qgp_week

    FROM base
  )
  SELECT
    account_id,
    ANY_VALUE(account_name) AS account_name,
    campaign_id,
    ANY_VALUE(campaign_name) AS campaign_name,

    ANY_VALUE(week_start_date) AS week_start_date,
    ANY_VALUE(week_end_date) AS week_end_date,

    qgp_week,

    DATE_ADD(
      DATE_SUB(qgp_week, INTERVAL MOD(EXTRACT(DAYOFWEEK FROM qgp_week), 7) DAY),
      INTERVAL 1 DAY
    ) AS qgp_period_start_date,
    qgp_week AS qgp_period_end_date,

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

    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS cost,
    SUM(all_conversions) AS all_conversions,

    SUM(bi) AS bi,
    SUM(buying_intent) AS buying_intent,
    SUM(bts_quality_traffic) AS bts_quality_traffic,
    SUM(digital_gross_add) AS digital_gross_add,
    SUM(magenta_pqt) AS magenta_pqt,

    SUM(cart_start) AS cart_start,
    SUM(postpaid_cart_start) AS postpaid_cart_start,
    SUM(postpaid_pspv) AS postpaid_pspv,
    SUM(aal) AS aal,
    SUM(add_a_line) AS add_a_line,

    SUM(connect_low_funnel_prospect) AS connect_low_funnel_prospect,
    SUM(connect_low_funnel_visit) AS connect_low_funnel_visit,
    SUM(connect_qt) AS connect_qt,

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

    SUM(fiber_activations) AS fiber_activations,
    SUM(fiber_pre_order) AS fiber_pre_order,
    SUM(fiber_waitlist_sign_up) AS fiber_waitlist_sign_up,
    SUM(fiber_web_orders) AS fiber_web_orders,
    SUM(fiber_ec) AS fiber_ec,
    SUM(fiber_ec_dda) AS fiber_ec_dda,
    SUM(fiber_sec) AS fiber_sec,
    SUM(fiber_sec_dda) AS fiber_sec_dda,

    SUM(metro_top_funnel_prospect) AS metro_top_funnel_prospect,
    SUM(metro_upper_funnel_prospect) AS metro_upper_funnel_prospect,
    SUM(metro_mid_funnel_prospect) AS metro_mid_funnel_prospect,
    SUM(metro_low_funnel_cs) AS metro_low_funnel_cs,
    SUM(metro_qt) AS metro_qt,
    SUM(metro_hint_qt) AS metro_hint_qt,

    SUM(tmo_top_funnel_prospect) AS tmo_top_funnel_prospect,
    SUM(tmo_upper_funnel_prospect) AS tmo_upper_funnel_prospect,
    SUM(t_mobile_prepaid_low_funnel_prospect) AS t_mobile_prepaid_low_funnel_prospect,

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
  GROUP BY
    account_id,
    campaign_id,
    qgp_week
) S
ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.qgp_week = S.qgp_week

WHEN MATCHED THEN UPDATE SET
  account_name = S.account_name,
  campaign_name = S.campaign_name,

  week_start_date = S.week_start_date,
  week_end_date = S.week_end_date,

  qgp_period_start_date = S.qgp_period_start_date,
  qgp_period_end_date = S.qgp_period_end_date,

  lob = S.lob,
  ad_platform = S.ad_platform,

  campaign_type = S.campaign_type,
  advertising_channel_type = S.advertising_channel_type,
  advertising_channel_sub_type = S.advertising_channel_sub_type,
  bidding_strategy_type = S.bidding_strategy_type,
  serving_status = S.serving_status,

  customer_id = S.customer_id,
  customer_name = S.customer_name,
  resource_name = S.resource_name,
  client_manager_id = S.client_manager_id,
  client_manager_name = S.client_manager_name,

  impressions = S.impressions,
  clicks = S.clicks,
  cost = S.cost,
  all_conversions = S.all_conversions,

  bi = S.bi,
  buying_intent = S.buying_intent,
  bts_quality_traffic = S.bts_quality_traffic,
  digital_gross_add = S.digital_gross_add,
  magenta_pqt = S.magenta_pqt,

  cart_start = S.cart_start,
  postpaid_cart_start = S.postpaid_cart_start,
  postpaid_pspv = S.postpaid_pspv,
  aal = S.aal,
  add_a_line = S.add_a_line,

  connect_low_funnel_prospect = S.connect_low_funnel_prospect,
  connect_low_funnel_visit = S.connect_low_funnel_visit,
  connect_qt = S.connect_qt,

  hint_ec = S.hint_ec,
  hint_sec = S.hint_sec,
  hint_web_orders = S.hint_web_orders,
  hint_invoca_calls = S.hint_invoca_calls,
  hint_offline_invoca_calls = S.hint_offline_invoca_calls,
  hint_offline_invoca_eligibility = S.hint_offline_invoca_eligibility,
  hint_offline_invoca_order = S.hint_offline_invoca_order,
  hint_offline_invoca_order_rt = S.hint_offline_invoca_order_rt,
  hint_offline_invoca_sales_opp = S.hint_offline_invoca_sales_opp,
  ma_hint_ec_eligibility_check = S.ma_hint_ec_eligibility_check,

  fiber_activations = S.fiber_activations,
  fiber_pre_order = S.fiber_pre_order,
  fiber_waitlist_sign_up = S.fiber_waitlist_sign_up,
  fiber_web_orders = S.fiber_web_orders,
  fiber_ec = S.fiber_ec,
  fiber_ec_dda = S.fiber_ec_dda,
  fiber_sec = S.fiber_sec,
  fiber_sec_dda = S.fiber_sec_dda,

  metro_top_funnel_prospect = S.metro_top_funnel_prospect,
  metro_upper_funnel_prospect = S.metro_upper_funnel_prospect,
  metro_mid_funnel_prospect = S.metro_mid_funnel_prospect,
  metro_low_funnel_cs = S.metro_low_funnel_cs,
  metro_qt = S.metro_qt,
  metro_hint_qt = S.metro_hint_qt,

  tmo_top_funnel_prospect = S.tmo_top_funnel_prospect,
  tmo_upper_funnel_prospect = S.tmo_upper_funnel_prospect,
  t_mobile_prepaid_low_funnel_prospect = S.t_mobile_prepaid_low_funnel_prospect,

  tfb_credit_check = S.tfb_credit_check,
  tfb_invoca_sales_calls = S.tfb_invoca_sales_calls,
  tfb_leads = S.tfb_leads,
  tfb_quality_traffic = S.tfb_quality_traffic,
  tfb_hint_ec = S.tfb_hint_ec,
  total_tfb_conversions = S.total_tfb_conversions,
  tfb_low_funnel = S.tfb_low_funnel,
  tfb_lead_form_submit = S.tfb_lead_form_submit,
  tfb_invoca_sales_intent_dda = S.tfb_invoca_sales_intent_dda,
  tfb_invoca_order_dda = S.tfb_invoca_order_dda,

  file_load_datetime = S.file_load_datetime,
  gold_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT ROW;

END;
