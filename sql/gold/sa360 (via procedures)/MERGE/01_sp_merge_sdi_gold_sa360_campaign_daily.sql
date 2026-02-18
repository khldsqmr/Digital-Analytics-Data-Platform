/*
===============================================================================
FILE: 01_sp_merge_sdi_gold_sa360_campaign_daily.sql
LAYER: Gold
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_daily

PURPOSE:
  Upsert recent Silver daily rows into Gold daily.
  - NO additional dedupe
  - Gold is a clean projection of Silver (reporting schema)
  - lookback window handles late arriving entity updates that affect Silver

MERGE KEY:
  (account_id, campaign_id, date)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 14;

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily` T
  USING (
    SELECT
      account_id,
      campaign_id,
      date,

      lob,
      ad_platform,
      account_name,
      campaign_name,
      campaign_type,

      advertising_channel_type,
      advertising_channel_sub_type,
      bidding_strategy_type,
      serving_status,

      impressions,
      clicks,
      cost,
      all_conversions,

      bi,
      buying_intent,
      bts_quality_traffic,
      digital_gross_add,
      magenta_pqt,

      cart_start,
      postpaid_cart_start,
      postpaid_pspv,
      aal,
      add_a_line,

      connect_low_funnel_prospect,
      connect_low_funnel_visit,
      connect_qt,

      hint_ec,
      hint_sec,
      hint_web_orders,
      hint_invoca_calls,
      hint_offline_invoca_calls,
      hint_offline_invoca_eligibility,
      hint_offline_invoca_order,
      hint_offline_invoca_order_rt,
      hint_offline_invoca_sales_opp,
      ma_hint_ec_eligibility_check,

      fiber_activations,
      fiber_pre_order,
      fiber_waitlist_sign_up,
      fiber_web_orders,
      fiber_ec,
      fiber_ec_dda,
      fiber_sec,
      fiber_sec_dda,

      metro_low_funnel_cs,
      metro_mid_funnel_prospect,
      metro_top_funnel_prospect,
      metro_upper_funnel_prospect,
      metro_hint_qt,
      metro_qt,

      tmo_prepaid_low_funnel_prospect,
      tmo_top_funnel_prospect,
      tmo_upper_funnel_prospect,

      tfb_low_funnel,
      tfb_lead_form_submit,
      tfb_invoca_sales_intent_dda,
      tfb_invoca_order_dda,

      tfb_credit_check,
      tfb_hint_ec,
      tfb_invoca_sales_calls,
      tfb_leads,
      tfb_quality_traffic,
      total_tfb_conversions,

      file_load_datetime,
      CURRENT_TIMESTAMP() AS gold_inserted_at
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ) S
  ON  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date = S.date

  WHEN MATCHED THEN UPDATE SET
    lob = S.lob,
    ad_platform = S.ad_platform,
    account_name = S.account_name,
    campaign_name = S.campaign_name,
    campaign_type = S.campaign_type,

    advertising_channel_type = S.advertising_channel_type,
    advertising_channel_sub_type = S.advertising_channel_sub_type,
    bidding_strategy_type = S.bidding_strategy_type,
    serving_status = S.serving_status,

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

    metro_low_funnel_cs = S.metro_low_funnel_cs,
    metro_mid_funnel_prospect = S.metro_mid_funnel_prospect,
    metro_top_funnel_prospect = S.metro_top_funnel_prospect,
    metro_upper_funnel_prospect = S.metro_upper_funnel_prospect,
    metro_hint_qt = S.metro_hint_qt,
    metro_qt = S.metro_qt,

    tmo_prepaid_low_funnel_prospect = S.tmo_prepaid_low_funnel_prospect,
    tmo_top_funnel_prospect = S.tmo_top_funnel_prospect,
    tmo_upper_funnel_prospect = S.tmo_upper_funnel_prospect,

    tfb_low_funnel = S.tfb_low_funnel,
    tfb_lead_form_submit = S.tfb_lead_form_submit,
    tfb_invoca_sales_intent_dda = S.tfb_invoca_sales_intent_dda,
    tfb_invoca_order_dda = S.tfb_invoca_order_dda,

    tfb_credit_check = S.tfb_credit_check,
    tfb_hint_ec = S.tfb_hint_ec,
    tfb_invoca_sales_calls = S.tfb_invoca_sales_calls,
    tfb_leads = S.tfb_leads,
    tfb_quality_traffic = S.tfb_quality_traffic,
    total_tfb_conversions = S.total_tfb_conversions,

    file_load_datetime = S.file_load_datetime,
    gold_inserted_at = CURRENT_TIMESTAMP()

  WHEN NOT MATCHED THEN INSERT (
    account_id, campaign_id, date,
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
    total_tfb_conversions,
    file_load_datetime, gold_inserted_at
  )
  VALUES (
    S.account_id, S.campaign_id, S.date,
    S.lob, S.ad_platform, S.account_name, S.campaign_name, S.campaign_type,
    S.advertising_channel_type, S.advertising_channel_sub_type, S.bidding_strategy_type, S.serving_status,
    S.impressions, S.clicks, S.cost, S.all_conversions,
    S.bi, S.buying_intent, S.bts_quality_traffic, S.digital_gross_add, S.magenta_pqt,
    S.cart_start, S.postpaid_cart_start, S.postpaid_pspv, S.aal, S.add_a_line,
    S.connect_low_funnel_prospect, S.connect_low_funnel_visit, S.connect_qt,
    S.hint_ec, S.hint_sec, S.hint_web_orders, S.hint_invoca_calls, S.hint_offline_invoca_calls,
    S.hint_offline_invoca_eligibility, S.hint_offline_invoca_order, S.hint_offline_invoca_order_rt,
    S.hint_offline_invoca_sales_opp, S.ma_hint_ec_eligibility_check,
    S.fiber_activations, S.fiber_pre_order, S.fiber_waitlist_sign_up, S.fiber_web_orders,
    S.fiber_ec, S.fiber_ec_dda, S.fiber_sec, S.fiber_sec_dda,
    S.metro_low_funnel_cs, S.metro_mid_funnel_prospect, S.metro_top_funnel_prospect, S.metro_upper_funnel_prospect,
    S.metro_hint_qt, S.metro_qt,
    S.tmo_prepaid_low_funnel_prospect, S.tmo_top_funnel_prospect, S.tmo_upper_funnel_prospect,
    S.tfb_low_funnel, S.tfb_lead_form_submit, S.tfb_invoca_sales_intent_dda, S.tfb_invoca_order_dda,
    S.tfb_credit_check, S.tfb_hint_ec, S.tfb_invoca_sales_calls, S.tfb_leads, S.tfb_quality_traffic,
    S.total_tfb_conversions,
    S.file_load_datetime, S.gold_inserted_at
  );

END;
