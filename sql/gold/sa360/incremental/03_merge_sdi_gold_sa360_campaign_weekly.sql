/*
===============================================================================
FILE: 04_sp_merge_sdi_gold_sa360_campaign_weekly.sql
LAYER: Gold (Weekly)

PURPOSE:
  Recompute recent qgp_week buckets from Gold Daily and MERGE into Weekly.
  Deletes stale keys in-window so weekly always matches recomputation.

DEFAULT LOOKBACK:
  60 days (safe for quarter-end partial logic)

SPILL:
  Pull (window_start - 6 days) from daily so Sat-week buckets aren’t clipped.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_weekly`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 60;
  DECLARE window_start DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY);

  DECLARE spill_days INT64 DEFAULT 6;
  DECLARE daily_pull_start DATE DEFAULT DATE_SUB(window_start, INTERVAL spill_days DAY);

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly` T
  USING (
    WITH base AS (
      SELECT
        d.*,
        DATE_ADD(DATE_TRUNC(d.date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS week_end_saturday,
        DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY) AS quarter_end_date,
        DATE_SUB(
          DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY),
          INTERVAL MOD(EXTRACT(DAYOFWEEK FROM DATE_SUB(DATE_ADD(DATE_TRUNC(d.date, QUARTER), INTERVAL 3 MONTH), INTERVAL 1 DAY)), 7) DAY
        ) AS last_saturday_before_qe
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily` d
      WHERE d.date >= daily_pull_start
    ),
    bucketed AS (
      SELECT
        *,
        CASE
          WHEN date > last_saturday_before_qe AND date <= quarter_end_date THEN quarter_end_date
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

      -- sums (COALESCE protects weekly totals if a metric is NULL in daily)
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

      SUM(COALESCE(metro_top_funnel_prospect,0)) AS metro_top_funnel_prospect,
      SUM(COALESCE(metro_upper_funnel_prospect,0)) AS metro_upper_funnel_prospect,
      SUM(COALESCE(metro_mid_funnel_prospect,0)) AS metro_mid_funnel_prospect,
      SUM(COALESCE(metro_low_funnel_cs,0)) AS metro_low_funnel_cs,
      SUM(COALESCE(metro_qt,0)) AS metro_qt,
      SUM(COALESCE(metro_hint_qt,0)) AS metro_hint_qt,

      SUM(COALESCE(tmo_top_funnel_prospect,0)) AS tmo_top_funnel_prospect,
      SUM(COALESCE(tmo_upper_funnel_prospect,0)) AS tmo_upper_funnel_prospect,
      SUM(COALESCE(tmo_prepaid_low_funnel_prospect,0)) AS tmo_prepaid_low_funnel_prospect,

      SUM(COALESCE(tfb_credit_check,0)) AS tfb_credit_check,
      SUM(COALESCE(tfb_invoca_sales_calls,0)) AS tfb_invoca_sales_calls,
      SUM(COALESCE(tfb_leads,0)) AS tfb_leads,
      SUM(COALESCE(tfb_quality_traffic,0)) AS tfb_quality_traffic,
      SUM(COALESCE(tfb_hint_ec,0)) AS tfb_hint_ec,
      SUM(COALESCE(total_tfb_conversions,0)) AS total_tfb_conversions,
      SUM(COALESCE(tfb_low_funnel,0)) AS tfb_low_funnel,
      SUM(COALESCE(tfb_lead_form_submit,0)) AS tfb_lead_form_submit,
      SUM(COALESCE(tfb_invoca_sales_intent_dda,0)) AS tfb_invoca_sales_intent_dda,
      SUM(COALESCE(tfb_invoca_order_dda,0)) AS tfb_invoca_order_dda,

      MAX(file_load_datetime) AS file_load_datetime,
      CURRENT_TIMESTAMP() AS gold_inserted_at

    FROM bucketed
    WHERE qgp_week >= window_start
    GROUP BY account_id, campaign_id, qgp_week
  ) S
  ON T.account_id = S.account_id
 AND T.campaign_id = S.campaign_id
 AND T.qgp_week = S.qgp_week

  WHEN MATCHED THEN UPDATE SET
    account_name = S.account_name,
    campaign_name = S.campaign_name,
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
    tmo_prepaid_low_funnel_prospect = S.tmo_prepaid_low_funnel_prospect,
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
    INSERT (
      account_id, account_name, campaign_id, campaign_name, qgp_week,
      lob, ad_platform, campaign_type,
      advertising_channel_type, advertising_channel_sub_type, bidding_strategy_type, serving_status,
      customer_id, customer_name, resource_name, client_manager_id, client_manager_name,
      impressions, clicks, cost, all_conversions,
      bi, buying_intent, bts_quality_traffic, digital_gross_add, magenta_pqt,
      cart_start, postpaid_cart_start, postpaid_pspv, aal, add_a_line,
      connect_low_funnel_prospect, connect_low_funnel_visit, connect_qt,
      hint_ec, hint_sec, hint_web_orders, hint_invoca_calls,
      hint_offline_invoca_calls, hint_offline_invoca_eligibility, hint_offline_invoca_order,
      hint_offline_invoca_order_rt, hint_offline_invoca_sales_opp, ma_hint_ec_eligibility_check,
      fiber_activations, fiber_pre_order, fiber_waitlist_sign_up, fiber_web_orders,
      fiber_ec, fiber_ec_dda, fiber_sec, fiber_sec_dda,
      metro_top_funnel_prospect, metro_upper_funnel_prospect, metro_mid_funnel_prospect,
      metro_low_funnel_cs, metro_qt, metro_hint_qt,
      tmo_top_funnel_prospect, tmo_upper_funnel_prospect, tmo_prepaid_low_funnel_prospect,
      tfb_credit_check, tfb_invoca_sales_calls, tfb_leads, tfb_quality_traffic,
      tfb_hint_ec, total_tfb_conversions, tfb_low_funnel, tfb_lead_form_submit,
      tfb_invoca_sales_intent_dda, tfb_invoca_order_dda,
      file_load_datetime, gold_inserted_at
    )
    VALUES (
      S.account_id, S.account_name, S.campaign_id, S.campaign_name, S.qgp_week,
      S.lob, S.ad_platform, S.campaign_type,
      S.advertising_channel_type, S.advertising_channel_sub_type, S.bidding_strategy_type, S.serving_status,
      S.customer_id, S.customer_name, S.resource_name, S.client_manager_id, S.client_manager_name,
      S.impressions, S.clicks, S.cost, S.all_conversions,
      S.bi, S.buying_intent, S.bts_quality_traffic, S.digital_gross_add, S.magenta_pqt,
      S.cart_start, S.postpaid_cart_start, S.postpaid_pspv, S.aal, S.add_a_line,
      S.connect_low_funnel_prospect, S.connect_low_funnel_visit, S.connect_qt,
      S.hint_ec, S.hint_sec, S.hint_web_orders, S.hint_invoca_calls,
      S.hint_offline_invoca_calls, S.hint_offline_invoca_eligibility, S.hint_offline_invoca_order,
      S.hint_offline_invoca_order_rt, S.hint_offline_invoca_sales_opp, S.ma_hint_ec_eligibility_check,
      S.fiber_activations, S.fiber_pre_order, S.fiber_waitlist_sign_up, S.fiber_web_orders,
      S.fiber_ec, S.fiber_ec_dda, S.fiber_sec, S.fiber_sec_dda,
      S.metro_top_funnel_prospect, S.metro_upper_funnel_prospect, S.metro_mid_funnel_prospect,
      S.metro_low_funnel_cs, S.metro_qt, S.metro_hint_qt,
      S.tmo_top_funnel_prospect, S.tmo_upper_funnel_prospect, S.tmo_prepaid_low_funnel_prospect,
      S.tfb_credit_check, S.tfb_invoca_sales_calls, S.tfb_leads, S.tfb_quality_traffic,
      S.tfb_hint_ec, S.total_tfb_conversions, S.tfb_low_funnel, S.tfb_lead_form_submit,
      S.tfb_invoca_sales_intent_dda, S.tfb_invoca_order_dda,
      S.file_load_datetime, S.gold_inserted_at
    )

  WHEN NOT MATCHED BY SOURCE
    AND T.qgp_week >= window_start
  THEN DELETE;

END;
