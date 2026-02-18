/*
===============================================================================
FILE: 01_sp_merge_sdi_gold_sa360_campaign_weekly.sql
LAYER: Gold
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_weekly

PURPOSE:
  Rebuild / upsert QGP-week aggregates from Gold Daily within lookback window.

QGP WEEK LOGIC:
  - week_end_sat = DATE_TRUNC(date, WEEK(SATURDAY))
  - quarter_end  = last day of quarter for `date`
  - if quarter_end occurs before that Saturday:
      * dates <= quarter_end bucket to qgp_week = quarter_end (QUARTER_END_PARTIAL)
      * dates after quarter_end bucket to qgp_week = week_end_sat (WEEKLY)
    else:
      * bucket to qgp_week = week_end_sat (WEEKLY)

MERGE KEY:
  (account_id, campaign_id, qgp_week)

RECONCILIATION:
  Period metrics = SUM(daily metrics) over that qgp_week bucket.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_weekly`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 56;

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly` T
  USING (
    WITH base AS (
      SELECT
        account_id,
        campaign_id,
        date,

        -- Normal week end (Saturday)
        DATE_TRUNC(date, WEEK(SATURDAY)) AS week_end_sat,

        -- Quarter end (last day of quarter)
        DATE_SUB(
          DATE_ADD(DATE_TRUNC(date, QUARTER), INTERVAL 3 MONTH),
          INTERVAL 1 DAY
        ) AS quarter_end,

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
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
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

    agg AS (
      SELECT
        account_id,
        campaign_id,
        qgp_week,
        FORMAT_DATE('%Y%m%d', qgp_week) AS qgp_week_yyyymmdd,
        period_type,

        -- Latest-in-period dims/settings (non-null preferred)
        ARRAY_AGG(lob IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS lob,
        ARRAY_AGG(ad_platform IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS ad_platform,
        ARRAY_AGG(account_name IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS account_name,
        ARRAY_AGG(campaign_name IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS campaign_name,
        ARRAY_AGG(campaign_type IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS campaign_type,

        ARRAY_AGG(advertising_channel_type IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS advertising_channel_type,
        ARRAY_AGG(advertising_channel_sub_type IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS advertising_channel_sub_type,
        ARRAY_AGG(bidding_strategy_type IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS bidding_strategy_type,
        ARRAY_AGG(serving_status IGNORE NULLS ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS serving_status,

        -- Sums (reconciliation-safe)
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

        SUM(metro_low_funnel_cs) AS metro_low_funnel_cs,
        SUM(metro_mid_funnel_prospect) AS metro_mid_funnel_prospect,
        SUM(metro_top_funnel_prospect) AS metro_top_funnel_prospect,
        SUM(metro_upper_funnel_prospect) AS metro_upper_funnel_prospect,
        SUM(metro_hint_qt) AS metro_hint_qt,
        SUM(metro_qt) AS metro_qt,

        SUM(tmo_prepaid_low_funnel_prospect) AS tmo_prepaid_low_funnel_prospect,
        SUM(tmo_top_funnel_prospect) AS tmo_top_funnel_prospect,
        SUM(tmo_upper_funnel_prospect) AS tmo_upper_funnel_prospect,

        SUM(tfb_low_funnel) AS tfb_low_funnel,
        SUM(tfb_lead_form_submit) AS tfb_lead_form_submit,
        SUM(tfb_invoca_sales_intent_dda) AS tfb_invoca_sales_intent_dda,
        SUM(tfb_invoca_order_dda) AS tfb_invoca_order_dda,

        SUM(tfb_credit_check) AS tfb_credit_check,
        SUM(tfb_hint_ec) AS tfb_hint_ec,
        SUM(tfb_invoca_sales_calls) AS tfb_invoca_sales_calls,
        SUM(tfb_leads) AS tfb_leads,
        SUM(tfb_quality_traffic) AS tfb_quality_traffic,
        SUM(total_tfb_conversions) AS total_tfb_conversions,

        CURRENT_TIMESTAMP() AS gold_weekly_inserted_at
      FROM bucketed
      GROUP BY account_id, campaign_id, qgp_week, qgp_week_yyyymmdd, period_type
    )

    SELECT * FROM agg
  ) S
  ON  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.qgp_week = S.qgp_week

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

    gold_weekly_inserted_at = CURRENT_TIMESTAMP()

  WHEN NOT MATCHED THEN INSERT ROW;

END;
