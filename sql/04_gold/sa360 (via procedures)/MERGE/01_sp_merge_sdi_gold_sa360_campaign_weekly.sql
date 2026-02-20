/*
===============================================================================
GOLD | SA360 | CAMPAIGN WEEKLY (WIDE)
File Name: 01_sp_merge_sdi_gold_sa360_campaign_weekly.sql
PROC: sp_merge_gold_sa360_campaign_weekly
SOURCE: sdi_gold_sa360_campaign_daily
TARGET: sdi_gold_sa360_campaign_weekly
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_weekly`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 120;

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly` T
  USING (
    WITH daily_base AS (
      SELECT
        *
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
    ),

    -- Defensive dedupe (if Gold Daily is already unique this does nothing)
    daily_dedup AS (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          d.*,
          ROW_NUMBER() OVER (
            PARTITION BY account_id, campaign_id, date
            ORDER BY file_load_datetime DESC
          ) AS rn
        FROM daily_base d
      )
      WHERE rn = 1
    ),

    daily_mapped AS (
      SELECT
        dd.*,
        `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(dd.date) AS qgp_week,

        -- week_end_saturday for the day (used to detect partial)
        DATE_ADD(dd.date, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM dd.date)) DAY) AS week_end_saturday,
        LAST_DAY(dd.date, QUARTER) AS quarter_end
      FROM daily_dedup dd
      WHERE dd.date IS NOT NULL
    ),

    -- Aggregated metrics by week + keys
    weekly_metrics AS (
      SELECT
        account_id,
        campaign_id,
        qgp_week,

        -- If ANY row in this qgp_week group is partial, the group is partial (it will be consistent in practice)
        CASE
          WHEN MAX(CASE WHEN quarter_end < week_end_saturday AND qgp_week = quarter_end THEN 1 ELSE 0 END) = 1
            THEN 'QUARTER_END_PARTIAL'
          ELSE 'WEEKLY'
        END AS period_type,

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
        SUM(COALESCE(total_tfb_conversions,0)) AS total_tfb_conversions,

        MAX(file_load_datetime) AS file_load_datetime
      FROM daily_mapped
      GROUP BY 1,2,3
    ),

    -- Pick the "best" dimension values per week (latest date, then latest file_load_datetime)
    weekly_dims AS (
      SELECT * EXCEPT(rn, week_end_saturday, quarter_end)
      FROM (
        SELECT
          account_id,
          campaign_id,
          qgp_week,

          lob,
          ad_platform,
          account_name,
          campaign_name,
          campaign_type,

          advertising_channel_type,
          advertising_channel_sub_type,
          bidding_strategy_type,
          serving_status,

          file_load_datetime,
          ROW_NUMBER() OVER (
            PARTITION BY account_id, campaign_id, qgp_week
            ORDER BY date DESC, file_load_datetime DESC
          ) AS rn,

          week_end_saturday,
          quarter_end
        FROM daily_mapped
      )
      WHERE rn = 1
    ),

    final AS (
      SELECT
        m.account_id,
        m.campaign_id,
        m.qgp_week,
        FORMAT_DATE('%Y%m%d', m.qgp_week) AS qgp_week_yyyymmdd,
        m.period_type,

        d.lob,
        d.ad_platform,
        d.account_name,
        d.campaign_name,
        d.campaign_type,

        d.advertising_channel_type,
        d.advertising_channel_sub_type,
        d.bidding_strategy_type,
        d.serving_status,

        m.impressions,
        m.clicks,
        m.cost,
        m.all_conversions,

        m.bi,
        m.buying_intent,
        m.bts_quality_traffic,
        m.digital_gross_add,
        m.magenta_pqt,

        m.cart_start,
        m.postpaid_cart_start,
        m.postpaid_pspv,
        m.aal,
        m.add_a_line,

        m.connect_low_funnel_prospect,
        m.connect_low_funnel_visit,
        m.connect_qt,

        m.hint_ec,
        m.hint_sec,
        m.hint_web_orders,
        m.hint_invoca_calls,
        m.hint_offline_invoca_calls,
        m.hint_offline_invoca_eligibility,
        m.hint_offline_invoca_order,
        m.hint_offline_invoca_order_rt,
        m.hint_offline_invoca_sales_opp,
        m.ma_hint_ec_eligibility_check,

        m.fiber_activations,
        m.fiber_pre_order,
        m.fiber_waitlist_sign_up,
        m.fiber_web_orders,
        m.fiber_ec,
        m.fiber_ec_dda,
        m.fiber_sec,
        m.fiber_sec_dda,

        m.metro_low_funnel_cs,
        m.metro_mid_funnel_prospect,
        m.metro_top_funnel_prospect,
        m.metro_upper_funnel_prospect,
        m.metro_hint_qt,
        m.metro_qt,

        m.tmo_prepaid_low_funnel_prospect,
        m.tmo_top_funnel_prospect,
        m.tmo_upper_funnel_prospect,

        m.tfb_low_funnel,
        m.tfb_lead_form_submit,
        m.tfb_invoca_sales_intent_dda,
        m.tfb_invoca_order_dda,
        m.tfb_credit_check,
        m.tfb_hint_ec,
        m.tfb_invoca_sales_calls,
        m.tfb_leads,
        m.tfb_quality_traffic,
        m.total_tfb_conversions,

        m.file_load_datetime,
        CURRENT_TIMESTAMP() AS gold_inserted_at
      FROM weekly_metrics m
      LEFT JOIN weekly_dims d
        USING (account_id, campaign_id, qgp_week)
    )
    SELECT * FROM final
  ) S
  ON  T.account_id  = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.qgp_week    = S.qgp_week

  WHEN MATCHED THEN UPDATE SET
    qgp_week_yyyymmdd = S.qgp_week_yyyymmdd,
    period_type       = S.period_type,

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
    account_id, campaign_id, qgp_week, qgp_week_yyyymmdd, period_type,
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
    S.account_id, S.campaign_id, S.qgp_week, S.qgp_week_yyyymmdd, S.period_type,
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