/*
===============================================================================
FILE: 01_sp_merge_sdi_gold_sa360_campaign_weekly_long.sql
LAYER: Gold
PROC:  sp_merge_gold_sa360_campaign_weekly_long

PURPOSE:
  Build qgp_week that includes:
    - WEEKLY buckets ending on Saturday
    - QUARTER_END_PARTIAL buckets ending on quarter-end date
  Then UNPIVOT into LONG and MERGE.

MERGE KEY:
  (account_id, campaign_id, qgp_week, metric_name)

LEAP YEAR:
  Fully supported by BigQuery date functions; no hardcoding.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_gold_sa360_campaign_weekly_long`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 56;

  DECLARE start_date DATE DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY);
  DECLARE end_date   DATE DEFAULT CURRENT_DATE();

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly_long` T
  USING (
    WITH
    /* ------------------------------------------------------------------------
       QGP CALENDAR: date -> (qgp_week, period_type)
       weekly_qgp_week = Saturday on/after activity_date
       quarter_end     = last day of quarter
       last_sat_qe     = last Saturday on/before quarter_end
       quarter tail    = (last_sat_qe, quarter_end] -> QUARTER_END_PARTIAL
       ------------------------------------------------------------------------ */
    calendar AS (
      SELECT day AS activity_date
      FROM UNNEST(GENERATE_DATE_ARRAY(start_date, end_date)) AS day
    ),
    qgp_calendar AS (
      SELECT
        activity_date,

        -- Saturday week-ending ON/AFTER this date
        DATE_ADD(activity_date, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM activity_date)) DAY) AS weekly_qgp_week,

        -- Quarter end date (leap-year-safe)
        LAST_DAY(activity_date, QUARTER) AS quarter_end
      FROM calendar
    ),
    qgp_calendar_labeled AS (
      SELECT
        activity_date,
        weekly_qgp_week,
        quarter_end,

        -- Last Saturday on/before quarter_end (DAYOFWEEK: Sun=1 ... Sat=7)
        DATE_SUB(quarter_end, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM quarter_end)) DAY) AS last_sat_on_or_before_qe,

        -- Tail length in days: 0 if quarter_end is Saturday; else 1..6
        DATE_DIFF(
          quarter_end,
          DATE_SUB(quarter_end, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM quarter_end)) DAY),
          DAY
        ) AS tail_days
      FROM qgp_calendar
    ),
    qgp_final AS (
      SELECT
        activity_date,
        CASE
          -- if tail_days > 0 and date is in (last_sat, quarter_end] then quarter-end bucket
          WHEN tail_days > 0
           AND activity_date > last_sat_on_or_before_qe
           AND activity_date <= quarter_end
          THEN quarter_end
          ELSE weekly_qgp_week
        END AS qgp_week,
        CASE
          WHEN tail_days > 0
           AND activity_date > last_sat_on_or_before_qe
           AND activity_date <= quarter_end
          THEN 'QUARTER_END_PARTIAL'
          ELSE 'WEEKLY'
        END AS period_type
      FROM qgp_calendar_labeled
    ),

    /* ------------------------------------------------------------------------
       DAILY SOURCE (replace table + date column if needed)
       Must include: date, keys/dims, and metrics
       ------------------------------------------------------------------------ */
    daily AS (
      SELECT
        date AS activity_date,   -- <--- change if your daily date column differs

        account_id,
        campaign_id,

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
        total_tfb_conversions
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
      WHERE date BETWEEN start_date AND end_date
    ),

    /* ------------------------------------------------------------------------
       AGG DAILY -> QGP_WEEK (Saturday weeks + quarter-end partial)
       ------------------------------------------------------------------------ */
    qgp_agg AS (
      SELECT
        d.account_id,
        d.campaign_id,

        q.qgp_week,
        FORMAT_DATE('%Y%m%d', q.qgp_week) AS qgp_week_yyyymmdd,
        q.period_type,

        -- dims: stable within campaign; ANY_VALUE is OK for rollups
        ANY_VALUE(d.lob) AS lob,
        ANY_VALUE(d.ad_platform) AS ad_platform,
        ANY_VALUE(d.account_name) AS account_name,
        ANY_VALUE(d.campaign_name) AS campaign_name,
        ANY_VALUE(d.campaign_type) AS campaign_type,

        ANY_VALUE(d.advertising_channel_type) AS advertising_channel_type,
        ANY_VALUE(d.advertising_channel_sub_type) AS advertising_channel_sub_type,
        ANY_VALUE(d.bidding_strategy_type) AS bidding_strategy_type,
        ANY_VALUE(d.serving_status) AS serving_status,

        -- sums
        SUM(COALESCE(d.impressions,0)) AS impressions,
        SUM(COALESCE(d.clicks,0)) AS clicks,
        SUM(COALESCE(d.cost,0)) AS cost,
        SUM(COALESCE(d.all_conversions,0)) AS all_conversions,

        SUM(COALESCE(d.bi,0)) AS bi,
        SUM(COALESCE(d.buying_intent,0)) AS buying_intent,
        SUM(COALESCE(d.bts_quality_traffic,0)) AS bts_quality_traffic,
        SUM(COALESCE(d.digital_gross_add,0)) AS digital_gross_add,
        SUM(COALESCE(d.magenta_pqt,0)) AS magenta_pqt,

        SUM(COALESCE(d.cart_start,0)) AS cart_start,
        SUM(COALESCE(d.postpaid_cart_start,0)) AS postpaid_cart_start,
        SUM(COALESCE(d.postpaid_pspv,0)) AS postpaid_pspv,
        SUM(COALESCE(d.aal,0)) AS aal,
        SUM(COALESCE(d.add_a_line,0)) AS add_a_line,

        SUM(COALESCE(d.connect_low_funnel_prospect,0)) AS connect_low_funnel_prospect,
        SUM(COALESCE(d.connect_low_funnel_visit,0)) AS connect_low_funnel_visit,
        SUM(COALESCE(d.connect_qt,0)) AS connect_qt,

        SUM(COALESCE(d.hint_ec,0)) AS hint_ec,
        SUM(COALESCE(d.hint_sec,0)) AS hint_sec,
        SUM(COALESCE(d.hint_web_orders,0)) AS hint_web_orders,
        SUM(COALESCE(d.hint_invoca_calls,0)) AS hint_invoca_calls,
        SUM(COALESCE(d.hint_offline_invoca_calls,0)) AS hint_offline_invoca_calls,
        SUM(COALESCE(d.hint_offline_invoca_eligibility,0)) AS hint_offline_invoca_eligibility,
        SUM(COALESCE(d.hint_offline_invoca_order,0)) AS hint_offline_invoca_order,
        SUM(COALESCE(d.hint_offline_invoca_order_rt,0)) AS hint_offline_invoca_order_rt,
        SUM(COALESCE(d.hint_offline_invoca_sales_opp,0)) AS hint_offline_invoca_sales_opp,
        SUM(COALESCE(d.ma_hint_ec_eligibility_check,0)) AS ma_hint_ec_eligibility_check,

        SUM(COALESCE(d.fiber_activations,0)) AS fiber_activations,
        SUM(COALESCE(d.fiber_pre_order,0)) AS fiber_pre_order,
        SUM(COALESCE(d.fiber_waitlist_sign_up,0)) AS fiber_waitlist_sign_up,
        SUM(COALESCE(d.fiber_web_orders,0)) AS fiber_web_orders,
        SUM(COALESCE(d.fiber_ec,0)) AS fiber_ec,
        SUM(COALESCE(d.fiber_ec_dda,0)) AS fiber_ec_dda,
        SUM(COALESCE(d.fiber_sec,0)) AS fiber_sec,
        SUM(COALESCE(d.fiber_sec_dda,0)) AS fiber_sec_dda,

        SUM(COALESCE(d.metro_low_funnel_cs,0)) AS metro_low_funnel_cs,
        SUM(COALESCE(d.metro_mid_funnel_prospect,0)) AS metro_mid_funnel_prospect,
        SUM(COALESCE(d.metro_top_funnel_prospect,0)) AS metro_top_funnel_prospect,
        SUM(COALESCE(d.metro_upper_funnel_prospect,0)) AS metro_upper_funnel_prospect,
        SUM(COALESCE(d.metro_hint_qt,0)) AS metro_hint_qt,
        SUM(COALESCE(d.metro_qt,0)) AS metro_qt,

        SUM(COALESCE(d.tmo_prepaid_low_funnel_prospect,0)) AS tmo_prepaid_low_funnel_prospect,
        SUM(COALESCE(d.tmo_top_funnel_prospect,0)) AS tmo_top_funnel_prospect,
        SUM(COALESCE(d.tmo_upper_funnel_prospect,0)) AS tmo_upper_funnel_prospect,

        SUM(COALESCE(d.tfb_low_funnel,0)) AS tfb_low_funnel,
        SUM(COALESCE(d.tfb_lead_form_submit,0)) AS tfb_lead_form_submit,
        SUM(COALESCE(d.tfb_invoca_sales_intent_dda,0)) AS tfb_invoca_sales_intent_dda,
        SUM(COALESCE(d.tfb_invoca_order_dda,0)) AS tfb_invoca_order_dda,

        SUM(COALESCE(d.tfb_credit_check,0)) AS tfb_credit_check,
        SUM(COALESCE(d.tfb_hint_ec,0)) AS tfb_hint_ec,
        SUM(COALESCE(d.tfb_invoca_sales_calls,0)) AS tfb_invoca_sales_calls,
        SUM(COALESCE(d.tfb_leads,0)) AS tfb_leads,
        SUM(COALESCE(d.tfb_quality_traffic,0)) AS tfb_quality_traffic,
        SUM(COALESCE(d.total_tfb_conversions,0)) AS total_tfb_conversions

      FROM daily d
      JOIN qgp_final q
        ON d.activity_date = q.activity_date
      GROUP BY 1,2,3,4,5
    ),

    /* ------------------------------------------------------------------------
       UNPIVOT -> LONG
       ------------------------------------------------------------------------ */
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
      FROM qgp_agg
      UNPIVOT (
        metric_value FOR metric_name IN (
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
          total_tfb_conversions
        )
      )
    )

    SELECT * FROM longified
  ) S
  ON  T.account_id  = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.qgp_week    = S.qgp_week
  AND T.metric_name = S.metric_name

  WHEN MATCHED THEN UPDATE SET
    qgp_week_yyyymmdd = S.qgp_week_yyyymmdd,
    period_type       = S.period_type,

    lob                       = S.lob,
    ad_platform               = S.ad_platform,
    account_name              = S.account_name,
    campaign_name             = S.campaign_name,
    campaign_type             = S.campaign_type,
    advertising_channel_type  = S.advertising_channel_type,
    advertising_channel_sub_type = S.advertising_channel_sub_type,
    bidding_strategy_type     = S.bidding_strategy_type,
    serving_status            = S.serving_status,

    metric_value              = S.metric_value,
    gold_qgp_long_inserted_at = CURRENT_TIMESTAMP()

  WHEN NOT MATCHED THEN INSERT (
    account_id, campaign_id, qgp_week, qgp_week_yyyymmdd, period_type,
    lob, ad_platform, account_name, campaign_name, campaign_type,
    advertising_channel_type, advertising_channel_sub_type, bidding_strategy_type, serving_status,
    metric_name, metric_value,
    gold_qgp_long_inserted_at
  )
  VALUES (
    S.account_id, S.campaign_id, S.qgp_week, S.qgp_week_yyyymmdd, S.period_type,
    S.lob, S.ad_platform, S.account_name, S.campaign_name, S.campaign_type,
    S.advertising_channel_type, S.advertising_channel_sub_type, S.bidding_strategy_type, S.serving_status,
    S.metric_name, S.metric_value,
    S.gold_qgp_long_inserted_at
  );

END;
