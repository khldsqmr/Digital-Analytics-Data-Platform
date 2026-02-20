/*
===============================================================================
FILE: 00_backfill_gold_sa360_campaign_daily.sql
LAYER: Gold (One-time Backfill)
TARGET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily
SOURCE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily
NOTES:
  - Deduped per (account_id, campaign_id, date) using latest file_load_datetime
  - SAFE_CAST for schema safety
===============================================================================
*/

DECLARE backfill_start_date DATE DEFAULT DATE('2024-01-01');
DECLARE backfill_end_date   DATE DEFAULT CURRENT_DATE();
DECLARE chunk_days          INT64 DEFAULT 14;

DECLARE chunk_start DATE;
DECLARE chunk_end   DATE;

SET chunk_start = backfill_start_date;

LOOP
  IF chunk_start > backfill_end_date THEN
    LEAVE;
  END IF;

  SET chunk_end = LEAST(DATE_ADD(chunk_start, INTERVAL chunk_days - 1 DAY), backfill_end_date);

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily` T
  USING (
    WITH base AS (
      SELECT
        SAFE_CAST(account_id AS STRING)  AS account_id,
        SAFE_CAST(campaign_id AS STRING) AS campaign_id,
        SAFE_CAST(date AS DATE)          AS date,

        SAFE_CAST(lob AS STRING)         AS lob,
        SAFE_CAST(ad_platform AS STRING) AS ad_platform,
        SAFE_CAST(account_name AS STRING)  AS account_name,
        SAFE_CAST(campaign_name AS STRING) AS campaign_name,
        SAFE_CAST(campaign_type AS STRING) AS campaign_type,

        SAFE_CAST(advertising_channel_type AS STRING)     AS advertising_channel_type,
        SAFE_CAST(advertising_channel_sub_type AS STRING) AS advertising_channel_sub_type,
        SAFE_CAST(bidding_strategy_type AS STRING)        AS bidding_strategy_type,
        SAFE_CAST(serving_status AS STRING)               AS serving_status,

        SAFE_CAST(impressions AS FLOAT64)     AS impressions,
        SAFE_CAST(clicks AS FLOAT64)          AS clicks,
        SAFE_CAST(cost AS FLOAT64)            AS cost,
        SAFE_CAST(all_conversions AS FLOAT64) AS all_conversions,

        SAFE_CAST(bi AS FLOAT64)                AS bi,
        SAFE_CAST(buying_intent AS FLOAT64)     AS buying_intent,
        SAFE_CAST(bts_quality_traffic AS FLOAT64) AS bts_quality_traffic,
        SAFE_CAST(digital_gross_add AS FLOAT64) AS digital_gross_add,
        SAFE_CAST(magenta_pqt AS FLOAT64)       AS magenta_pqt,

        SAFE_CAST(cart_start AS FLOAT64)         AS cart_start,
        SAFE_CAST(postpaid_cart_start AS FLOAT64) AS postpaid_cart_start,
        SAFE_CAST(postpaid_pspv AS FLOAT64)      AS postpaid_pspv,
        SAFE_CAST(aal AS FLOAT64)                AS aal,
        SAFE_CAST(add_a_line AS FLOAT64)         AS add_a_line,

        SAFE_CAST(connect_low_funnel_prospect AS FLOAT64) AS connect_low_funnel_prospect,
        SAFE_CAST(connect_low_funnel_visit AS FLOAT64)    AS connect_low_funnel_visit,
        SAFE_CAST(connect_qt AS FLOAT64)                  AS connect_qt,

        SAFE_CAST(hint_ec AS FLOAT64)                     AS hint_ec,
        SAFE_CAST(hint_sec AS FLOAT64)                    AS hint_sec,
        SAFE_CAST(hint_web_orders AS FLOAT64)             AS hint_web_orders,
        SAFE_CAST(hint_invoca_calls AS FLOAT64)           AS hint_invoca_calls,
        SAFE_CAST(hint_offline_invoca_calls AS FLOAT64)   AS hint_offline_invoca_calls,
        SAFE_CAST(hint_offline_invoca_eligibility AS FLOAT64) AS hint_offline_invoca_eligibility,
        SAFE_CAST(hint_offline_invoca_order AS FLOAT64)   AS hint_offline_invoca_order,
        SAFE_CAST(hint_offline_invoca_order_rt AS FLOAT64) AS hint_offline_invoca_order_rt,
        SAFE_CAST(hint_offline_invoca_sales_opp AS FLOAT64) AS hint_offline_invoca_sales_opp,
        SAFE_CAST(ma_hint_ec_eligibility_check AS FLOAT64) AS ma_hint_ec_eligibility_check,

        SAFE_CAST(fiber_activations AS FLOAT64)     AS fiber_activations,
        SAFE_CAST(fiber_pre_order AS FLOAT64)       AS fiber_pre_order,
        SAFE_CAST(fiber_waitlist_sign_up AS FLOAT64) AS fiber_waitlist_sign_up,
        SAFE_CAST(fiber_web_orders AS FLOAT64)      AS fiber_web_orders,
        SAFE_CAST(fiber_ec AS FLOAT64)              AS fiber_ec,
        SAFE_CAST(fiber_ec_dda AS FLOAT64)          AS fiber_ec_dda,
        SAFE_CAST(fiber_sec AS FLOAT64)             AS fiber_sec,
        SAFE_CAST(fiber_sec_dda AS FLOAT64)         AS fiber_sec_dda,

        SAFE_CAST(metro_low_funnel_cs AS FLOAT64)       AS metro_low_funnel_cs,
        SAFE_CAST(metro_mid_funnel_prospect AS FLOAT64) AS metro_mid_funnel_prospect,
        SAFE_CAST(metro_top_funnel_prospect AS FLOAT64) AS metro_top_funnel_prospect,
        SAFE_CAST(metro_upper_funnel_prospect AS FLOAT64) AS metro_upper_funnel_prospect,
        SAFE_CAST(metro_hint_qt AS FLOAT64)             AS metro_hint_qt,
        SAFE_CAST(metro_qt AS FLOAT64)                  AS metro_qt,

        SAFE_CAST(tmo_prepaid_low_funnel_prospect AS FLOAT64) AS tmo_prepaid_low_funnel_prospect,
        SAFE_CAST(tmo_top_funnel_prospect AS FLOAT64)         AS tmo_top_funnel_prospect,
        SAFE_CAST(tmo_upper_funnel_prospect AS FLOAT64)       AS tmo_upper_funnel_prospect,

        SAFE_CAST(tfb_low_funnel AS FLOAT64)            AS tfb_low_funnel,
        SAFE_CAST(tfb_lead_form_submit AS FLOAT64)      AS tfb_lead_form_submit,
        SAFE_CAST(tfb_invoca_sales_intent_dda AS FLOAT64) AS tfb_invoca_sales_intent_dda,
        SAFE_CAST(tfb_invoca_order_dda AS FLOAT64)      AS tfb_invoca_order_dda,

        SAFE_CAST(tfb_credit_check AS FLOAT64)          AS tfb_credit_check,
        SAFE_CAST(tfb_hint_ec AS FLOAT64)               AS tfb_hint_ec,
        SAFE_CAST(tfb_invoca_sales_calls AS FLOAT64)    AS tfb_invoca_sales_calls,
        SAFE_CAST(tfb_leads AS FLOAT64)                 AS tfb_leads,
        SAFE_CAST(tfb_quality_traffic AS FLOAT64)       AS tfb_quality_traffic,
        SAFE_CAST(total_tfb_conversions AS FLOAT64)     AS total_tfb_conversions,

        SAFE_CAST(file_load_datetime AS DATETIME) AS file_load_datetime
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
      WHERE date BETWEEN chunk_start AND chunk_end
    ),
    dedup AS (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          b.*,
          ROW_NUMBER() OVER (
            PARTITION BY account_id, campaign_id, date
            ORDER BY file_load_datetime DESC
          ) AS rn
        FROM base b
      )
      WHERE rn = 1
    )
    SELECT
      d.*,
      CURRENT_TIMESTAMP() AS gold_inserted_at
    FROM dedup d
  ) S
  ON  T.account_id  = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date        = S.date

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

  SET chunk_start = DATE_ADD(chunk_end, INTERVAL 1 DAY);
END LOOP;