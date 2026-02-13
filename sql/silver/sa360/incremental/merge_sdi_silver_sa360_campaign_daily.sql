/*
===============================================================================
FILE: merge_sdi_silver_sa360_campaign_daily.sql
LAYER: Silver
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Incrementally build Silver Campaign Daily table using Bronze Daily
  and latest Campaign Entity snapshot.

GRAIN:
  account_id + campaign_id + date

DESIGN:
  - Explicit column mapping
  - Controlled updates
  - Partition-aware incremental load
  - Idempotent MERGE
  - Enterprise production safe

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

-- ============================================================================
-- STEP 1: Latest Campaign Entity Snapshot
-- ============================================================================

WITH latest_entity AS (

  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, campaign_id
        ORDER BY file_load_datetime DESC
      ) AS rn
    FROM
      `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
  )
  WHERE rn = 1

),

-- ============================================================================
-- STEP 2: Filter Bronze Daily for Incremental Window
-- ============================================================================

filtered_daily AS (

  SELECT *
  FROM
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
  WHERE
    date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)

),

-- ============================================================================
-- STEP 3: Build Silver Dataset
-- ============================================================================

silver_source AS (

  SELECT

    d.account_id,
    d.account_name,
    d.campaign_id,
    e.name AS campaign_name,
    d.date,

    -- Campaign Classification
    CASE
      WHEN LOWER(e.name) LIKE '%brand%' THEN 'Brand'
      WHEN LOWER(e.name) LIKE '%generic%' THEN 'Generic'
      WHEN LOWER(e.name) LIKE '%shopping%' THEN 'Shopping'
      WHEN LOWER(e.name) LIKE '%shop%' THEN 'Shopping'
      WHEN LOWER(e.name) LIKE '%pmax%' THEN 'PMax'
      WHEN LOWER(e.name) LIKE '%demandgen%' THEN 'DemandGen'
      ELSE 'Unclassified'
    END AS campaign_type,

    e.advertising_channel_type,
    e.advertising_channel_sub_type,
    e.bidding_strategy_type,
    e.status AS campaign_status,
    e.serving_status,

    d.impressions,
    d.clicks,
    d.cost,
    d.all_conversions,

    d.postpaid_cart_start,
    d.postpaid_pspv,
    d.aal,
    d.add_a_line,

    d.hint_ec,
    d.hint_sec,
    d.hint_web_orders,
    d.hint_invoca_calls,
    d.hint_offline_invoca_calls,
    d.hint_offline_invoca_eligibility,
    d.hint_offline_invoca_order,
    d.hint_offline_invoca_order_rt,
    d.hint_offline_invoca_sales_opp,
    d.ma_hint_ec_eligibility_check,

    d.fiber_activations,
    d.fiber_pre_order,
    d.fiber_waitlist_sign_up,
    d.fiber_web_orders,
    d.fiber_ec,
    d.fiber_ec_dda,
    d.fiber_sec,
    d.fiber_sec_dda,

    d.metro_top_funnel_prospect,
    d.metro_upper_funnel_prospect,
    d.metro_mid_funnel_prospect,
    d.metro_low_funnel_cs,
    d.metro_qt,
    d.metro_hint_qt,

    d.tmo_top_funnel_prospect,
    d.tmo_upper_funnel_prospect,
    d.tmo_prepaid_low_funnel_prospect,

    d.tfb_credit_check,
    d.tfb_invoca_sales_calls,
    d.tfb_leads,
    d.tfb_quality_traffic,
    d.tfb_hint_ec,
    d.total_tfb_conversions,

    d.magenta_pqt,

    d.file_load_datetime,
    CURRENT_TIMESTAMP() AS silver_inserted_at

  FROM filtered_daily d
  LEFT JOIN latest_entity e
    ON d.account_id = e.account_id
   AND d.campaign_id = e.campaign_id

)

-- ============================================================================
-- STEP 4: MERGE INTO SILVER
-- ============================================================================

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily` T
USING silver_source S

ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date = S.date

-- ============================================================================
-- WHEN MATCHED
-- ============================================================================

WHEN MATCHED THEN
  UPDATE SET

    account_name = S.account_name,
    campaign_name = S.campaign_name,
    campaign_type = S.campaign_type,
    advertising_channel_type = S.advertising_channel_type,
    advertising_channel_sub_type = S.advertising_channel_sub_type,
    bidding_strategy_type = S.bidding_strategy_type,
    campaign_status = S.campaign_status,
    serving_status = S.serving_status,

    impressions = S.impressions,
    clicks = S.clicks,
    cost = S.cost,
    all_conversions = S.all_conversions,

    postpaid_cart_start = S.postpaid_cart_start,
    postpaid_pspv = S.postpaid_pspv,
    aal = S.aal,
    add_a_line = S.add_a_line,

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

    magenta_pqt = S.magenta_pqt,

    file_load_datetime = S.file_load_datetime,
    silver_inserted_at = CURRENT_TIMESTAMP()

-- ============================================================================
-- WHEN NOT MATCHED
-- ============================================================================

WHEN NOT MATCHED THEN
  INSERT (
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    date,
    campaign_type,
    advertising_channel_type,
    advertising_channel_sub_type,
    bidding_strategy_type,
    campaign_status,
    serving_status,
    impressions,
    clicks,
    cost,
    all_conversions,
    postpaid_cart_start,
    postpaid_pspv,
    aal,
    add_a_line,
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
    metro_top_funnel_prospect,
    metro_upper_funnel_prospect,
    metro_mid_funnel_prospect,
    metro_low_funnel_cs,
    metro_qt,
    metro_hint_qt,
    tmo_top_funnel_prospect,
    tmo_upper_funnel_prospect,
    tmo_prepaid_low_funnel_prospect,
    tfb_credit_check,
    tfb_invoca_sales_calls,
    tfb_leads,
    tfb_quality_traffic,
    tfb_hint_ec,
    total_tfb_conversions,
    magenta_pqt,
    file_load_datetime,
    silver_inserted_at
  )
  VALUES (
    S.account_id,
    S.account_name,
    S.campaign_id,
    S.campaign_name,
    S.date,
    S.campaign_type,
    S.advertising_channel_type,
    S.advertising_channel_sub_type,
    S.bidding_strategy_type,
    S.campaign_status,
    S.serving_status,
    S.impressions,
    S.clicks,
    S.cost,
    S.all_conversions,
    S.postpaid_cart_start,
    S.postpaid_pspv,
    S.aal,
    S.add_a_line,
    S.hint_ec,
    S.hint_sec,
    S.hint_web_orders,
    S.hint_invoca_calls,
    S.hint_offline_invoca_calls,
    S.hint_offline_invoca_eligibility,
    S.hint_offline_invoca_order,
    S.hint_offline_invoca_order_rt,
    S.hint_offline_invoca_sales_opp,
    S.ma_hint_ec_eligibility_check,
    S.fiber_activations,
    S.fiber_pre_order,
    S.fiber_waitlist_sign_up,
    S.fiber_web_orders,
    S.fiber_ec,
    S.fiber_ec_dda,
    S.fiber_sec,
    S.fiber_sec_dda,
    S.metro_top_funnel_prospect,
    S.metro_upper_funnel_prospect,
    S.metro_mid_funnel_prospect,
    S.metro_low_funnel_cs,
    S.metro_qt,
    S.metro_hint_qt,
    S.tmo_top_funnel_prospect,
    S.tmo_upper_funnel_prospect,
    S.tmo_prepaid_low_funnel_prospect,
    S.tfb_credit_check,
    S.tfb_invoca_sales_calls,
    S.tfb_leads,
    S.tfb_quality_traffic,
    S.tfb_hint_ec,
    S.total_tfb_conversions,
    S.magenta_pqt,
    S.file_load_datetime,
    S.silver_inserted_at
  );
