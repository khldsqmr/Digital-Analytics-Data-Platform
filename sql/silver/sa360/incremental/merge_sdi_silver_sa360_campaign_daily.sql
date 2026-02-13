/*
===============================================================================
SILVER | SA 360 | CAMPAIGN DAILY | INCREMENTAL MERGE
===============================================================================

GRAIN
  account_id + campaign_id + date

SOURCE
  - sdi_bronze_sa360_campaign_daily
  - sdi_bronze_sa360_campaign_entity

LOGIC
  - 7-day lookback for late-arriving Bronze data
  - Uses latest campaign entity snapshot
  - Idempotent MERGE
  - Safe for daily scheduling

TARGET TABLE
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily

===============================================================================
*/
BEGIN

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily` T

USING (

  -- ============================================================
  -- STEP 1: Latest Campaign Entity Snapshot
  -- ============================================================

  WITH latest_entity AS (

    SELECT *
    FROM (
      SELECT
        account_id,
        campaign_id,
        name,
        advertising_channel_type,
        advertising_channel_sub_type,
        bidding_strategy_type,
        status,
        serving_status,
        file_load_datetime,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id
          ORDER BY file_load_datetime DESC
        ) AS rn
      FROM
      `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    )
    WHERE rn = 1
  ),

  -- ============================================================
  -- STEP 2: Filter Bronze Daily for Lookback Window
  -- ============================================================

  filtered_daily AS (
    SELECT *
    FROM
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE
      date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  )

  -- ============================================================
  -- STEP 3: Join & Transform
  -- ============================================================

  SELECT

    d.account_id,
    d.account_name,
    d.campaign_id,
    e.name AS campaign_name,
    d.date,

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

) S

-- ============================================================
-- MATCH CONDITION (Silver Grain)
-- ============================================================

ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date = S.date

-- ============================================================
-- UPDATE LOGIC
-- ============================================================

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
    file_load_datetime = S.file_load_datetime,
    silver_inserted_at = CURRENT_TIMESTAMP()

-- ============================================================
-- INSERT LOGIC
-- ============================================================

WHEN NOT MATCHED THEN
  INSERT ROW;

END;
