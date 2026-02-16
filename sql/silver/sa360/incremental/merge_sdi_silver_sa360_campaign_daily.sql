/*
===============================================================================
FILE: 01_merge_sdi_silver_sa360_campaign_daily.sql
LAYER: Silver
TABLE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily

PURPOSE:
  Daily incremental MERGE with 7-day lookback to capture late arrivals.

KEY RULES:
  - Grain: account_id + campaign_id + date
  - Lookback window: variable lookback_days = 7
  - Join to latest entity snapshot per account_id+campaign_id by file_load_datetime DESC
  - Derive lob + ad_platform from account_name
  - Idempotent MERGE: no duplicates

IMPORTANT FIX:
  - Entity campaign name column is campaign_name (NOT raw "name")

===============================================================================
*/
BEGIN

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily` T
USING (
  -- ============================================================
  -- STEP 1: Latest Campaign Entity Snapshot (1 row per account_id+campaign_id)
  -- ============================================================
  WITH latest_entity AS (
    SELECT
      account_id,
      campaign_id,
      campaign_name,
      advertising_channel_type,
      advertising_channel_sub_type,
      bidding_strategy_type,
      campaign_status,
      serving_status,
      file_load_datetime
    FROM (
      SELECT
        account_id,
        campaign_id,
        campaign_name,
        advertising_channel_type,
        advertising_channel_sub_type,
        bidding_strategy_type,
        campaign_status,
        serving_status,
        file_load_datetime,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id
          ORDER BY file_load_datetime DESC
        ) AS rn
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    )
    WHERE rn = 1
  ),

  -- ============================================================
  -- STEP 2: Filter Bronze Daily with 7-day lookback
  -- ============================================================
  filtered_daily AS (
    SELECT *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  )

  -- ============================================================
  -- STEP 3: Join Daily + Entity and Derive Business Fields
  -- ============================================================
  SELECT
    -- -------------------------
    -- Grain
    -- -------------------------
    d.account_id,
    d.account_name,
    d.campaign_id,
    e.campaign_name,
    d.date,

    -- -------------------------
    -- Derived LOB (based on your exact mapping list)
    -- -------------------------
    CASE
      WHEN d.account_name IN ('Postpaid Google','Postpaid Bing','BTS Google','BTS Bing') THEN 'Postpaid'
      WHEN d.account_name IN ('Broadband Google','Broadband Bing') THEN 'HSI'
      WHEN d.account_name IN ('Fiber Google','Fiber Bing') THEN 'Fiber'
      WHEN d.account_name IN ('Metro Google','Metro Bing') THEN 'Metro'
      WHEN d.account_name IN ('TFB Google','TFB Bing') THEN 'TFB'
      ELSE 'Unclassified'
    END AS lob,

    -- -------------------------
    -- Derived Ad Platform
    -- -------------------------
    CASE
      WHEN LOWER(d.account_name) LIKE '%google%' THEN 'Google'
      WHEN LOWER(d.account_name) LIKE '%bing%' THEN 'Bing'
      ELSE 'Unknown'
    END AS ad_platform,

    -- -------------------------
    -- Campaign Type (derived from campaign_name)
    -- -------------------------
    CASE
      WHEN e.campaign_name IS NULL THEN 'Unclassified'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'(^|[^a-z])brand([^a-z]|$)') THEN 'Brand'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'(^|[^a-z])generic([^a-z]|$)') THEN 'Generic'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'shopping|shop') THEN 'Shopping'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'pmax|performance\s*max') THEN 'PMax'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'demand\s*gen|demandgen') THEN 'DemandGen'
      ELSE 'Unclassified'
    END AS campaign_type,

    -- -------------------------
    -- Entity Metadata
    -- -------------------------
    e.advertising_channel_type,
    e.advertising_channel_sub_type,
    e.bidding_strategy_type,
    e.campaign_status,
    e.serving_status,

    -- -------------------------
    -- Optional business attrs
    -- -------------------------
    d.customer_id,
    d.customer_name,
    d.client_manager_id,
    d.client_manager_name,
    d.resource_name,

    -- -------------------------
    -- CORE METRICS (MANDATORY)
    -- -------------------------
    d.impressions,
    d.clicks,
    d.cost,
    d.all_conversions,

    -- -------------------------
    -- QUALITY / INTENT
    -- -------------------------
    d.bi,
    d.buying_intent,
    d.bts_quality_traffic,
    d.digital_gross_add,
    d.magenta_pqt,

    -- -------------------------
    -- CART START + POSTPAID / PSPV
    -- -------------------------
    d.cart_start,
    d.postpaid_cart_start,
    d.postpaid_pspv,
    d.aal,
    d.add_a_line,

    -- -------------------------
    -- CONNECT
    -- -------------------------
    d.connect_low_funnel_visit,
    d.connect_low_funnel_prospect,
    d.connect_qt,

    -- -------------------------
    -- HINT / HSI
    -- -------------------------
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

    -- -------------------------
    -- FIBER
    -- -------------------------
    d.fiber_activations,
    d.fiber_pre_order,
    d.fiber_waitlist_sign_up,
    d.fiber_web_orders,
    d.fiber_ec,
    d.fiber_ec_dda,
    d.fiber_sec,
    d.fiber_sec_dda,

    -- -------------------------
    -- METRO
    -- -------------------------
    d.metro_top_funnel_prospect,
    d.metro_upper_funnel_prospect,
    d.metro_mid_funnel_prospect,
    d.metro_low_funnel_cs,
    d.metro_qt,
    d.metro_hint_qt,

    -- -------------------------
    -- TMO FUNNEL
    -- -------------------------
    d.tmo_top_funnel_prospect,
    d.tmo_upper_funnel_prospect,
    d.tmo_prepaid_low_funnel_prospect,

    -- -------------------------
    -- TFB + TBG→TFB (already standardized in Bronze)
    -- -------------------------
    d.tfb_credit_check,
    d.tfb_invoca_sales_calls,
    d.tfb_leads,
    d.tfb_quality_traffic,
    d.tfb_hint_ec,
    d.total_tfb_conversions,
    d.tfb_low_funnel,
    d.tfb_lead_form_submit,
    d.tfb_invoca_sales_intent_dda,
    d.tfb_invoca_order_dda,

    -- -------------------------
    -- METADATA
    -- -------------------------
    d.file_load_datetime,
    CURRENT_TIMESTAMP() AS silver_inserted_at

  FROM filtered_daily d
  LEFT JOIN latest_entity e
    ON d.account_id = e.account_id
   AND d.campaign_id = e.campaign_id
) S
ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date = S.date

WHEN MATCHED THEN
  UPDATE SET
    account_name = S.account_name,
    campaign_name = S.campaign_name,
    lob = S.lob,
    ad_platform = S.ad_platform,

    campaign_type = S.campaign_type,
    advertising_channel_type = S.advertising_channel_type,
    advertising_channel_sub_type = S.advertising_channel_sub_type,
    bidding_strategy_type = S.bidding_strategy_type,
    campaign_status = S.campaign_status,
    serving_status = S.serving_status,

    customer_id = S.customer_id,
    customer_name = S.customer_name,
    client_manager_id = S.client_manager_id,
    client_manager_name = S.client_manager_name,
    resource_name = S.resource_name,

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

    connect_low_funnel_visit = S.connect_low_funnel_visit,
    connect_low_funnel_prospect = S.connect_low_funnel_prospect,
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
    silver_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT ROW;

END;
