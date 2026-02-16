/*
===============================================================================
FILE: 01_sp_silver_sa360_campaign_daily_incremental.sql
LAYER: Silver
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Incremental daily MERGE to keep Silver table up to date.

GRAIN:
  account_id + campaign_id + date

SOURCE:
  - sdi_bronze_sa360_campaign_daily
  - sdi_bronze_sa360_campaign_entity (latest snapshot per account_id + campaign_id)

KEY REQUIREMENTS YOU ASKED FOR:
  - 7-day lookback window variable (controls cost)
  - No duplicates in Silver (idempotent MERGE)
  - Uses correct column names (no insert_date mistakes; uses __insert_date only if needed)
  - Uses date derived in Bronze as "date" (DATE type)

SCHEDULING:
  Schedule daily. Lookback allows late-arriving rows/updates.

===============================================================================
*/
BEGIN

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily` T
USING (

  -- ============================================================
  -- STEP 1: Latest Campaign Entity Snapshot (per account_id + campaign_id)
  --         Pick the newest File_Load_datetime.
  -- ============================================================
  WITH latest_entity AS (
    SELECT
      account_id,
      campaign_id,
      name AS campaign_name,
      advertising_channel_type,
      advertising_channel_sub_type,
      bidding_strategy_type,
      status AS campaign_status,
      serving_status
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
        File_Load_datetime,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id
          ORDER BY File_Load_datetime DESC
        ) AS rn
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
    )
    WHERE rn = 1
  ),

  -- ============================================================
  -- STEP 2: Filter Bronze Daily (lookback window)
  -- ============================================================
  filtered_daily AS (
    SELECT
      *
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  )

  -- ============================================================
  -- STEP 3: Join & Produce Silver-shaped rows (all columns)
  -- ============================================================
  SELECT
    -- Grain
    d.account_id,
    d.account_name,
    d.campaign_id,
    e.campaign_name,
    d.date,

    -- Business dimensions
    d.customer_id,
    d.customer_name,
    d.client_manager_id,
    d.client_manager_name,
    d.resource_name,

    -- Classification
    CASE
      WHEN e.campaign_name IS NULL THEN 'Unclassified'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'(^|[^a-z])brand([^a-z]|$)') THEN 'Brand'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'(^|[^a-z])generic([^a-z]|$)') THEN 'Generic'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'shopping|shop') THEN 'Shopping'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'pmax|performance\s*max') THEN 'PMax'
      WHEN REGEXP_CONTAINS(LOWER(e.campaign_name), r'demand\s*gen|demandgen') THEN 'DemandGen'
      ELSE 'Unclassified'
    END AS campaign_type,

    e.advertising_channel_type,
    e.advertising_channel_sub_type,
    e.bidding_strategy_type,
    e.campaign_status,
    e.serving_status,

    -- Core metrics
    d.impressions,
    d.clicks,
    d.cost,
    d.all_conversions,

    -- Intent / quality / generic business metrics
    d.bi,
    d.buying_intent,
    d.bts_quality_traffic,
    d.digital_gross_add,
    d.cart_start,

    d.connect_low_funnel_visit,
    d.connect_low_funnel_prospect,
    d.connect_qt,

    -- Postpaid
    d.postpaid_cart_start,
    d.postpaid_pspv,
    d.aal,
    d.add_a_line,

    -- HINT
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

    -- Fiber
    d.fiber_activations,
    d.fiber_pre_order,
    d.fiber_waitlist_sign_up,
    d.fiber_web_orders,
    d.fiber_ec,
    d.fiber_ec_dda,
    d.fiber_sec,
    d.fiber_sec_dda,

    -- Metro
    d.metro_top_funnel_prospect,
    d.metro_upper_funnel_prospect,
    d.metro_mid_funnel_prospect,
    d.metro_low_funnel_cs,
    d.metro_qt,
    d.metro_hint_qt,

    -- TMO
    d.tmo_top_funnel_prospect,
    d.tmo_upper_funnel_prospect,
    d.tmo_prepaid_low_funnel_prospect,

    -- TFB canonical (includes TBG mapping)
    d.tfb_credit_check,
    d.tfb_invoca_sales_calls,
    d.tfb_leads,
    d.tfb_quality_traffic,
    d.tfb_hint_ec,

    -- Canonical fields from TBG family in the campaign table
    d.tfb_low_funnel,
    d.tfb_lead_form_submit,
    d.tfb_invoca_sales_intent_dda,
    d.tfb_invoca_order_dda,

    d.total_tfb_conversions,

    -- Other
    d.magenta_pqt,

    -- Metadata
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
    -- Grain-adjacent
    account_name = S.account_name,
    campaign_name = S.campaign_name,
    customer_id = S.customer_id,
    customer_name = S.customer_name,
    client_manager_id = S.client_manager_id,
    client_manager_name = S.client_manager_name,
    resource_name = S.resource_name,

    -- Classification
    campaign_type = S.campaign_type,
    advertising_channel_type = S.advertising_channel_type,
    advertising_channel_sub_type = S.advertising_channel_sub_type,
    bidding_strategy_type = S.bidding_strategy_type,
    campaign_status = S.campaign_status,
    serving_status = S.serving_status,

    -- Core metrics
    impressions = S.impressions,
    clicks = S.clicks,
    cost = S.cost,
    all_conversions = S.all_conversions,

    -- Intent/quality
    bi = S.bi,
    buying_intent = S.buying_intent,
    bts_quality_traffic = S.bts_quality_traffic,
    digital_gross_add = S.digital_gross_add,
    cart_start = S.cart_start,
    connect_low_funnel_visit = S.connect_low_funnel_visit,
    connect_low_funnel_prospect = S.connect_low_funnel_prospect,
    connect_qt = S.connect_qt,

    -- Postpaid
    postpaid_cart_start = S.postpaid_cart_start,
    postpaid_pspv = S.postpaid_pspv,
    aal = S.aal,
    add_a_line = S.add_a_line,

    -- HINT
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

    -- Fiber
    fiber_activations = S.fiber_activations,
    fiber_pre_order = S.fiber_pre_order,
    fiber_waitlist_sign_up = S.fiber_waitlist_sign_up,
    fiber_web_orders = S.fiber_web_orders,
    fiber_ec = S.fiber_ec,
    fiber_ec_dda = S.fiber_ec_dda,
    fiber_sec = S.fiber_sec,
    fiber_sec_dda = S.fiber_sec_dda,

    -- Metro
    metro_top_funnel_prospect = S.metro_top_funnel_prospect,
    metro_upper_funnel_prospect = S.metro_upper_funnel_prospect,
    metro_mid_funnel_prospect = S.metro_mid_funnel_prospect,
    metro_low_funnel_cs = S.metro_low_funnel_cs,
    metro_qt = S.metro_qt,
    metro_hint_qt = S.metro_hint_qt,

    -- TMO
    tmo_top_funnel_prospect = S.tmo_top_funnel_prospect,
    tmo_upper_funnel_prospect = S.tmo_upper_funnel_prospect,
    tmo_prepaid_low_funnel_prospect = S.tmo_prepaid_low_funnel_prospect,

    -- TFB canonical
    tfb_credit_check = S.tfb_credit_check,
    tfb_invoca_sales_calls = S.tfb_invoca_sales_calls,
    tfb_leads = S.tfb_leads,
    tfb_quality_traffic = S.tfb_quality_traffic,
    tfb_hint_ec = S.tfb_hint_ec,
    tfb_low_funnel = S.tfb_low_funnel,
    tfb_lead_form_submit = S.tfb_lead_form_submit,
    tfb_invoca_sales_intent_dda = S.tfb_invoca_sales_intent_dda,
    tfb_invoca_order_dda = S.tfb_invoca_order_dda,
    total_tfb_conversions = S.total_tfb_conversions,

    -- Other
    magenta_pqt = S.magenta_pqt,

    -- Metadata
    file_load_datetime = S.file_load_datetime,
    silver_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    account_id,
    account_name,
    campaign_id,
    campaign_name,
    date,

    customer_id,
    customer_name,
    client_manager_id,
    client_manager_name,
    resource_name,

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

    bi,
    buying_intent,
    bts_quality_traffic,
    digital_gross_add,
    cart_start,
    connect_low_funnel_visit,
    connect_low_funnel_prospect,
    connect_qt,

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
    tfb_low_funnel,
    tfb_lead_form_submit,
    tfb_invoca_sales_intent_dda,
    tfb_invoca_order_dda,
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

    S.customer_id,
    S.customer_name,
    S.client_manager_id,
    S.client_manager_name,
    S.resource_name,

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

    S.bi,
    S.buying_intent,
    S.bts_quality_traffic,
    S.digital_gross_add,
    S.cart_start,
    S.connect_low_funnel_visit,
    S.connect_low_funnel_prospect,
    S.connect_qt,

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
    S.tfb_low_funnel,
    S.tfb_lead_form_submit,
    S.tfb_invoca_sales_intent_dda,
    S.tfb_invoca_order_dda,
    S.total_tfb_conversions,

    S.magenta_pqt,

    S.file_load_datetime,
    S.silver_inserted_at
  );

END;
