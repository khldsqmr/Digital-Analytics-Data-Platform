/*
===============================================================================
FILE: 01_merge_sdi_bronze_sa360_campaign_daily.sql
LAYER: Bronze
TABLE: sdi_bronze_sa360_campaign_daily

PURPOSE:
  Incrementally upsert recent SA360 daily snapshots into Bronze:
    - lookback window for late-arriving files
    - dedup within the window by (file_load_datetime desc, filename desc)
    - enforce canonical date from date_yyyymmdd
    - enforce TMO naming and TBG->TFB standardization

MERGE KEY:
  (account_id, campaign_id, date_yyyymmdd)

NO-GARBAGE RULES:
  - Parse date using SAFE.PARSE_DATE
  - Drop rows where parsed date IS NULL (prevents partition garbage)

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` T
USING (
  WITH src AS (
    SELECT
      -- Keys
      SAFE_CAST(raw.account_id AS STRING) AS account_id,
      SAFE_CAST(raw.campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,

      -- Dimensions (clean strings)
      NULLIF(TRIM(SAFE_CAST(raw.account_name AS STRING)), '') AS account_name,
      NULLIF(TRIM(SAFE_CAST(raw.customer_id AS STRING)), '') AS customer_id,
      NULLIF(TRIM(SAFE_CAST(raw.customer_name AS STRING)), '') AS customer_name,
      NULLIF(TRIM(SAFE_CAST(raw.resource_name AS STRING)), '') AS resource_name,
      NULLIF(TRIM(SAFE_CAST(raw.segments_date AS STRING)), '') AS segments_date,
      SAFE_CAST(raw.client_manager_id AS FLOAT64) AS client_manager_id,
      NULLIF(TRIM(SAFE_CAST(raw.client_manager_name AS STRING)), '') AS client_manager_name,

      -- Ingestion
      SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename,

      -- Metrics
      SAFE_CAST(raw._ma_hint_ec__eligibility__check_ AS FLOAT64) AS ma_hint_ec_eligibility_check,

      SAFE_CAST(raw.aal AS FLOAT64) AS aal,
      SAFE_CAST(raw.add_a__line AS FLOAT64) AS add_a_line,
      SAFE_CAST(raw.all_conversions AS FLOAT64) AS all_conversions,
      SAFE_CAST(raw.bi AS FLOAT64) AS bi,
      SAFE_CAST(raw.bts__quality__traffic AS FLOAT64) AS bts_quality_traffic,
      SAFE_CAST(raw.buying__intent AS FLOAT64) AS buying_intent,

      SAFE_CAST(raw.clicks AS FLOAT64) AS clicks,
      SAFE_CAST(raw.impressions AS FLOAT64) AS impressions,

      SAFE_CAST(raw.cost_micros AS FLOAT64) AS cost_micros,
      SAFE_DIVIDE(SAFE_CAST(raw.cost_micros AS FLOAT64), 1000000.0) AS cost,

      SAFE_CAST(raw.cart__start_ AS FLOAT64) AS cart_start,
      SAFE_CAST(raw.postpaid__cart__start_ AS FLOAT64) AS postpaid_cart_start,
      SAFE_CAST(raw.postpaid_pspv_ AS FLOAT64) AS postpaid_pspv,

      SAFE_CAST(raw.connect__low__funnel__prospect AS FLOAT64) AS connect_low_funnel_prospect,
      SAFE_CAST(raw.connect__low__funnel__visit AS FLOAT64) AS connect_low_funnel_visit,
      SAFE_CAST(raw.connect_qt AS FLOAT64) AS connect_qt,

      SAFE_CAST(raw.digital__gross__add AS FLOAT64) AS digital_gross_add,

      SAFE_CAST(raw.fiber__activations AS FLOAT64) AS fiber_activations,
      SAFE_CAST(raw.fiber__pre__order AS FLOAT64) AS fiber_pre_order,
      SAFE_CAST(raw.fiber__waitlist__sign__up AS FLOAT64) AS fiber_waitlist_sign_up,
      SAFE_CAST(raw.fiber__web__orders AS FLOAT64) AS fiber_web_orders,
      SAFE_CAST(raw.fiber_ec AS FLOAT64) AS fiber_ec,
      SAFE_CAST(raw.fiber_ec_dda AS FLOAT64) AS fiber_ec_dda,
      SAFE_CAST(raw.fiber_sec AS FLOAT64) AS fiber_sec,
      SAFE_CAST(raw.fiber_sec_dda AS FLOAT64) AS fiber_sec_dda,

      SAFE_CAST(raw.hint__invoca__calls AS FLOAT64) AS hint_invoca_calls,
      SAFE_CAST(raw.hint__offline__invoca__calls AS FLOAT64) AS hint_offline_invoca_calls,
      SAFE_CAST(raw.hint__offline__invoca__eligibility AS FLOAT64) AS hint_offline_invoca_eligibility,
      SAFE_CAST(raw.hint__offline__invoca__order AS FLOAT64) AS hint_offline_invoca_order,
      SAFE_CAST(raw.hint__offline__invoca__order_rt_ AS FLOAT64) AS hint_offline_invoca_order_rt,
      SAFE_CAST(raw.hint__offline__invoca__sales__opp AS FLOAT64) AS hint_offline_invoca_sales_opp,
      SAFE_CAST(raw.hint__web__orders AS FLOAT64) AS hint_web_orders,
      SAFE_CAST(raw.hint_ec AS FLOAT64) AS hint_ec,
      SAFE_CAST(raw.hint_sec AS FLOAT64) AS hint_sec,

      SAFE_CAST(raw.magenta_pqt AS FLOAT64) AS magenta_pqt,

      SAFE_CAST(raw.metro__low__funnel_cs_ AS FLOAT64) AS metro_low_funnel_cs,
      SAFE_CAST(raw.metro__mid__funnel__prospect AS FLOAT64) AS metro_mid_funnel_prospect,
      SAFE_CAST(raw.metro__top__funnel__prospect AS FLOAT64) AS metro_top_funnel_prospect,
      SAFE_CAST(raw.metro__upper__funnel__prospect AS FLOAT64) AS metro_upper_funnel_prospect,
      SAFE_CAST(raw.metro_hint_qt AS FLOAT64) AS metro_hint_qt,
      SAFE_CAST(raw.metro_qt AS FLOAT64) AS metro_qt,

      -- TMO naming enforced (raw column is t__mobile__..., Bronze name is tmo_...)
      SAFE_CAST(raw.t__mobile__prepaid__low__funnel__prospect AS FLOAT64) AS tmo_prepaid_low_funnel_prospect,
      SAFE_CAST(raw.tmo__top__funnel__prospect AS FLOAT64) AS tmo_top_funnel_prospect,
      SAFE_CAST(raw.tmo__upper__funnel__prospect AS FLOAT64) AS tmo_upper_funnel_prospect,

      -- Standardize TBG -> TFB
      SAFE_CAST(raw.tbg__low__funnel AS FLOAT64) AS tfb_low_funnel,
      SAFE_CAST(raw.tbg__lead__form__submit AS FLOAT64) AS tfb_lead_form_submit,
      SAFE_CAST(raw.tbg__invoca__sales__intent_dda AS FLOAT64) AS tfb_invoca_sales_intent_dda,
      SAFE_CAST(raw.tbg__invoca__order_dda AS FLOAT64) AS tfb_invoca_order_dda,

      SAFE_CAST(raw.tfb__credit__check AS FLOAT64) AS tfb_credit_check,
      SAFE_CAST(raw.tfb_hint_ec AS FLOAT64) AS tfb_hint_ec,
      SAFE_CAST(raw.tfb__invoca__sales__calls AS FLOAT64) AS tfb_invoca_sales_calls,
      SAFE_CAST(raw.tfb__leads AS FLOAT64) AS tfb_leads,
      SAFE_CAST(raw.tfb__quality__traffic AS FLOAT64) AS tfb_quality_traffic,
      SAFE_CAST(raw.total_tfb__conversions AS FLOAT64) AS total_tfb_conversions

    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` raw
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING))
          >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),

  cleaned AS (
    -- Hard no-garbage rule: drop unparseable snapshot dates
    SELECT *
    FROM src
    WHERE date IS NOT NULL
  ),

  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        cleaned.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC
        ) AS rn
      FROM cleaned
    )
    WHERE rn = 1
  )

  SELECT * FROM dedup
) S
ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN UPDATE SET
  date = S.date,

  account_name = S.account_name,
  customer_id = S.customer_id,
  customer_name = S.customer_name,
  resource_name = S.resource_name,
  segments_date = S.segments_date,
  client_manager_id = S.client_manager_id,
  client_manager_name = S.client_manager_name,

  insert_date = S.insert_date,
  file_load_datetime = S.file_load_datetime,
  filename = S.filename,

  ma_hint_ec_eligibility_check = S.ma_hint_ec_eligibility_check,

  aal = S.aal,
  add_a_line = S.add_a_line,
  all_conversions = S.all_conversions,
  bi = S.bi,
  bts_quality_traffic = S.bts_quality_traffic,
  buying_intent = S.buying_intent,

  clicks = S.clicks,
  impressions = S.impressions,

  cost_micros = S.cost_micros,
  cost = S.cost,

  cart_start = S.cart_start,
  postpaid_cart_start = S.postpaid_cart_start,
  postpaid_pspv = S.postpaid_pspv,

  connect_low_funnel_prospect = S.connect_low_funnel_prospect,
  connect_low_funnel_visit = S.connect_low_funnel_visit,
  connect_qt = S.connect_qt,

  digital_gross_add = S.digital_gross_add,

  fiber_activations = S.fiber_activations,
  fiber_pre_order = S.fiber_pre_order,
  fiber_waitlist_sign_up = S.fiber_waitlist_sign_up,
  fiber_web_orders = S.fiber_web_orders,
  fiber_ec = S.fiber_ec,
  fiber_ec_dda = S.fiber_ec_dda,
  fiber_sec = S.fiber_sec,
  fiber_sec_dda = S.fiber_sec_dda,

  hint_invoca_calls = S.hint_invoca_calls,
  hint_offline_invoca_calls = S.hint_offline_invoca_calls,
  hint_offline_invoca_eligibility = S.hint_offline_invoca_eligibility,
  hint_offline_invoca_order = S.hint_offline_invoca_order,
  hint_offline_invoca_order_rt = S.hint_offline_invoca_order_rt,
  hint_offline_invoca_sales_opp = S.hint_offline_invoca_sales_opp,
  hint_web_orders = S.hint_web_orders,
  hint_ec = S.hint_ec,
  hint_sec = S.hint_sec,

  magenta_pqt = S.magenta_pqt,

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
  total_tfb_conversions = S.total_tfb_conversions

WHEN NOT MATCHED THEN INSERT (
  account_id, campaign_id, date_yyyymmdd, date,
  account_name, customer_id, customer_name, resource_name, segments_date,
  client_manager_id, client_manager_name,
  insert_date, file_load_datetime, filename,

  ma_hint_ec_eligibility_check,
  aal, add_a_line, all_conversions, bi, bts_quality_traffic, buying_intent,
  clicks, impressions,
  cost_micros, cost,
  cart_start, postpaid_cart_start, postpaid_pspv,
  connect_low_funnel_prospect, connect_low_funnel_visit, connect_qt,
  digital_gross_add,
  fiber_activations, fiber_pre_order, fiber_waitlist_sign_up, fiber_web_orders,
  fiber_ec, fiber_ec_dda, fiber_sec, fiber_sec_dda,
  hint_invoca_calls, hint_offline_invoca_calls, hint_offline_invoca_eligibility,
  hint_offline_invoca_order, hint_offline_invoca_order_rt, hint_offline_invoca_sales_opp,
  hint_web_orders, hint_ec, hint_sec,
  magenta_pqt,
  metro_low_funnel_cs, metro_mid_funnel_prospect, metro_top_funnel_prospect, metro_upper_funnel_prospect,
  metro_hint_qt, metro_qt,

  tmo_prepaid_low_funnel_prospect, tmo_top_funnel_prospect, tmo_upper_funnel_prospect,

  tfb_low_funnel, tfb_lead_form_submit, tfb_invoca_sales_intent_dda, tfb_invoca_order_dda,
  tfb_credit_check, tfb_hint_ec, tfb_invoca_sales_calls, tfb_leads, tfb_quality_traffic,
  total_tfb_conversions
)
VALUES (
  S.account_id, S.campaign_id, S.date_yyyymmdd, S.date,
  S.account_name, S.customer_id, S.customer_name, S.resource_name, S.segments_date,
  S.client_manager_id, S.client_manager_name,
  S.insert_date, S.file_load_datetime, S.filename,

  S.ma_hint_ec_eligibility_check,
  S.aal, S.add_a_line, S.all_conversions, S.bi, S.bts_quality_traffic, S.buying_intent,
  S.clicks, S.impressions,
  S.cost_micros, S.cost,
  S.cart_start, S.postpaid_cart_start, S.postpaid_pspv,
  S.connect_low_funnel_prospect, S.connect_low_funnel_visit, S.connect_qt,
  S.digital_gross_add,
  S.fiber_activations, S.fiber_pre_order, S.fiber_waitlist_sign_up, S.fiber_web_orders,
  S.fiber_ec, S.fiber_ec_dda, S.fiber_sec, S.fiber_sec_dda,
  S.hint_invoca_calls, S.hint_offline_invoca_calls, S.hint_offline_invoca_eligibility,
  S.hint_offline_invoca_order, S.hint_offline_invoca_order_rt, S.hint_offline_invoca_sales_opp,
  S.hint_web_orders, S.hint_ec, S.hint_sec,
  S.magenta_pqt,
  S.metro_low_funnel_cs, S.metro_mid_funnel_prospect, S.metro_top_funnel_prospect, S.metro_upper_funnel_prospect,
  S.metro_hint_qt, S.metro_qt,

  S.tmo_prepaid_low_funnel_prospect, S.tmo_top_funnel_prospect, S.tmo_upper_funnel_prospect,

  S.tfb_low_funnel, S.tfb_lead_form_submit, S.tfb_invoca_sales_intent_dda, S.tfb_invoca_order_dda,
  S.tfb_credit_check, S.tfb_hint_ec, S.tfb_invoca_sales_calls, S.tfb_leads, S.tfb_quality_traffic,
  S.total_tfb_conversions
);
