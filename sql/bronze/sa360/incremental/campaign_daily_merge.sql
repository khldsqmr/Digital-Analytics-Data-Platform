/*
===============================================================================
INCREMENTAL | BRONZE | SA360 | CAMPAIGN DAILY (PERFORMANCE SNAPSHOT)
===============================================================================

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

TARGET (BRONZE):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily

COST CONTROL:
  Process only the most recent N days of snapshot data using lookback_days variable.

NO DUPLICATES:
  MERGE KEY:
    (account_id, campaign_id, date_yyyymmdd)

  DEDUP WITHIN WINDOW:
    Keep latest row by:
      File_Load_datetime DESC, Filename DESC

STANDARDIZATION:
  - Parsed date from date_yyyymmdd is named "date"
  - Raw INT64 "date" from source becomes date_serial
  - __insert_date -> insert_date
  - _ma_hint_ec__eligibility__check_ -> ma_hint_ec_eligibility_check
  - Standardize TBG -> TFB (per your rule)

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` T
USING (
  WITH src AS (
    SELECT
      CAST(raw.account_id AS STRING) AS account_id,
      CAST(raw.campaign_id AS STRING) AS campaign_id,
      CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING)) AS date,
      CAST(raw.date AS INT64) AS date_serial,

      CAST(raw.account_name AS STRING) AS account_name,
      CAST(raw.customer_id AS STRING) AS customer_id,
      CAST(raw.customer_name AS STRING) AS customer_name,
      CAST(raw.resource_name AS STRING) AS resource_name,
      CAST(raw.segments_date AS STRING) AS segments_date,
      CAST(raw.client_manager_id AS FLOAT64) AS client_manager_id,
      CAST(raw.client_manager_name AS STRING) AS client_manager_name,

      CAST(raw.__insert_date AS INT64) AS insert_date,
      CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      CAST(raw.Filename AS STRING) AS filename,

      CAST(raw._ma_hint_ec__eligibility__check_ AS FLOAT64) AS ma_hint_ec_eligibility_check,

      CAST(raw.aal AS FLOAT64) AS aal,
      CAST(raw.add_a__line AS FLOAT64) AS add_a_line,
      CAST(raw.all_conversions AS FLOAT64) AS all_conversions,
      CAST(raw.bi AS FLOAT64) AS bi,
      CAST(raw.bts__quality__traffic AS FLOAT64) AS bts_quality_traffic,
      CAST(raw.buying__intent AS FLOAT64) AS buying_intent,

      CAST(raw.clicks AS FLOAT64) AS clicks,
      CAST(raw.impressions AS FLOAT64) AS impressions,

      CAST(raw.cost_micros AS FLOAT64) AS cost_micros,
      SAFE_DIVIDE(CAST(raw.cost_micros AS FLOAT64), 1000000.0) AS cost,

      CAST(raw.cart__start_ AS FLOAT64) AS cart_start,
      CAST(raw.postpaid__cart__start_ AS FLOAT64) AS postpaid_cart_start,
      CAST(raw.postpaid_pspv_ AS FLOAT64) AS postpaid_pspv,

      CAST(raw.connect__low__funnel__prospect AS FLOAT64) AS connect_low_funnel_prospect,
      CAST(raw.connect__low__funnel__visit AS FLOAT64) AS connect_low_funnel_visit,
      CAST(raw.connect_qt AS FLOAT64) AS connect_qt,

      CAST(raw.digital__gross__add AS FLOAT64) AS digital_gross_add,

      CAST(raw.fiber__activations AS FLOAT64) AS fiber_activations,
      CAST(raw.fiber__pre__order AS FLOAT64) AS fiber_pre_order,
      CAST(raw.fiber__waitlist__sign__up AS FLOAT64) AS fiber_waitlist_sign_up,
      CAST(raw.fiber__web__orders AS FLOAT64) AS fiber_web_orders,
      CAST(raw.fiber_ec AS FLOAT64) AS fiber_ec,
      CAST(raw.fiber_ec_dda AS FLOAT64) AS fiber_ec_dda,
      CAST(raw.fiber_sec AS FLOAT64) AS fiber_sec,
      CAST(raw.fiber_sec_dda AS FLOAT64) AS fiber_sec_dda,

      CAST(raw.hint__invoca__calls AS FLOAT64) AS hint_invoca_calls,
      CAST(raw.hint__offline__invoca__calls AS FLOAT64) AS hint_offline_invoca_calls,
      CAST(raw.hint__offline__invoca__eligibility AS FLOAT64) AS hint_offline_invoca_eligibility,
      CAST(raw.hint__offline__invoca__order AS FLOAT64) AS hint_offline_invoca_order,
      CAST(raw.hint__offline__invoca__order_rt_ AS FLOAT64) AS hint_offline_invoca_order_rt,
      CAST(raw.hint__offline__invoca__sales__opp AS FLOAT64) AS hint_offline_invoca_sales_opp,
      CAST(raw.hint__web__orders AS FLOAT64) AS hint_web_orders,
      CAST(raw.hint_ec AS FLOAT64) AS hint_ec,
      CAST(raw.hint_sec AS FLOAT64) AS hint_sec,

      CAST(raw.magenta_pqt AS FLOAT64) AS magenta_pqt,

      CAST(raw.metro__low__funnel_cs_ AS FLOAT64) AS metro_low_funnel_cs,
      CAST(raw.metro__mid__funnel__prospect AS FLOAT64) AS metro_mid_funnel_prospect,
      CAST(raw.metro__top__funnel__prospect AS FLOAT64) AS metro_top_funnel_prospect,
      CAST(raw.metro__upper__funnel__prospect AS FLOAT64) AS metro_upper_funnel_prospect,
      CAST(raw.metro_hint_qt AS FLOAT64) AS metro_hint_qt,
      CAST(raw.metro_qt AS FLOAT64) AS metro_qt,

      CAST(raw.t__mobile__prepaid__low__funnel__prospect AS FLOAT64) AS t_mobile_prepaid_low_funnel_prospect,
      CAST(raw.tmo__top__funnel__prospect AS FLOAT64) AS tmo_top_funnel_prospect,
      CAST(raw.tmo__upper__funnel__prospect AS FLOAT64) AS tmo_upper_funnel_prospect,

      -- Standardize TBG -> TFB
      CAST(raw.tbg__low__funnel AS FLOAT64) AS tfb_low_funnel,
      CAST(raw.tbg__lead__form__submit AS FLOAT64) AS tfb_lead_form_submit,
      CAST(raw.tbg__invoca__sales__intent_dda AS FLOAT64) AS tfb_invoca_sales_intent_dda,
      CAST(raw.tbg__invoca__order_dda AS FLOAT64) AS tfb_invoca_order_dda,

      CAST(raw.tfb__credit__check AS FLOAT64) AS tfb_credit_check,
      CAST(raw.tfb_hint_ec AS FLOAT64) AS tfb_hint_ec,
      CAST(raw.tfb__invoca__sales__calls AS FLOAT64) AS tfb_invoca_sales_calls,
      CAST(raw.tfb__leads AS FLOAT64) AS tfb_leads,
      CAST(raw.tfb__quality__traffic AS FLOAT64) AS tfb_quality_traffic,
      CAST(raw.total_tfb__conversions AS FLOAT64) AS total_tfb_conversions

    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` raw
    WHERE
      SAFE.PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING))
        >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),

  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        src.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC
        ) AS rn
      FROM src
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
  date_serial = S.date_serial,

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

  t_mobile_prepaid_low_funnel_prospect = S.t_mobile_prepaid_low_funnel_prospect,
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
  account_id, campaign_id, date_yyyymmdd, date, date_serial,
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
  t_mobile_prepaid_low_funnel_prospect, tmo_top_funnel_prospect, tmo_upper_funnel_prospect,

  tfb_low_funnel, tfb_lead_form_submit, tfb_invoca_sales_intent_dda, tfb_invoca_order_dda,
  tfb_credit_check, tfb_hint_ec, tfb_invoca_sales_calls, tfb_leads, tfb_quality_traffic,
  total_tfb_conversions
)
VALUES (
  S.account_id, S.campaign_id, S.date_yyyymmdd, S.date, S.date_serial,
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
  S.t_mobile_prepaid_low_funnel_prospect, S.tmo_top_funnel_prospect, S.tmo_upper_funnel_prospect,

  S.tfb_low_funnel, S.tfb_lead_form_submit, S.tfb_invoca_sales_intent_dda, S.tfb_invoca_order_dda,
  S.tfb_credit_check, S.tfb_hint_ec, S.tfb_invoca_sales_calls, S.tfb_leads, S.tfb_quality_traffic,
  S.total_tfb_conversions
);
