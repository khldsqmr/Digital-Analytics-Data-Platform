/*
===============================================================================
INCREMENTAL | BRONZE | SA360 | CAMPAIGN DAILY (SCHEDULED DAILY)
===============================================================================

GOAL
----
Upsert daily campaign performance rows without duplicates.

KEY
---
account_id + campaign_id + date_yyyymmdd

LOOKBACK
--------
7 days by File_Load_datetime to capture late arriving corrections.

CRITICAL FIX
------------
We select `bi` explicitly into the MERGE source subquery and always reference it
as S.bi in UPDATE/INSERT.

TBG == TFB
----------
We populate tfb_* unified columns using the TBG raw columns.
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` T
USING (
  SELECT
    /* ---------- LOAD METADATA ---------- */
    File_Load_datetime AS file_load_datetime,
    Filename AS filename,
    `__insert_date` AS insert_date,

    /* ---------- KEYS / DIMENSIONS ---------- */
    CAST(account_id AS STRING) AS account_id,
    account_name,
    customer_id,
    customer_name,
    CAST(campaign_id AS STRING) AS campaign_id,
    resource_name,

    /* ---------- DATES ---------- */
    `date` AS date_serial,
    CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS report_date,
    SAFE.PARSE_DATE('%Y-%m-%d', segments_date) AS segments_date,

    /* ---------- OPTIONAL DIMENSIONS ---------- */
    client_manager_id,
    client_manager_name,

    /* ---------- DELIVERY / COST ---------- */
    impressions,
    clicks,
    cost_micros,
    SAFE_DIVIDE(cost_micros, 1000000.0) AS cost,

    /* ---------- METRICS (explicit, includes bi) ---------- */
    all_conversions,
    `bi` AS bi,  -- <- important: raw bi explicitly projected
    buying__intent AS buying_intent,
    bts__quality__traffic AS bts_quality_traffic,
    aal,
    add_a__line AS add_a_line,
    digital__gross__add AS digital_gross_add,
    magenta_pqt,

    /* ---------- CART / PSPV ---------- */
    cart__start_ AS cart_start,
    postpaid__cart__start_ AS postpaid_cart_start,
    postpaid_pspv_ AS postpaid_pspv,

    /* ---------- CONNECT ---------- */
    connect__low__funnel__visit AS connect_low_funnel_visit,
    connect__low__funnel__prospect AS connect_low_funnel_prospect,
    connect_qt,

    /* ---------- METRO ---------- */
    metro__low__funnel_cs_ AS metro_low_funnel_cs,
    metro_qt,
    metro_hint_qt,
    metro__top__funnel__prospect AS metro_top_funnel_prospect,
    metro__mid__funnel__prospect AS metro_mid_funnel_prospect,
    metro__upper__funnel__prospect AS metro_upper_funnel_prospect,

    /* ---------- FIBER ---------- */
    fiber__activations AS fiber_activations,
    fiber__pre__order AS fiber_pre_order,
    fiber__waitlist__sign__up AS fiber_waitlist_sign_up,
    fiber__web__orders AS fiber_web_orders,
    fiber_ec,
    fiber_ec_dda,
    fiber_sec,
    fiber_sec_dda,

    /* ---------- HINT / INVOCA ---------- */
    hint_ec,
    hint_sec,
    `_ma_hint_ec__eligibility__check_` AS ma_hint_ec_eligibility_check,

    hint__invoca__calls AS hint_invoca_calls,
    hint__offline__invoca__calls AS hint_offline_invoca_calls,
    hint__offline__invoca__eligibility AS hint_offline_invoca_eligibility,
    hint__offline__invoca__order AS hint_offline_invoca_order,
    hint__offline__invoca__order_rt_ AS hint_offline_invoca_order_rt,
    hint__offline__invoca__sales__opp AS hint_offline_invoca_sales_opp,
    hint__web__orders AS hint_web_orders,

    /* ---------- TMO FUNNEL + PREPAID ---------- */
    tmo__top__funnel__prospect AS tmo_top_funnel_prospect,
    tmo__upper__funnel__prospect AS tmo_upper_funnel_prospect,
    t__mobile__prepaid__low__funnel__prospect AS tmobile_prepaid_low_funnel_prospect,

    /* ---------- TFB (Unified for TBG + TFB) ---------- */
    tbg__low__funnel AS tfb_low_funnel,
    tbg__lead__form__submit AS tfb_lead_form_submit,
    tbg__invoca__sales__intent_dda AS tfb_invoca_sales_intent_dda,
    tbg__invoca__order_dda AS tfb_invoca_order_dda,

    tfb__credit__check AS tfb_credit_check,
    tfb_hint_ec,
    tfb__invoca__sales__calls AS tfb_invoca_sales_calls,
    tfb__leads AS tfb_leads,
    tfb__quality__traffic AS tfb_quality_traffic,
    total_tfb__conversions AS total_tfb_conversions

  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
  WHERE TIMESTAMP(File_Load_datetime) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)
) S
ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN
UPDATE SET
  /* metadata */
  T.file_load_datetime = S.file_load_datetime,
  T.filename = S.filename,
  T.insert_date = S.insert_date,

  /* dims */
  T.account_name = S.account_name,
  T.customer_id = S.customer_id,
  T.customer_name = S.customer_name,
  T.resource_name = S.resource_name,
  T.client_manager_id = S.client_manager_id,
  T.client_manager_name = S.client_manager_name,

  /* dates */
  T.date_serial = S.date_serial,
  T.report_date = S.report_date,
  T.segments_date = S.segments_date,

  /* delivery */
  T.impressions = S.impressions,
  T.clicks = S.clicks,
  T.cost_micros = S.cost_micros,
  T.cost = S.cost,

  /* metrics (includes bi) */
  T.all_conversions = S.all_conversions,
  T.bi = S.bi,
  T.buying_intent = S.buying_intent,
  T.bts_quality_traffic = S.bts_quality_traffic,
  T.aal = S.aal,
  T.add_a_line = S.add_a_line,
  T.digital_gross_add = S.digital_gross_add,
  T.magenta_pqt = S.magenta_pqt,

  /* cart/pspv */
  T.cart_start = S.cart_start,
  T.postpaid_cart_start = S.postpaid_cart_start,
  T.postpaid_pspv = S.postpaid_pspv,

  /* connect */
  T.connect_low_funnel_visit = S.connect_low_funnel_visit,
  T.connect_low_funnel_prospect = S.connect_low_funnel_prospect,
  T.connect_qt = S.connect_qt,

  /* metro */
  T.metro_low_funnel_cs = S.metro_low_funnel_cs,
  T.metro_qt = S.metro_qt,
  T.metro_hint_qt = S.metro_hint_qt,
  T.metro_top_funnel_prospect = S.metro_top_funnel_prospect,
  T.metro_mid_funnel_prospect = S.metro_mid_funnel_prospect,
  T.metro_upper_funnel_prospect = S.metro_upper_funnel_prospect,

  /* fiber */
  T.fiber_activations = S.fiber_activations,
  T.fiber_pre_order = S.fiber_pre_order,
  T.fiber_waitlist_sign_up = S.fiber_waitlist_sign_up,
  T.fiber_web_orders = S.fiber_web_orders,
  T.fiber_ec = S.fiber_ec,
  T.fiber_ec_dda = S.fiber_ec_dda,
  T.fiber_sec = S.fiber_sec,
  T.fiber_sec_dda = S.fiber_sec_dda,

  /* hint */
  T.hint_ec = S.hint_ec,
  T.hint_sec = S.hint_sec,
  T.ma_hint_ec_eligibility_check = S.ma_hint_ec_eligibility_check,
  T.hint_invoca_calls = S.hint_invoca_calls,
  T.hint_offline_invoca_calls = S.hint_offline_invoca_calls,
  T.hint_offline_invoca_eligibility = S.hint_offline_invoca_eligibility,
  T.hint_offline_invoca_order = S.hint_offline_invoca_order,
  T.hint_offline_invoca_order_rt = S.hint_offline_invoca_order_rt,
  T.hint_offline_invoca_sales_opp = S.hint_offline_invoca_sales_opp,
  T.hint_web_orders = S.hint_web_orders,

  /* tmo/prepaid */
  T.tmo_top_funnel_prospect = S.tmo_top_funnel_prospect,
  T.tmo_upper_funnel_prospect = S.tmo_upper_funnel_prospect,
  T.tmobile_prepaid_low_funnel_prospect = S.tmobile_prepaid_low_funnel_prospect,

  /* tfb unified */
  T.tfb_low_funnel = S.tfb_low_funnel,
  T.tfb_lead_form_submit = S.tfb_lead_form_submit,
  T.tfb_invoca_sales_intent_dda = S.tfb_invoca_sales_intent_dda,
  T.tfb_invoca_order_dda = S.tfb_invoca_order_dda,
  T.tfb_credit_check = S.tfb_credit_check,
  T.tfb_hint_ec = S.tfb_hint_ec,
  T.tfb_invoca_sales_calls = S.tfb_invoca_sales_calls,
  T.tfb_leads = S.tfb_leads,
  T.tfb_quality_traffic = S.tfb_quality_traffic,
  T.total_tfb_conversions = S.total_tfb_conversions,

  T.bronze_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT (
  file_load_datetime, filename, insert_date,
  account_id, account_name, customer_id, customer_name, campaign_id, resource_name,
  date_serial, date_yyyymmdd, report_date, segments_date,
  client_manager_id, client_manager_name,
  impressions, clicks, cost_micros, cost,
  all_conversions, bi, buying_intent, bts_quality_traffic, aal, add_a_line, digital_gross_add, magenta_pqt,
  cart_start, postpaid_cart_start, postpaid_pspv,
  connect_low_funnel_visit, connect_low_funnel_prospect, connect_qt,
  metro_low_funnel_cs, metro_qt, metro_hint_qt, metro_top_funnel_prospect, metro_mid_funnel_prospect, metro_upper_funnel_prospect,
  fiber_activations, fiber_pre_order, fiber_waitlist_sign_up, fiber_web_orders, fiber_ec, fiber_ec_dda, fiber_sec, fiber_sec_dda,
  hint_ec, hint_sec, ma_hint_ec_eligibility_check, hint_invoca_calls, hint_offline_invoca_calls, hint_offline_invoca_eligibility,
  hint_offline_invoca_order, hint_offline_invoca_order_rt, hint_offline_invoca_sales_opp, hint_web_orders,
  tmo_top_funnel_prospect, tmo_upper_funnel_prospect, tmobile_prepaid_low_funnel_prospect,
  tfb_low_funnel, tfb_lead_form_submit, tfb_invoca_sales_intent_dda, tfb_invoca_order_dda,
  tfb_credit_check, tfb_hint_ec, tfb_invoca_sales_calls, tfb_leads, tfb_quality_traffic, total_tfb_conversions,
  bronze_inserted_at
)
VALUES (
  S.file_load_datetime, S.filename, S.insert_date,
  S.account_id, S.account_name, S.customer_id, S.customer_name, S.campaign_id, S.resource_name,
  S.date_serial, S.date_yyyymmdd, S.report_date, S.segments_date,
  S.client_manager_id, S.client_manager_name,
  S.impressions, S.clicks, S.cost_micros, S.cost,
  S.all_conversions, S.bi, S.buying_intent, S.bts_quality_traffic, S.aal, S.add_a_line, S.digital_gross_add, S.magenta_pqt,
  S.cart_start, S.postpaid_cart_start, S.postpaid_pspv,
  S.connect_low_funnel_visit, S.connect_low_funnel_prospect, S.connect_qt,
  S.metro_low_funnel_cs, S.metro_qt, S.metro_hint_qt, S.metro_top_funnel_prospect, S.metro_mid_funnel_prospect, S.metro_upper_funnel_prospect,
  S.fiber_activations, S.fiber_pre_order, S.fiber_waitlist_sign_up, S.fiber_web_orders, S.fiber_ec, S.fiber_ec_dda, S.fiber_sec, S.fiber_sec_dda,
  S.hint_ec, S.hint_sec, S.ma_hint_ec_eligibility_check, S.hint_invoca_calls, S.hint_offline_invoca_calls, S.hint_offline_invoca_eligibility,
  S.hint_offline_invoca_order, S.hint_offline_invoca_order_rt, S.hint_offline_invoca_sales_opp, S.hint_web_orders,
  S.tmo_top_funnel_prospect, S.tmo_upper_funnel_prospect, S.tmobile_prepaid_low_funnel_prospect,
  S.tfb_low_funnel, S.tfb_lead_form_submit, S.tfb_invoca_sales_intent_dda, S.tfb_invoca_order_dda,
  S.tfb_credit_check, S.tfb_hint_ec, S.tfb_invoca_sales_calls, S.tfb_leads, S.tfb_quality_traffic, S.total_tfb_conversions,
  CURRENT_TIMESTAMP()
);
