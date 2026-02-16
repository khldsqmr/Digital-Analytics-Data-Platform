/*
===============================================================================
INCREMENTAL | BRONZE | SA360 | CAMPAIGN DAILY (SCHEDULED DAILY)
===============================================================================

KEY (no duplicates)
-------------------
(account_id, campaign_id, date_yyyymmdd)

- We MERGE using the key above to prevent duplicates.
- We always select `bi` explicitly in the source subquery, then reference it as S.bi.

DATE REQUIREMENT
----------------
Derived DATE from date_yyyymmdd is named `date`.
Raw INT64 `date` is preserved as date_int64.
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` T
USING (
  SELECT
    /* ingestion */
    File_Load_datetime AS file_load_datetime,
    Filename AS filename,
    `__insert_date` AS insert_date,

    /* keys/dims */
    CAST(account_id AS STRING) AS account_id,
    account_name,
    CAST(customer_id AS STRING) AS customer_id,
    customer_name,
    CAST(campaign_id AS STRING) AS campaign_id,
    resource_name,

    /* dates */
    `date` AS date_int64,
    CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS date,
    SAFE.PARSE_DATE('%Y-%m-%d', segments_date) AS segments_date,

    /* optional dims */
    client_manager_id,
    client_manager_name,

    /* delivery/cost */
    impressions,
    clicks,
    cost_micros,
    SAFE_DIVIDE(cost_micros, 1000000.0) AS cost,

    /* metrics (explicit) */
    all_conversions,
    `bi` AS bi,
    bts__quality__traffic AS bts_quality_traffic,
    buying__intent AS buying_intent,
    aal,
    add_a__line AS add_a_line,
    digital__gross__add AS digital_gross_add,

    /* cart/pspv */
    cart__start_ AS cart_start,
    postpaid__cart__start_ AS postpaid_cart_start,
    postpaid_pspv_ AS postpaid_pspv,

    /* connect */
    connect__low__funnel__prospect AS connect_low_funnel_prospect,
    connect__low__funnel__visit AS connect_low_funnel_visit,
    connect_qt,

    /* metro */
    metro__low__funnel_cs_ AS metro_low_funnel_cs,
    metro__mid__funnel__prospect AS metro_mid_funnel_prospect,
    metro__top__funnel__prospect AS metro_top_funnel_prospect,
    metro__upper__funnel__prospect AS metro_upper_funnel_prospect,
    metro_hint_qt,
    metro_qt,

    /* fiber */
    fiber__activations AS fiber_activations,
    fiber__pre__order AS fiber_pre_order,
    fiber__waitlist__sign__up AS fiber_waitlist_sign_up,
    fiber__web__orders AS fiber_web_orders,
    fiber_ec,
    fiber_ec_dda,
    fiber_sec,
    fiber_sec_dda,

    /* hint + invoca */
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

    /* prepaid + tmo */
    t__mobile__prepaid__low__funnel__prospect AS tmobile_prepaid_low_funnel_prospect,
    tmo__top__funnel__prospect AS tmo_top_funnel_prospect,
    tmo__upper__funnel__prospect AS tmo_upper_funnel_prospect,

    /* tfb unified (tbg==tfb) */
    tbg__low__funnel AS tfb_low_funnel,
    tbg__lead__form__submit AS tfb_lead_form_submit,
    tbg__invoca__sales__intent_dda AS tfb_invoca_sales_intent_dda,
    tbg__invoca__order_dda AS tfb_invoca_order_dda,

    tfb__credit__check AS tfb_credit_check,
    tfb__invoca__sales__calls AS tfb_invoca_sales_calls,
    tfb__leads AS tfb_leads,
    tfb__quality__traffic AS tfb_quality_traffic,
    tfb_hint_ec,
    total_tfb__conversions AS total_tfb_conversions,

    magenta_pqt

  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
  WHERE TIMESTAMP(File_Load_datetime) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)
) S
ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN
UPDATE SET
  T.file_load_datetime = S.file_load_datetime,
  T.filename = S.filename,
  T.insert_date = S.insert_date,

  T.account_name = S.account_name,
  T.customer_id = S.customer_id,
  T.customer_name = S.customer_name,
  T.resource_name = S.resource_name,

  T.date_int64 = S.date_int64,
  T.date = S.date,
  T.segments_date = S.segments_date,

  T.client_manager_id = S.client_manager_id,
  T.client_manager_name = S.client_manager_name,

  T.impressions = S.impressions,
  T.clicks = S.clicks,
  T.cost_micros = S.cost_micros,
  T.cost = S.cost,

  T.all_conversions = S.all_conversions,
  T.bi = S.bi,
  T.bts_quality_traffic = S.bts_quality_traffic,
  T.buying_intent = S.buying_intent,
  T.aal = S.aal,
  T.add_a_line = S.add_a_line,
  T.digital_gross_add = S.digital_gross_add,

  T.cart_start = S.cart_start,
  T.postpaid_cart_start = S.postpaid_cart_start,
  T.postpaid_pspv = S.postpaid_pspv,

  T.connect_low_funnel_prospect = S.connect_low_funnel_prospect,
  T.connect_low_funnel_visit = S.connect_low_funnel_visit,
  T.connect_qt = S.connect_qt,

  T.metro_low_funnel_cs = S.metro_low_funnel_cs,
  T.metro_mid_funnel_prospect = S.metro_mid_funnel_prospect,
  T.metro_top_funnel_prospect = S.metro_top_funnel_prospect,
  T.metro_upper_funnel_prospect = S.metro_upper_funnel_prospect,
  T.metro_hint_qt = S.metro_hint_qt,
  T.metro_qt = S.metro_qt,

  T.fiber_activations = S.fiber_activations,
  T.fiber_pre_order = S.fiber_pre_order,
  T.fiber_waitlist_sign_up = S.fiber_waitlist_sign_up,
  T.fiber_web_orders = S.fiber_web_orders,
  T.fiber_ec = S.fiber_ec,
  T.fiber_ec_dda = S.fiber_ec_dda,
  T.fiber_sec = S.fiber_sec,
  T.fiber_sec_dda = S.fiber_sec_dda,

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

  T.tmobile_prepaid_low_funnel_prospect = S.tmobile_prepaid_low_funnel_prospect,
  T.tmo_top_funnel_prospect = S.tmo_top_funnel_prospect,
  T.tmo_upper_funnel_prospect = S.tmo_upper_funnel_prospect,

  T.tfb_low_funnel = S.tfb_low_funnel,
  T.tfb_lead_form_submit = S.tfb_lead_form_submit,
  T.tfb_invoca_sales_intent_dda = S.tfb_invoca_sales_intent_dda,
  T.tfb_invoca_order_dda = S.tfb_invoca_order_dda,

  T.tfb_credit_check = S.tfb_credit_check,
  T.tfb_invoca_sales_calls = S.tfb_invoca_sales_calls,
  T.tfb_leads = S.tfb_leads,
  T.tfb_quality_traffic = S.tfb_quality_traffic,
  T.tfb_hint_ec = S.tfb_hint_ec,
  T.total_tfb_conversions = S.total_tfb_conversions,

  T.magenta_pqt = S.magenta_pqt,

  T.bronze_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT (
  file_load_datetime, filename, insert_date,
  account_id, account_name, customer_id, customer_name, campaign_id, resource_name,
  date_int64, date_yyyymmdd, date, segments_date,
  client_manager_id, client_manager_name,
  impressions, clicks, cost_micros, cost,
  all_conversions, bi, bts_quality_traffic, buying_intent, aal, add_a_line, digital_gross_add,
  cart_start, postpaid_cart_start, postpaid_pspv,
  connect_low_funnel_prospect, connect_low_funnel_visit, connect_qt,
  metro_low_funnel_cs, metro_mid_funnel_prospect, metro_top_funnel_prospect, metro_upper_funnel_prospect, metro_hint_qt, metro_qt,
  fiber_activations, fiber_pre_order, fiber_waitlist_sign_up, fiber_web_orders, fiber_ec, fiber_ec_dda, fiber_sec, fiber_sec_dda,
  hint_ec, hint_sec, ma_hint_ec_eligibility_check,
  hint_invoca_calls, hint_offline_invoca_calls, hint_offline_invoca_eligibility, hint_offline_invoca_order,
  hint_offline_invoca_order_rt, hint_offline_invoca_sales_opp, hint_web_orders,
  tmobile_prepaid_low_funnel_prospect, tmo_top_funnel_prospect, tmo_upper_funnel_prospect,
  tfb_low_funnel, tfb_lead_form_submit, tfb_invoca_sales_intent_dda, tfb_invoca_order_dda,
  tfb_credit_check, tfb_invoca_sales_calls, tfb_leads, tfb_quality_traffic, tfb_hint_ec, total_tfb_conversions,
  magenta_pqt,
  bronze_updated_at
)
VALUES (
  S.file_load_datetime, S.filename, S.insert_date,
  S.account_id, S.account_name, S.customer_id, S.customer_name, S.campaign_id, S.resource_name,
  S.date_int64, S.date_yyyymmdd, S.date, S.segments_date,
  S.client_manager_id, S.client_manager_name,
  S.impressions, S.clicks, S.cost_micros, S.cost,
  S.all_conversions, S.bi, S.bts_quality_traffic, S.buying_intent, S.aal, S.add_a_line, S.digital_gross_add,
  S.cart_start, S.postpaid_cart_start, S.postpaid_pspv,
  S.connect_low_funnel_prospect, S.connect_low_funnel_visit, S.connect_qt,
  S.metro_low_funnel_cs, S.metro_mid_funnel_prospect, S.metro_top_funnel_prospect, S.metro_upper_funnel_prospect, S.metro_hint_qt, S.metro_qt,
  S.fiber_activations, S.fiber_pre_order, S.fiber_waitlist_sign_up, S.fiber_web_orders, S.fiber_ec, S.fiber_ec_dda, S.fiber_sec, S.fiber_sec_dda,
  S.hint_ec, S.hint_sec, S.ma_hint_ec_eligibility_check,
  S.hint_invoca_calls, S.hint_offline_invoca_calls, S.hint_offline_invoca_eligibility, S.hint_offline_invoca_order,
  S.hint_offline_invoca_order_rt, S.hint_offline_invoca_sales_opp, S.hint_web_orders,
  S.tmobile_prepaid_low_funnel_prospect, S.tmo_top_funnel_prospect, S.tmo_upper_funnel_prospect,
  S.tfb_low_funnel, S.tfb_lead_form_submit, S.tfb_invoca_sales_intent_dda, S.tfb_invoca_order_dda,
  S.tfb_credit_check, S.tfb_invoca_sales_calls, S.tfb_leads, S.tfb_quality_traffic, S.tfb_hint_ec, S.total_tfb_conversions,
  S.magenta_pqt,
  CURRENT_TIMESTAMP()
);
