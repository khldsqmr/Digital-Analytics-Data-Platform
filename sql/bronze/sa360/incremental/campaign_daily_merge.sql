/*
===============================================================================
BRONZE | SA360 | CAMPAIGN DAILY | INCREMENTAL MERGE (NORMALIZED)
===============================================================================

PURPOSE
-------
Incrementally load campaign daily data into normalized Bronze table.

FEATURES
--------
• 7 day lookback for late-arriving data
• Normalized column naming
• Explicit UPDATE of metrics
• Explicit INSERT column list
• Idempotent design
• No raw messy column names in target

GRAIN
-----
account_id + campaign_id + date_yyyymmdd
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` T

USING (

  SELECT

    /* IDENTIFIERS */
    account_id,
    account_name,
    campaign_id,
    resource_name,
    customer_id,
    customer_name,
    client_manager_id,
    client_manager_name,

    /* DATE */
    date_yyyymmdd,
    date AS date_serial,
    segments_date,
    __insert_date AS insert_date_id,

    /* LOAD METADATA */
    File_Load_datetime AS file_load_datetime,
    Filename AS filename,

    /* CORE PERFORMANCE */
    clicks,
    impressions,
    cost_micros,
    SAFE_DIVIDE(cost_micros, 1000000) AS cost,
    all_conversions,

    /* GENERAL */
    aal,
    add_a__line AS add_a_line,
    bi,
    bts__quality__traffic AS bts_quality_traffic,
    buying__intent AS buying_intent,
    digital__gross__add AS digital_gross_add,

    /* CART */
    cart__start_ AS cart_start,
    postpaid__cart__start_ AS postpaid_cart_start,
    postpaid_pspv_ AS postpaid_pspv,

    /* CONNECT */
    connect__low__funnel__prospect AS connect_low_funnel_prospect,
    connect__low__funnel__visit AS connect_low_funnel_visit,
    connect_qt,

    /* HINT */
    hint_ec,
    hint_sec,
    hint__web__orders AS hint_web_orders,
    hint__invoca__calls AS hint_invoca_calls,
    hint__offline__invoca__calls AS hint_offline_invoca_calls,
    hint__offline__invoca__eligibility AS hint_offline_invoca_eligibility,
    hint__offline__invoca__order AS hint_offline_invoca_order,
    hint__offline__invoca__order_rt_ AS hint_offline_invoca_order_rt,
    hint__offline__invoca__sales__opp AS hint_offline_invoca_sales_opp,
    _ma_hint_ec__eligibility__check_ AS ma_hint_ec_eligibility_check,

    /* FIBER */
    fiber__activations AS fiber_activations,
    fiber__pre__order AS fiber_pre_order,
    fiber__waitlist__sign__up AS fiber_waitlist_sign_up,
    fiber__web__orders AS fiber_web_orders,
    fiber_ec,
    fiber_ec_dda,
    fiber_sec,
    fiber_sec_dda,

    /* METRO */
    metro__low__funnel_cs_ AS metro_low_funnel_cs,
    metro__mid__funnel__prospect AS metro_mid_funnel_prospect,
    metro__top__funnel__prospect AS metro_top_funnel_prospect,
    metro__upper__funnel__prospect AS metro_upper_funnel_prospect,
    metro_hint_qt,
    metro_qt,

    /* TMO */
    tmo__top__funnel__prospect AS tmo_top_funnel_prospect,
    tmo__upper__funnel__prospect AS tmo_upper_funnel_prospect,
    t__mobile__prepaid__low__funnel__prospect AS tmo_prepaid_low_funnel_prospect,

    /* TFB / TBG */
    tbg__low__funnel AS tbg_low_funnel,
    tbg__lead__form__submit AS tbg_lead_form_submit,
    tbg__invoca__sales__intent_dda AS tbg_invoca_sales_intent_dda,
    tbg__invoca__order_dda AS tbg_invoca_order_dda,
    tfb__credit__check AS tfb_credit_check,
    tfb__invoca__sales__calls AS tfb_invoca_sales_calls,
    tfb__leads AS tfb_leads,
    tfb__quality__traffic AS tfb_quality_traffic,
    tfb_hint_ec,
    total_tfb__conversions AS total_tfb_conversions,

    /* OTHER */
    magenta_pqt,

    CURRENT_TIMESTAMP() AS bronze_inserted_at

  FROM
  `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`

  WHERE
    PARSE_DATE('%Y%m%d', date_yyyymmdd)
    >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)

) S

ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

/* ============================================================================
   WHEN MATCHED
============================================================================ */

WHEN MATCHED THEN UPDATE SET

  clicks = S.clicks,
  impressions = S.impressions,
  cost_micros = S.cost_micros,
  cost = S.cost,
  all_conversions = S.all_conversions,

  aal = S.aal,
  add_a_line = S.add_a_line,
  bi = S.bi,
  bts_quality_traffic = S.bts_quality_traffic,
  buying_intent = S.buying_intent,
  digital_gross_add = S.digital_gross_add,

  cart_start = S.cart_start,
  postpaid_cart_start = S.postpaid_cart_start,
  postpaid_pspv = S.postpaid_pspv,

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

  tmo_top_funnel_prospect = S.tmo_top_funnel_prospect,
  tmo_upper_funnel_prospect = S.tmo_upper_funnel_prospect,
  tmo_prepaid_low_funnel_prospect = S.tmo_prepaid_low_funnel_prospect,

  tbg_low_funnel = S.tbg_low_funnel,
  tbg_lead_form_submit = S.tbg_lead_form_submit,
  tbg_invoca_sales_intent_dda = S.tbg_invoca_sales_intent_dda,
  tbg_invoca_order_dda = S.tbg_invoca_order_dda,
  tfb_credit_check = S.tfb_credit_check,
  tfb_invoca_sales_calls = S.tfb_invoca_sales_calls,
  tfb_leads = S.tfb_leads,
  tfb_quality_traffic = S.tfb_quality_traffic,
  tfb_hint_ec = S.tfb_hint_ec,
  total_tfb_conversions = S.total_tfb_conversions,

  magenta_pqt = S.magenta_pqt,
  bronze_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT (
  account_id,
  account_name,
  campaign_id,
  resource_name,
  customer_id,
  customer_name,
  client_manager_id,
  client_manager_name,
  date_yyyymmdd,
  date_serial,
  segments_date,
  insert_date_id,
  file_load_datetime,
  filename,
  clicks,
  impressions,
  cost_micros,
  cost,
  all_conversions,
  aal,
  add_a_line,
  bi,
  bts_quality_traffic,
  buying_intent,
  digital_gross_add,
  cart_start,
  postpaid_cart_start,
  postpaid_pspv,
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
  tmo_top_funnel_prospect,
  tmo_upper_funnel_prospect,
  tmo_prepaid_low_funnel_prospect,
  tbg_low_funnel,
  tbg_lead_form_submit,
  tbg_invoca_sales_intent_dda,
  tbg_invoca_order_dda,
  tfb_credit_check,
  tfb_invoca_sales_calls,
  tfb_leads,
  tfb_quality_traffic,
  tfb_hint_ec,
  total_tfb_conversions,
  magenta_pqt,
  bronze_inserted_at
)
VALUES (
  S.account_id,
  S.account_name,
  S.campaign_id,
  S.resource_name,
  S.customer_id,
  S.customer_name,
  S.client_manager_id,
  S.client_manager_name,
  S.date_yyyymmdd,
  S.date_serial,
  S.segments_date,
  S.insert_date_id,
  S.file_load_datetime,
  S.filename,
  S.clicks,
  S.impressions,
  S.cost_micros,
  S.cost,
  S.all_conversions,
  S.aal,
  S.add_a_line,
  S.bi,
  S.bts_quality_traffic,
  S.buying_intent,
  S.digital_gross_add,
  S.cart_start,
  S.postpaid_cart_start,
  S.postpaid_pspv,
  S.connect_low_funnel_prospect,
  S.connect_low_funnel_visit,
  S.connect_qt,
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
  S.metro_low_funnel_cs,
  S.metro_mid_funnel_prospect,
  S.metro_top_funnel_prospect,
  S.metro_upper_funnel_prospect,
  S.metro_hint_qt,
  S.metro_qt,
  S.tmo_top_funnel_prospect,
  S.tmo_upper_funnel_prospect,
  S.tmo_prepaid_low_funnel_prospect,
  S.tbg_low_funnel,
  S.tbg_lead_form_submit,
  S.tbg_invoca_sales_intent_dda,
  S.tbg_invoca_order_dda,
  S.tfb_credit_check,
  S.tfb_invoca_sales_calls,
  S.tfb_leads,
  S.tfb_quality_traffic,
  S.tfb_hint_ec,
  S.total_tfb_conversions,
  S.magenta_pqt,
  S.bronze_inserted_at
);
