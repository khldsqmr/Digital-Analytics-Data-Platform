/*
===============================================================================
BRONZE | SA 360 | CAMPAIGN DAILY | INCREMENTAL MERGE (FULL LOSSLESS VERSION)
===============================================================================

PURPOSE
-------
Incrementally load and refresh the Bronze Campaign Daily table
from raw Improvado Search Ads 360 export.

This merge:
  • Preserves ALL raw metrics (lossless Bronze principle)
  • Applies a 7-day lookback window for late-arriving data
  • Fully refreshes all metric columns on match
  • Is idempotent and safe for daily scheduling

SOURCE TABLE
------------
prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

TARGET TABLE
------------
prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily

GRAIN
-----
account_id + campaign_id + date_yyyymmdd

LOOKBACK STRATEGY
-----------------
7-day rolling window to handle:
  • Late-arriving files
  • Metric backfills
  • Attribution adjustments

DESIGN PRINCIPLES
-----------------
1. No metric dropped
2. No business logic applied
3. Cost normalized
4. Fully refreshed UPDATE
5. Safe for orchestration
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` T

USING (

  SELECT

    /* ============================================================
       IDENTIFIERS
       ============================================================ */

    account_id,
    account_name,
    campaign_id,
    resource_name,
    customer_id,
    customer_name,
    client_manager_id,
    client_manager_name,

    /* ============================================================
       DATE FIELDS
       ============================================================ */

    date_yyyymmdd,
    date AS raw_numeric_date,
    PARSE_DATE('%Y%m%d', date_yyyymmdd) AS date,
    SAFE.PARSE_DATE('%Y-%m-%d', segments_date) AS segments_date,

    /* ============================================================
       CORE PERFORMANCE
       ============================================================ */

    clicks,
    impressions,
    cost_micros,
    SAFE_DIVIDE(cost_micros, 1000000) AS cost,
    all_conversions,

    /* ============================================================
       GENERAL METRICS
       ============================================================ */

    aal,
    add_a__line AS add_a_line,
    bi,
    buying__intent AS buying_intent,
    bts__quality__traffic AS bts_quality_traffic,
    digital__gross__add AS digital_gross_add,

    /* ============================================================
       CART / CHECKOUT
       ============================================================ */

    cart__start_ AS cart_start,
    postpaid__cart__start_ AS postpaid_cart_start,
    postpaid_pspv_ AS postpaid_pspv,

    /* ============================================================
       CONNECT
       ============================================================ */

    connect__low__funnel__visit AS connect_low_funnel_visit,
    connect__low__funnel__prospect AS connect_low_funnel_prospect,
    connect_qt,

    /* ============================================================
       HINT
       ============================================================ */

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

    /* ============================================================
       FIBER
       ============================================================ */

    fiber__activations AS fiber_activations,
    fiber__pre__order AS fiber_pre_order,
    fiber__waitlist__sign__up AS fiber_waitlist_sign_up,
    fiber_ec,
    fiber_ec_dda,
    fiber__web__orders AS fiber_web_orders,
    fiber_sec,
    fiber_sec_dda,

    /* ============================================================
       METRO
       ============================================================ */

    metro__top__funnel__prospect AS metro_top_funnel_prospect,
    metro__upper__funnel__prospect AS metro_upper_funnel_prospect,
    metro__mid__funnel__prospect AS metro_mid_funnel_prospect,
    metro__low__funnel_cs_ AS metro_low_funnel_cs,
    metro_qt,
    metro_hint_qt,

    /* ============================================================
       TMO
       ============================================================ */

    tmo__top__funnel__prospect AS tmo_top_funnel_prospect,
    tmo__upper__funnel__prospect AS tmo_upper_funnel_prospect,
    t__mobile__prepaid__low__funnel__prospect AS tmo_prepaid_low_funnel_prospect,

    /* ============================================================
       TFB / TBG
       ============================================================ */

    tbg__low__funnel AS tbg_low_funnel,
    tbg__lead__form__submit AS tbg_lead_form_submit,
    tbg__invoca__sales__intent_dda AS tbg_invoca_sales_intent_dda,
    tbg__invoca__order_dda AS tbg_invoca_order_dda,

    tfb__credit__check AS tfb_credit_check,
    tfb_hint_ec,
    tfb__invoca__sales__calls AS tfb_invoca_sales_calls,
    tfb__leads AS tfb_leads,
    tfb__quality__traffic AS tfb_quality_traffic,
    total_tfb__conversions AS total_tfb_conversions,

    /* ============================================================
       OTHER
       ============================================================ */

    magenta_pqt,

    /* ============================================================
       LOAD METADATA
       ============================================================ */

    __insert_date,
    File_Load_datetime AS file_load_datetime,
    Filename AS filename,
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
   WHEN MATCHED → FULL METRIC REFRESH
   ============================================================================ */

WHEN MATCHED THEN
UPDATE SET

  /* Core */
  clicks = S.clicks,
  impressions = S.impressions,
  cost_micros = S.cost_micros,
  cost = S.cost,
  all_conversions = S.all_conversions,

  /* General */
  aal = S.aal,
  add_a_line = S.add_a_line,
  bi = S.bi,
  buying_intent = S.buying_intent,
  bts_quality_traffic = S.bts_quality_traffic,
  digital_gross_add = S.digital_gross_add,

  /* Cart */
  cart_start = S.cart_start,
  postpaid_cart_start = S.postpaid_cart_start,
  postpaid_pspv = S.postpaid_pspv,

  /* Connect */
  connect_low_funnel_visit = S.connect_low_funnel_visit,
  connect_low_funnel_prospect = S.connect_low_funnel_prospect,
  connect_qt = S.connect_qt,

  /* Hint */
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

  /* Fiber */
  fiber_activations = S.fiber_activations,
  fiber_pre_order = S.fiber_pre_order,
  fiber_waitlist_sign_up = S.fiber_waitlist_sign_up,
  fiber_ec = S.fiber_ec,
  fiber_ec_dda = S.fiber_ec_dda,
  fiber_web_orders = S.fiber_web_orders,
  fiber_sec = S.fiber_sec,
  fiber_sec_dda = S.fiber_sec_dda,

  /* Metro */
  metro_top_funnel_prospect = S.metro_top_funnel_prospect,
  metro_upper_funnel_prospect = S.metro_upper_funnel_prospect,
  metro_mid_funnel_prospect = S.metro_mid_funnel_prospect,
  metro_low_funnel_cs = S.metro_low_funnel_cs,
  metro_qt = S.metro_qt,
  metro_hint_qt = S.metro_hint_qt,

  /* TMO */
  tmo_top_funnel_prospect = S.tmo_top_funnel_prospect,
  tmo_upper_funnel_prospect = S.tmo_upper_funnel_prospect,
  tmo_prepaid_low_funnel_prospect = S.tmo_prepaid_low_funnel_prospect,

  /* TFB */
  tbg_low_funnel = S.tbg_low_funnel,
  tbg_lead_form_submit = S.tbg_lead_form_submit,
  tbg_invoca_sales_intent_dda = S.tbg_invoca_sales_intent_dda,
  tbg_invoca_order_dda = S.tbg_invoca_order_dda,
  tfb_credit_check = S.tfb_credit_check,
  tfb_hint_ec = S.tfb_hint_ec,
  tfb_invoca_sales_calls = S.tfb_invoca_sales_calls,
  tfb_leads = S.tfb_leads,
  tfb_quality_traffic = S.tfb_quality_traffic,
  total_tfb_conversions = S.total_tfb_conversions,

  magenta_pqt = S.magenta_pqt,

  bronze_inserted_at = CURRENT_TIMESTAMP()

/* ============================================================================
   WHEN NOT MATCHED → INSERT COMPLETE ROW
   ============================================================================ */

WHEN NOT MATCHED THEN
INSERT ROW;
