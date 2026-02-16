/*
===============================================================================
BOOTSTRAP | BRONZE | SA360 | CAMPAIGN DAILY (ONE-TIME)
===============================================================================

PURPOSE
-------
Create the Bronze Campaign Daily table with:
- Normalized, analysis-friendly column names
- Column-level descriptions (governance-ready)
- Partitioning by report_date (derived from date_yyyymmdd)
- Clustering for performance

SOURCE
------
`prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`

RELATIONSHIP
------------
This table joins to the Campaign Entity table on:
  account_id + campaign_id
(and optionally customer_id / resource_name for validation).

NOTES ON NORMALIZATION
----------------------
Raw columns include double-underscores and trailing underscores.
We normalize to single underscores and remove trailing underscores.

Example:
  raw: cart__start_     -> cart_start
  raw: __insert_date    -> insert_date
  raw: _ma_hint_ec__eligibility__check_ -> ma_hint_ec_eligibility_check

TBG == TFB RULE
---------------
TBG and TFB are treated as the same line of business.
We standardize to TFB naming:
  tbg__low__funnel              -> tfb_low_funnel
  tbg__lead__form__submit       -> tfb_lead_form_submit
  tbg__invoca__sales__intent_dda-> tfb_invoca_sales_intent_dda
  tbg__invoca__order_dda        -> tfb_invoca_order_dda
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
(
  /* =============================
     LOAD METADATA (Technical)
  ============================== */
  file_load_datetime DATETIME OPTIONS(description="ETL load timestamp (file landed time)."),
  filename STRING OPTIONS(description="Source file path/name for this daily snapshot."),
  insert_date INT64 OPTIONS(description="Technical load integer (raw: __insert_date)."),

  /* =============================
     CORE KEYS / DIMENSIONS
  ============================== */
  account_id STRING OPTIONS(description="Search Ads 360 / advertiser account ID."),
  account_name STRING OPTIONS(description="Account/advertiser name."),
  customer_id STRING OPTIONS(description="Google Ads customer ID."),
  customer_name STRING OPTIONS(description="Customer (account) name."),
  campaign_id STRING OPTIONS(description="Campaign ID aligned to resource_name."),
  resource_name STRING OPTIONS(description="Google Ads API resource name for the campaign."),

  /* =============================
     REPORTING DATES
  ============================== */
  date_serial INT64 OPTIONS(description="Numeric date field from raw feed (raw: date)."),
  date_yyyymmdd STRING OPTIONS(description="Reporting date in YYYYMMDD (raw: date_yyyymmdd)."),
  report_date DATE OPTIONS(description="Reporting date as DATE derived from date_yyyymmdd."),
  segments_date DATE OPTIONS(description="Segments.date as DATE (raw segments_date; may be null)."),

  /* =============================
     OPTIONAL DIMENSIONS
  ============================== */
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID in SA360."),
  client_manager_name STRING OPTIONS(description="Client manager name."),

  /* =============================
     COST / DELIVERY
  ============================== */
  impressions FLOAT64 OPTIONS(description="Impressions."),
  clicks FLOAT64 OPTIONS(description="Total clicks."),
  cost_micros FLOAT64 OPTIONS(description="Advertiser cost in micros of currency (1e6 micros = 1 unit)."),
  cost FLOAT64 OPTIONS(description="Advertiser cost in currency units (cost_micros / 1e6)."),

  /* =============================
     GENERIC / HIGH-LEVEL METRICS
  ============================== */
  all_conversions FLOAT64 OPTIONS(description="All conversions including cross-device/modeled where applicable."),
  bi FLOAT64 OPTIONS(description="Business Intent (BI) or internal intent score (raw: bi)."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent signal/score (raw: buying__intent)."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic metric (raw: bts__quality__traffic)."),
  aal FLOAT64 OPTIONS(description="Add-a-line related conversions/score (raw: aal)."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-line conversions (raw: add_a__line)."),
  digital_gross_add FLOAT64 OPTIONS(description="Digital gross adds (raw: digital__gross__add)."),
  magenta_pqt FLOAT64 OPTIONS(description="Magenta pre-qualification tool completions (raw: magenta_pqt)."),

  /* =============================
     CART / PSPV
  ============================== */
  cart_start FLOAT64 OPTIONS(description="Cart start events attributed to campaign (raw: cart__start_)."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart start events (raw: postpaid__cart__start_)."),
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV metric (raw: postpaid_pspv_)."),

  /* =============================
     CONNECT
  ============================== */
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low-funnel visits (raw: connect__low__funnel__visit)."),
  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low-funnel prospects (raw: connect__low__funnel__prospect)."),
  connect_qt FLOAT64 OPTIONS(description="Connect qualified traffic (raw: connect_qt)."),

  /* =============================
     METRO
  ============================== */
  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low-funnel customer signups (raw: metro__low__funnel_cs_)."),
  metro_qt FLOAT64 OPTIONS(description="Metro qualified traffic (raw: metro_qt)."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro HINT qualified traffic (raw: metro_hint_qt)."),
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top-funnel prospects (raw: metro__top__funnel__prospect)."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid-funnel prospects (raw: metro__mid__funnel__prospect)."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper-funnel prospects (raw: metro__upper__funnel__prospect)."),

  /* =============================
     FIBER
  ============================== */
  fiber_activations FLOAT64 OPTIONS(description="Fiber service activations (raw: fiber__activations)."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-orders (raw: fiber__pre__order)."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-ups (raw: fiber__waitlist__sign__up)."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders (raw: fiber__web__orders)."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber e-commerce orders (raw: fiber_ec)."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber e-commerce orders (DDA) (raw: fiber_ec_dda)."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber secondary eligibility checks (raw: fiber_sec)."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber secondary eligibility checks (DDA) (raw: fiber_sec_dda)."),

  /* =============================
     HINT (Home Internet) + INVOCA
  ============================== */
  hint_ec FLOAT64 OPTIONS(description="HINT eligibility checks (raw: hint_ec)."),
  hint_sec FLOAT64 OPTIONS(description="HINT secondary eligibility checks (raw: hint_sec)."),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="Marketing automation HINT eligibility checks (raw: _ma_hint_ec__eligibility__check_)."),

  hint_invoca_calls FLOAT64 OPTIONS(description="HINT Invoca call events (raw: hint__invoca__calls)."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="HINT offline Invoca calls (raw: hint__offline__invoca__calls)."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="HINT offline Invoca eligibility events (raw: hint__offline__invoca__eligibility)."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="HINT offline Invoca order events (raw: hint__offline__invoca__order)."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="HINT offline Invoca real-time order events (raw: hint__offline__invoca__order_rt_)."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="HINT offline Invoca sales opportunities (raw: hint__offline__invoca__sales__opp)."),
  hint_web_orders FLOAT64 OPTIONS(description="HINT orders via web (raw: hint__web__orders)."),

  /* =============================
     TMO TOP/UPPER FUNNEL + PREPAID
  ============================== */
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top-funnel prospects (raw: tmo__top__funnel__prospect)."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper-funnel prospects (raw: tmo__upper__funnel__prospect)."),
  tmobile_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="Prepaid low-funnel prospects (raw: t__mobile__prepaid__low__funnel__prospect)."),

  /* =============================
     TFB (Unified for TBG + TFB)
  ============================== */
  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low-funnel conversions (unified: raw tbg__low__funnel)."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submissions (unified: raw tbg__lead__form__submit)."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB Invoca sales intent (DDA) (unified: raw tbg__invoca__sales__intent_dda)."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB Invoca orders (DDA) (unified: raw tbg__invoca__order_dda)."),

  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit check events (raw: tfb__credit__check)."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB HINT eligibility checks (raw: tfb_hint_ec)."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB Invoca sales calls (raw: tfb__invoca__sales__calls)."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads (raw: tfb__leads)."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic (raw: tfb__quality__traffic)."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions (raw: total_tfb__conversions)."),

  /* =============================
     OTHER
  ============================== */
  bronze_inserted_at TIMESTAMP OPTIONS(description="System timestamp when row was inserted/updated in Bronze.")
)
PARTITION BY report_date
CLUSTER BY account_id, campaign_id;
