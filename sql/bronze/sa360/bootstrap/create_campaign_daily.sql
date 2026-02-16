/*
===============================================================================
BOOTSTRAP | BRONZE | SA 360 | CAMPAIGN DAILY (ONE-TIME FULL BUILD)
===============================================================================

PURPOSE
-------
Create the Bronze Campaign Daily table from the raw Improvado
Search Ads 360 campaign export.

This table is intentionally:
  • LOSSLESS (all raw metrics preserved)
  • MINIMALLY TRANSFORMED (Bronze principle)
  • STRUCTURED for downstream Silver logic
  • SAFE for incremental MERGE

SOURCE TABLE
------------
prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

TARGET TABLE
------------
prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily

GRAIN
-----
account_id + campaign_id + date_yyyymmdd

PARTITION
---------
date  (Parsed from date_yyyymmdd)

CLUSTER
-------
account_id, campaign_id

DESIGN PRINCIPLES
-----------------
1. No metric dropped
2. No business logic applied
3. Naming standardized (snake_case)
4. Cost converted from micros
5. Raw metadata preserved
6. Ready for Silver modeling
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
(

  /* ========================================================================
     CORE IDENTIFIERS
     ======================================================================== */

  account_id STRING OPTIONS(description='Search Ads 360 advertiser account ID'),
  account_name STRING OPTIONS(description='Advertiser account name'),
  campaign_id STRING OPTIONS(description='Campaign ID'),
  resource_name STRING OPTIONS(description='Google Ads API resource name'),
  customer_id STRING OPTIONS(description='Google Ads customer ID'),
  customer_name STRING OPTIONS(description='Customer account name'),
  client_manager_id FLOAT64 OPTIONS(description='Client manager ID'),
  client_manager_name STRING OPTIONS(description='Client manager name'),

  /* ========================================================================
     DATE FIELDS
     ======================================================================== */

  date_yyyymmdd STRING OPTIONS(description='Raw date in YYYYMMDD format'),
  raw_numeric_date INT64 OPTIONS(description='Raw numeric date value from source'),
  date DATE OPTIONS(description='Parsed DATE derived from date_yyyymmdd'),
  segments_date DATE OPTIONS(description='Segments date dimension'),

  /* ========================================================================
     CORE PERFORMANCE METRICS
     ======================================================================== */

  clicks FLOAT64 OPTIONS(description='Total clicks'),
  impressions FLOAT64 OPTIONS(description='Total impressions'),
  cost_micros FLOAT64 OPTIONS(description='Cost in micros'),
  cost FLOAT64 OPTIONS(description='Cost converted from micros to currency'),
  all_conversions FLOAT64 OPTIONS(description='All conversions'),

  /* ========================================================================
     GENERAL METRICS
     ======================================================================== */

  aal FLOAT64 OPTIONS(description='Add-a-line conversions'),
  add_a_line FLOAT64 OPTIONS(description='Add-a-line explicit conversions'),
  bi FLOAT64 OPTIONS(description='Buying intent metric'),
  buying_intent FLOAT64 OPTIONS(description='Buying intent conversions'),
  bts_quality_traffic FLOAT64 OPTIONS(description='BTS quality traffic'),
  digital_gross_add FLOAT64 OPTIONS(description='Digital gross add metric'),

  /* ========================================================================
     CART / CHECKOUT
     ======================================================================== */

  cart_start FLOAT64 OPTIONS(description='Generic cart start events'),
  postpaid_cart_start FLOAT64 OPTIONS(description='Postpaid cart start events'),
  postpaid_pspv FLOAT64 OPTIONS(description='Postpaid PSPV metric'),

  /* ========================================================================
     CONNECT
     ======================================================================== */

  connect_low_funnel_visit FLOAT64 OPTIONS(description='Connect low funnel visits'),
  connect_low_funnel_prospect FLOAT64 OPTIONS(description='Connect low funnel prospects'),
  connect_qt FLOAT64 OPTIONS(description='Connect qualified traffic'),

  /* ========================================================================
     HINT (HOME INTERNET)
     ======================================================================== */

  hint_ec FLOAT64 OPTIONS(description='Home Internet eligibility checks'),
  hint_sec FLOAT64 OPTIONS(description='Home Internet secondary eligibility'),
  hint_web_orders FLOAT64 OPTIONS(description='HINT web orders'),
  hint_invoca_calls FLOAT64 OPTIONS(description='HINT Invoca calls'),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description='HINT offline calls'),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description='HINT offline eligibility'),
  hint_offline_invoca_order FLOAT64 OPTIONS(description='HINT offline order'),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description='HINT offline RT order'),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description='HINT offline sales opp'),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description='Marketing automation HINT eligibility'),

  /* ========================================================================
     FIBER
     ======================================================================== */

  fiber_activations FLOAT64 OPTIONS(description='Fiber activations'),
  fiber_pre_order FLOAT64 OPTIONS(description='Fiber pre-orders'),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description='Fiber waitlist sign-ups'),
  fiber_ec FLOAT64 OPTIONS(description='Fiber e-commerce orders'),
  fiber_ec_dda FLOAT64 OPTIONS(description='Fiber e-commerce DDA'),
  fiber_web_orders FLOAT64 OPTIONS(description='Fiber web orders'),
  fiber_sec FLOAT64 OPTIONS(description='Fiber secondary eligibility'),
  fiber_sec_dda FLOAT64 OPTIONS(description='Fiber secondary eligibility DDA'),

  /* ========================================================================
     METRO
     ======================================================================== */

  metro_top_funnel_prospect FLOAT64 OPTIONS(description='Metro top funnel prospects'),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description='Metro upper funnel prospects'),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description='Metro mid funnel prospects'),
  metro_low_funnel_cs FLOAT64 OPTIONS(description='Metro low funnel customer signups'),
  metro_qt FLOAT64 OPTIONS(description='Metro qualified traffic'),
  metro_hint_qt FLOAT64 OPTIONS(description='Metro HINT qualified traffic'),

  /* ========================================================================
     TMO
     ======================================================================== */

  tmo_top_funnel_prospect FLOAT64 OPTIONS(description='TMO top funnel prospects'),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description='TMO upper funnel prospects'),
  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description='TMO prepaid low funnel prospects'),

  /* ========================================================================
     TFB / TBG (Same Business Domain)
     ======================================================================== */

  tbg_low_funnel FLOAT64 OPTIONS(description='TFB low funnel events'),
  tbg_lead_form_submit FLOAT64 OPTIONS(description='TFB lead form submissions'),
  tbg_invoca_sales_intent_dda FLOAT64 OPTIONS(description='TFB Invoca sales intent DDA'),
  tbg_invoca_order_dda FLOAT64 OPTIONS(description='TFB Invoca order DDA'),

  tfb_credit_check FLOAT64 OPTIONS(description='TFB credit check events'),
  tfb_hint_ec FLOAT64 OPTIONS(description='TFB HINT eligibility'),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description='TFB Invoca sales calls'),
  tfb_leads FLOAT64 OPTIONS(description='TFB leads'),
  tfb_quality_traffic FLOAT64 OPTIONS(description='TFB quality traffic'),
  total_tfb_conversions FLOAT64 OPTIONS(description='Total TFB conversions'),

  /* ========================================================================
     OTHER METRICS
     ======================================================================== */

  magenta_pqt FLOAT64 OPTIONS(description='Magenta PQT metric'),

  /* ========================================================================
     LOAD METADATA
     ======================================================================== */

  __insert_date INT64 OPTIONS(description='Technical insert date identifier'),
  file_load_datetime DATETIME OPTIONS(description='File load timestamp'),
  filename STRING OPTIONS(description='Source file path'),
  bronze_inserted_at TIMESTAMP OPTIONS(description='Bronze ingestion timestamp')

)
PARTITION BY date
CLUSTER BY account_id, campaign_id;
