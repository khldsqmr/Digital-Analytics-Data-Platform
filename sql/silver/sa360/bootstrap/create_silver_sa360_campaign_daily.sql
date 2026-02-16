/*
===============================================================================
FILE: 00_create_sdi_silver_sa360_campaign_daily.sql
LAYER: Silver
TABLE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily

PURPOSE:
  Business-ready enriched daily campaign fact table for SA360 Paid Search,
  with:
    - LOB derived from account_name
    - Ad Platform derived from account_name
    - Campaign metadata from latest entity snapshot (campaign_name etc.)

GRAIN:
  account_id + campaign_id + date

SOURCES:
  - Bronze Daily:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily
  - Bronze Entity: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

PARTITIONING / CLUSTERING:
  - PARTITION BY date (daily fact table best practice)
  - CLUSTER BY (max 4 fields allowed in BigQuery):
      account_id, campaign_id, lob, ad_platform

NOTE:
  BigQuery allows up to 4 clustering fields — so we intentionally do NOT add campaign_type
  into clustering even though it is commonly filtered.

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
(
  -- ============================================================
  -- GRAIN IDENTIFIERS (MANDATORY)
  -- ============================================================
  account_id STRING OPTIONS(description="SA360 account identifier."),
  account_name STRING OPTIONS(description="Account name (e.g., 'Postpaid Google')."),
  campaign_id STRING OPTIONS(description="Campaign identifier."),
  campaign_name STRING OPTIONS(description="Latest campaign name from entity snapshot."),
  date DATE OPTIONS(description="Daily performance date."),

  -- ============================================================
  -- BUSINESS DIMENSIONS DERIVED FROM account_name (MANDATORY)
  -- ============================================================
  lob STRING OPTIONS(description="LOB derived from account_name (Postpaid | HSI | Fiber | Metro | TFB)."),
  ad_platform STRING OPTIONS(description="Ad platform derived from account_name (Google | Bing)."),

  -- ============================================================
  -- CAMPAIGN METADATA (from latest entity snapshot)
  -- ============================================================
  campaign_type STRING OPTIONS(description="Derived from campaign_name (Brand | Generic | Shopping | PMax | DemandGen | Unclassified)."),
  advertising_channel_type STRING OPTIONS(description="Entity: advertising_channel_type."),
  advertising_channel_sub_type STRING OPTIONS(description="Entity: advertising_channel_sub_type."),
  bidding_strategy_type STRING OPTIONS(description="Entity: bidding_strategy_type."),
  campaign_status STRING OPTIONS(description="Entity: campaign_status."),
  serving_status STRING OPTIONS(description="Entity: serving_status."),

  -- ============================================================
  -- OPTIONAL BUSINESS ATTRIBUTES (present in Bronze Daily)
  -- ============================================================
  customer_id STRING OPTIONS(description="Daily: customer_id (if present in Bronze)."),
  customer_name STRING OPTIONS(description="Daily: customer_name (if present in Bronze)."),
  client_manager_id STRING OPTIONS(description="Daily: client_manager_id (if present in Bronze)."),
  client_manager_name STRING OPTIONS(description="Daily: client_manager_name (if present in Bronze)."),
  resource_name STRING OPTIONS(description="Daily: resource_name (if present in Bronze)."),

  -- ============================================================
  -- CORE PERFORMANCE METRICS (MANDATORY)
  -- ============================================================
  impressions FLOAT64 OPTIONS(description="Daily impressions."),
  clicks FLOAT64 OPTIONS(description="Daily clicks."),
  cost FLOAT64 OPTIONS(description="Daily cost in account currency (already converted in Bronze)."),
  all_conversions FLOAT64 OPTIONS(description="Daily all_conversions."),

  -- ============================================================
  -- QUALITY / INTENT / GENERIC METRICS
  -- ============================================================
  bi FLOAT64 OPTIONS(description="BI metric."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent metric."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic metric."),
  digital_gross_add FLOAT64 OPTIONS(description="Digital gross add metric."),
  magenta_pqt FLOAT64 OPTIONS(description="Magenta PQT metric."),

  -- ============================================================
  -- CART START (MANDATORY FAMILY)
  -- ============================================================
  cart_start FLOAT64 OPTIONS(description="Generic cart start events."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart start events."),

  -- ============================================================
  -- POSTPAID + PSPV (MANDATORY FAMILY)
  -- ============================================================
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV events."),
  aal FLOAT64 OPTIONS(description="Add-a-line related conversions."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-line events."),

  -- ============================================================
  -- CONNECT METRICS (if applicable / present)
  -- ============================================================
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low-funnel visit events."),
  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low-funnel prospect events."),
  connect_qt FLOAT64 OPTIONS(description="Connect qualified traffic metric."),

  -- ============================================================
  -- HINT / HSI (MANDATORY FAMILY)
  -- ============================================================
  hint_ec FLOAT64 OPTIONS(description="HINT eligibility check events."),
  hint_sec FLOAT64 OPTIONS(description="HINT secondary eligibility check events."),
  hint_web_orders FLOAT64 OPTIONS(description="HINT web orders."),
  hint_invoca_calls FLOAT64 OPTIONS(description="HINT Invoca calls."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="HINT offline Invoca calls."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="HINT offline Invoca eligibility."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="HINT offline Invoca order."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="HINT offline Invoca order RT."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="HINT offline Invoca sales opp."),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="Marketing automation HINT EC eligibility check."),

  -- ============================================================
  -- FIBER (MANDATORY FAMILY)
  -- ============================================================
  fiber_activations FLOAT64 OPTIONS(description="Fiber activations."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-orders."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-ups."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber EC conversions."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber EC conversions (DDA)."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber SEC events."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber SEC events (DDA)."),

  -- ============================================================
  -- METRO (MANDATORY FAMILY)
  -- ============================================================
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top funnel prospect."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper funnel prospect."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid funnel prospect."),
  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low funnel CS."),
  metro_qt FLOAT64 OPTIONS(description="Metro qualified traffic."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro HINT qualified traffic."),

  -- ============================================================
  -- TMO FUNNEL (MANDATORY FAMILY)
  -- ============================================================
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top funnel prospect."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper funnel prospect."),
  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="TMO prepaid low funnel prospect."),

  -- ============================================================
  -- TFB + TBG→TFB (MANDATORY FAMILY)
  -- (Assumes Bronze already mapped TBG fields into these standardized TFB fields.)
  -- ============================================================
  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit check."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB Invoca sales calls."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB HINT EC."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions."),
  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low funnel (includes mapped TBG low funnel)."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submit (includes mapped TBG)."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB invoca sales intent DDA (includes mapped TBG)."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB invoca order DDA (includes mapped TBG)."),

  -- ============================================================
  -- METADATA (MANDATORY)
  -- ============================================================
  file_load_datetime DATETIME OPTIONS(description="Bronze daily file load timestamp."),
  silver_inserted_at TIMESTAMP OPTIONS(description="Silver insert/update timestamp.")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, lob, ad_platform;
