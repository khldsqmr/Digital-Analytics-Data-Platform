/*
===============================================================================
FILE: 01_create_sdi_gold_sa360_campaign_daily.sql
LAYER: Gold (Daily)
TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily

PURPOSE:
  Dashboard-ready DAILY campaign performance table.
  Gold Daily is a curated pass-through from Silver (same daily grain),
  with a stable schema for reporting.

GRAIN:
  account_id + campaign_id + date

PARTITION / CLUSTER:
  - PARTITION BY date
  - CLUSTER BY lob, ad_platform, account_id, campaign_id
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
(
  -- =========================
  -- Grain identifiers
  -- =========================
  account_id STRING OPTIONS(description="SA360 advertiser account ID (from Silver)."),
  account_name STRING OPTIONS(description="Account name (from Silver)."),
  campaign_id STRING OPTIONS(description="Campaign identifier (from Silver)."),
  campaign_name STRING OPTIONS(description="Campaign name (from Silver enrichment)."),
  date DATE OPTIONS(description="Canonical daily performance date (DATE)."),

  -- =========================
  -- Dimensions
  -- =========================
  lob STRING OPTIONS(description="LOB (from Silver)."),
  ad_platform STRING OPTIONS(description="Ad platform (from Silver)."),
  campaign_type STRING OPTIONS(description="Campaign classification (from Silver)."),

  advertising_channel_type STRING OPTIONS(description="From Silver entity enrichment."),
  advertising_channel_sub_type STRING OPTIONS(description="From Silver entity enrichment."),
  bidding_strategy_type STRING OPTIONS(description="From Silver entity enrichment."),
  serving_status STRING OPTIONS(description="From Silver entity enrichment."),

  -- =========================
  -- Drilldowns
  -- =========================
  customer_id STRING OPTIONS(description="Customer ID (from Silver)."),
  customer_name STRING OPTIONS(description="Customer name (from Silver)."),
  resource_name STRING OPTIONS(description="API resource name (from Silver)."),
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID (from Silver)."),
  client_manager_name STRING OPTIONS(description="Client manager name (from Silver)."),

  -- =========================
  -- Core metrics
  -- =========================
  impressions FLOAT64 OPTIONS(description="Daily impressions (from Silver)."),
  clicks FLOAT64 OPTIONS(description="Daily clicks (from Silver)."),
  cost FLOAT64 OPTIONS(description="Daily cost (from Silver)."),
  all_conversions FLOAT64 OPTIONS(description="Daily all_conversions (from Silver)."),

  -- =========================
  -- Intent / quality
  -- =========================
  bi FLOAT64 OPTIONS(description="BI metric (from Silver)."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent (from Silver)."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic (from Silver)."),
  digital_gross_add FLOAT64 OPTIONS(description="Digital gross adds (from Silver)."),
  magenta_pqt FLOAT64 OPTIONS(description="Magenta PQT (from Silver)."),

  -- =========================
  -- Cart / Postpaid
  -- =========================
  cart_start FLOAT64 OPTIONS(description="Cart start (from Silver)."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart start (from Silver)."),
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV (from Silver)."),
  aal FLOAT64 OPTIONS(description="AAL (from Silver)."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-line (from Silver)."),

  -- =========================
  -- Connect
  -- =========================
  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low funnel prospect (from Silver)."),
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low funnel visit (from Silver)."),
  connect_qt FLOAT64 OPTIONS(description="Connect QT (from Silver)."),

  -- =========================
  -- HINT / HSI
  -- =========================
  hint_ec FLOAT64 OPTIONS(description="HINT EC (from Silver)."),
  hint_sec FLOAT64 OPTIONS(description="HINT SEC (from Silver)."),
  hint_web_orders FLOAT64 OPTIONS(description="HINT web orders (from Silver)."),
  hint_invoca_calls FLOAT64 OPTIONS(description="HINT invoca calls (from Silver)."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="HINT offline invoca calls (from Silver)."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="HINT offline eligibility (from Silver)."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="HINT offline order (from Silver)."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="HINT offline order RT (from Silver)."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="HINT offline sales opp (from Silver)."),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="MA hint EC eligibility check (from Silver)."),

  -- =========================
  -- Fiber
  -- =========================
  fiber_activations FLOAT64 OPTIONS(description="Fiber activations (from Silver)."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-order (from Silver)."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-up (from Silver)."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders (from Silver)."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber EC (from Silver)."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber EC DDA (from Silver)."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber SEC (from Silver)."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber SEC DDA (from Silver)."),

  -- =========================
  -- Metro
  -- =========================
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top funnel prospect (from Silver)."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper funnel prospect (from Silver)."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid funnel prospect (from Silver)."),
  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low funnel cs (from Silver)."),
  metro_qt FLOAT64 OPTIONS(description="Metro QT (from Silver)."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro hint QT (from Silver)."),

  -- =========================
  -- TMO
  -- =========================
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top funnel prospect (from Silver)."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper funnel prospect (from Silver)."),
  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="TMO prepaid low funnel prospect (from Silver)."),

  -- =========================
  -- TFB
  -- =========================
  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit check (from Silver)."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB invoca sales calls (from Silver)."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads (from Silver)."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic (from Silver)."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB hint EC (from Silver)."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions (from Silver)."),
  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low funnel (from Silver)."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submit (from Silver)."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB invoca sales intent DDA (from Silver)."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB invoca order DDA (from Silver)."),

  -- =========================
  -- Lineage
  -- =========================
  file_load_datetime DATETIME OPTIONS(description="Upstream load datetime (from Silver)."),
  gold_inserted_at TIMESTAMP OPTIONS(description="Gold load timestamp.")
)
PARTITION BY date
CLUSTER BY lob, ad_platform, account_id, campaign_id
OPTIONS(description="Gold SA360 DAILY dashboard table derived from Silver. Partitioned by date.");
