/*
===============================================================================
FILE: 01_create_sdi_gold_sa360_campaign_daily.sql
LAYER: Gold (Daily)
TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily

PURPOSE:
  Dashboard-ready daily campaign performance table.
  Gold is curated but stays at the same grain as Silver:
    (account_id, campaign_id, date)

DESIGN PRINCIPLES:
  - Gold should not rename columns if Silver is already canonicalized.
  - Gold should be safe for BI tools: stable schema, partitioned by date.

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
  account_id STRING OPTIONS(description="SA360 advertiser account ID."),
  account_name STRING OPTIONS(description="Account name (from Silver)."),
  campaign_id STRING OPTIONS(description="Campaign ID."),
  campaign_name STRING OPTIONS(description="Campaign name (from Silver enrichment)."),
  date DATE OPTIONS(description="Canonical reporting date (daily grain)."),

  -- =========================
  -- Dimensions (curated)
  -- =========================
  lob STRING OPTIONS(description="LOB derived in Silver."),
  ad_platform STRING OPTIONS(description="Platform derived in Silver (Google/Bing)."),
  campaign_type STRING OPTIONS(description="Campaign classification derived in Silver."),

  advertising_channel_type STRING OPTIONS(description="From Silver entity."),
  advertising_channel_sub_type STRING OPTIONS(description="From Silver entity."),
  bidding_strategy_type STRING OPTIONS(description="From Silver entity."),
  serving_status STRING OPTIONS(description="From Silver entity."),

  -- =========================
  -- Drilldowns
  -- =========================
  customer_id STRING OPTIONS(description="Google Ads customer ID (from Silver)."),
  customer_name STRING OPTIONS(description="Customer name (from Silver)."),
  resource_name STRING OPTIONS(description="Resource name (from Silver)."),
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID (from Silver)."),
  client_manager_name STRING OPTIONS(description="Client manager name (from Silver)."),

  -- =========================
  -- Core metrics
  -- =========================
  impressions FLOAT64 OPTIONS(description="Daily impressions."),
  clicks FLOAT64 OPTIONS(description="Daily clicks."),
  cost FLOAT64 OPTIONS(description="Daily cost."),
  all_conversions FLOAT64 OPTIONS(description="Daily all conversions."),

  -- =========================
  -- Intent / quality
  -- =========================
  bi FLOAT64 OPTIONS(description="BI metric."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent metric."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic."),
  digital_gross_add FLOAT64 OPTIONS(description="Digital gross adds."),
  magenta_pqt FLOAT64 OPTIONS(description="Magenta PQT completions."),

  -- =========================
  -- Cart / Postpaid
  -- =========================
  cart_start FLOAT64 OPTIONS(description="Cart start."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart start."),
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV."),
  aal FLOAT64 OPTIONS(description="AAL."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-line."),

  -- =========================
  -- Connect
  -- =========================
  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low funnel prospect."),
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low funnel visit."),
  connect_qt FLOAT64 OPTIONS(description="Connect qualified traffic."),

  -- =========================
  -- HINT / HSI
  -- =========================
  hint_ec FLOAT64 OPTIONS(description="HINT EC."),
  hint_sec FLOAT64 OPTIONS(description="HINT SEC."),
  hint_web_orders FLOAT64 OPTIONS(description="HINT web orders."),
  hint_invoca_calls FLOAT64 OPTIONS(description="HINT invoca calls."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="HINT offline invoca calls."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="HINT offline eligibility."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="HINT offline order."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="HINT offline order RT."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="HINT offline sales opp."),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="MA HINT EC eligibility check."),

  -- =========================
  -- Fiber
  -- =========================
  fiber_activations FLOAT64 OPTIONS(description="Fiber activations."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-order."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-up."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber EC."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber EC DDA."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber SEC."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber SEC DDA."),

  -- =========================
  -- Metro
  -- =========================
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top funnel prospect."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper funnel prospect."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid funnel prospect."),
  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low funnel CS."),
  metro_qt FLOAT64 OPTIONS(description="Metro QT."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro hint QT."),

  -- =========================
  -- TMO
  -- =========================
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top funnel prospect."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper funnel prospect."),
  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="TMO prepaid low funnel prospect."),

  -- =========================
  -- TFB
  -- =========================
  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit check."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB invoca sales calls."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB hint EC."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions."),
  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low funnel."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submit."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB invoca sales intent DDA."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB invoca order DDA."),

  -- =========================
  -- Lineage
  -- =========================
  file_load_datetime DATETIME OPTIONS(description="Upstream file load datetime."),
  gold_inserted_at TIMESTAMP OPTIONS(description="Gold load timestamp.")
)
PARTITION BY date
CLUSTER BY lob, ad_platform, account_id, campaign_id
OPTIONS(description="Gold SA360 Daily table derived from Silver (same grain).");
