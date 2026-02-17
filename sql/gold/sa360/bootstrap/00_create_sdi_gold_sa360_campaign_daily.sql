/*
===============================================================================
FILE: 00_create_sdi_gold_sa360_campaign_daily.sql
LAYER: Gold

TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily

PURPOSE:
  Dashboard-ready DAILY campaign performance table.
  Gold Daily is curated (handpicked dimensions + full metric coverage).
  No aggregation expected because Silver already enforces the daily grain.

GRAIN:
  account_id + campaign_id + date

DESIGN NOTES:
  - Retains metric types (FLOAT64 for metrics) as upstream
  - Keeps minimal lineage fields: file_load_datetime, gold_inserted_at
  - Naming consistency: uses tmo_prepaid_low_funnel_prospect (NOT t_mobile...)

PARTITION / CLUSTER:
  - PARTITION BY date
  - CLUSTER BY lob, ad_platform, account_id, campaign_id (max 4)
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
(
  -- ============================================================
  -- GRAIN IDENTIFIERS
  -- ============================================================
  account_id STRING OPTIONS(description="SA360 account identifier."),
  campaign_id STRING OPTIONS(description="Campaign identifier within account."),
  date DATE OPTIONS(description="Daily performance date (canonical)."),

  -- ============================================================
  -- DASHBOARD DIMENSIONS
  -- ============================================================
  account_name STRING OPTIONS(description="Account name / advertiser name (from Silver)."),
  lob STRING OPTIONS(description="Line of Business derived in Silver (Postpaid | HSI | Fiber | Metro | TFB | Unclassified)."),
  ad_platform STRING OPTIONS(description="Ad platform derived in Silver (Google | Bing | Unknown)."),

  campaign_name STRING OPTIONS(description="Campaign name (as-of entity snapshot via Silver)."),
  campaign_type STRING OPTIONS(description="Derived campaign classification (Brand | Generic | Shopping | PMax | DemandGen | Unclassified)."),

  advertising_channel_type STRING OPTIONS(description="SA360 entity advertising_channel_type (from Silver)."),
  advertising_channel_sub_type STRING OPTIONS(description="SA360 entity advertising_channel_sub_type (from Silver)."),
  bidding_strategy_type STRING OPTIONS(description="SA360 entity bidding_strategy_type (from Silver)."),
  serving_status STRING OPTIONS(description="SA360 entity serving_status (from Silver)."),

  -- ============================================================
  -- OPTIONAL DRILLDOWN IDENTIFIERS
  -- ============================================================
  customer_id STRING OPTIONS(description="Customer ID (from Silver)."),
  customer_name STRING OPTIONS(description="Customer name (from Silver)."),
  resource_name STRING OPTIONS(description="Google Ads resource name (from Silver)."),
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID (matches upstream type)."),
  client_manager_name STRING OPTIONS(description="Client manager name (from Silver)."),

  -- ============================================================
  -- CORE PERFORMANCE METRICS
  -- ============================================================
  impressions FLOAT64 OPTIONS(description="Daily impressions."),
  clicks FLOAT64 OPTIONS(description="Daily clicks."),
  cost FLOAT64 OPTIONS(description="Daily cost in standard currency units."),
  all_conversions FLOAT64 OPTIONS(description="Daily all conversions."),

  -- ============================================================
  -- QUALITY / INTENT / GENERIC
  -- ============================================================
  bi FLOAT64 OPTIONS(description="BI metric."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent signal."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic."),
  digital_gross_add FLOAT64 OPTIONS(description="Digital gross adds."),
  magenta_pqt FLOAT64 OPTIONS(description="Magenta PQT completions."),

  -- ============================================================
  -- CART / POSTPAID / PSPV / AAL
  -- ============================================================
  cart_start FLOAT64 OPTIONS(description="Cart start events."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart start events."),
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV events."),
  aal FLOAT64 OPTIONS(description="AAL metric."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-line metric."),

  -- ============================================================
  -- CONNECT
  -- ============================================================
  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low-funnel prospects."),
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low-funnel visits."),
  connect_qt FLOAT64 OPTIONS(description="Connect qualified traffic."),

  -- ============================================================
  -- HINT / HSI
  -- ============================================================
  hint_ec FLOAT64 OPTIONS(description="HINT EC."),
  hint_sec FLOAT64 OPTIONS(description="HINT SEC."),
  hint_web_orders FLOAT64 OPTIONS(description="HINT web orders."),
  hint_invoca_calls FLOAT64 OPTIONS(description="HINT Invoca calls."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="HINT offline Invoca calls."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="HINT offline Invoca eligibility."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="HINT offline Invoca orders."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="HINT offline Invoca orders RT."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="HINT offline Invoca sales opp."),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="MA HINT EC eligibility check."),

  -- ============================================================
  -- FIBER
  -- ============================================================
  fiber_activations FLOAT64 OPTIONS(description="Fiber activations."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-orders."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-ups."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber EC."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber EC DDA."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber SEC."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber SEC DDA."),

  -- ============================================================
  -- METRO
  -- ============================================================
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top-funnel prospects."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper-funnel prospects."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid-funnel prospects."),
  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low-funnel CS."),
  metro_qt FLOAT64 OPTIONS(description="Metro qualified traffic."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro HINT qualified traffic."),

  -- ============================================================
  -- TMO
  -- ============================================================
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top-funnel prospects."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper-funnel prospects."),
  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="TMO prepaid low-funnel prospects (standardized)."),

  -- ============================================================
  -- TFB (includes TBG mapped upstream)
  -- ============================================================
  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit checks."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB Invoca sales calls."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB HINT EC."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions."),
  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low funnel."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submit."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB Invoca sales intent DDA."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB Invoca order DDA."),

  -- ============================================================
  -- LINEAGE / METADATA
  -- ============================================================
  file_load_datetime DATETIME OPTIONS(description="Upstream file load timestamp (from Bronze/Silver)."),
  gold_inserted_at TIMESTAMP OPTIONS(description="Timestamp when record was inserted/updated in Gold.")
)
PARTITION BY date
CLUSTER BY lob, ad_platform, account_id, campaign_id
OPTIONS(
  description = "Gold SA360 campaign DAILY dashboard table. Curated dimensions + full metric coverage. Partitioned by date."
);
