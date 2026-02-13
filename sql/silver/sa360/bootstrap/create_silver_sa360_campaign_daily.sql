/*
===============================================================================
FILE: create_sdi_silver_sa360_campaign_daily.sql
LAYER: Silver
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Business-ready enriched daily campaign fact table for SA360 Paid Search.

GRAIN:
  account_id + campaign_id + date

SOURCE TABLES:
  - sdi_bronze_sa360_campaign_daily
  - sdi_bronze_sa360_campaign_entity

DESIGN PRINCIPLES:
  - Cleaned metric names inherited from Bronze
  - Enriched with latest campaign metadata
  - Business-focused descriptions
  - No ingestion-only technical fields (e.g., cost_micros removed)
  - No derived calendar breakdown columns (handled downstream)
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
(

  -- ============================================================
  -- GRAIN IDENTIFIERS
  -- ============================================================

  account_id STRING OPTIONS(description=
    "Search Ads 360 advertiser account identifier."),

  account_name STRING OPTIONS(description=
    "Advertiser account name associated with account_id."),

  campaign_id STRING OPTIONS(description=
    "Unique campaign identifier within the SA360 account."),

  campaign_name STRING OPTIONS(description=
    "Latest campaign name from Campaign Entity snapshot."),

  date DATE OPTIONS(description=
    "Daily performance date for campaign metrics."),

  -- ============================================================
  -- CAMPAIGN CLASSIFICATION
  -- ============================================================

  campaign_type STRING OPTIONS(description=
    "Derived campaign classification based on campaign_name pattern (Brand, Generic, Shopping, PMax, DemandGen, Unclassified)."),

  advertising_channel_type STRING OPTIONS(description=
    "Primary advertising channel type from SA360 (e.g., SEARCH, PERFORMANCE_MAX)."),

  advertising_channel_sub_type STRING OPTIONS(description=
    "Advertising channel sub-type providing additional classification."),

  bidding_strategy_type STRING OPTIONS(description=
    "Bidding strategy type applied to the campaign (e.g., TARGET_ROAS, TARGET_CPA)."),

  campaign_status STRING OPTIONS(description=
    "Campaign lifecycle status from entity snapshot (e.g., ENABLED, PAUSED, REMOVED)."),

  serving_status STRING OPTIONS(description=
    "Campaign serving status indicating eligibility to deliver impressions."),

  -- ============================================================
  -- CORE PERFORMANCE METRICS
  -- ============================================================

  impressions FLOAT64 OPTIONS(description=
    "Total ad impressions recorded for the campaign on the given date."),

  clicks FLOAT64 OPTIONS(description=
    "Total clicks recorded for the campaign on the given date."),

  cost FLOAT64 OPTIONS(description=
    "Campaign cost in account currency. Derived from Bronze cost_micros / 1,000,000."),

  all_conversions FLOAT64 OPTIONS(description=
    "Total conversions including cross-device and modeled conversions."),

  -- ============================================================
  -- POSTPAID METRICS
  -- ============================================================

  postpaid_cart_start FLOAT64 OPTIONS(description=
    "Postpaid cart start events attributed to campaign."),

  postpaid_pspv FLOAT64 OPTIONS(description=
    "Postpaid Product Specific Page View (PSPV) events."),

  aal FLOAT64 OPTIONS(description=
    "Add-a-Line related conversions."),

  add_a_line FLOAT64 OPTIONS(description=
    "Add-a-Line conversion events."),

  -- ============================================================
  -- HINT (Home Internet)
  -- ============================================================

  hint_ec FLOAT64 OPTIONS(description=
    "Home Internet eligibility check events."),

  hint_sec FLOAT64 OPTIONS(description=
    "Home Internet secondary eligibility check events."),

  hint_web_orders FLOAT64 OPTIONS(description=
    "Home Internet web order events."),

  hint_invoca_calls FLOAT64 OPTIONS(description=
    "Home Internet tracked Invoca inbound calls."),

  hint_offline_invoca_calls FLOAT64 OPTIONS(description=
    "Home Internet offline attributed Invoca calls."),

  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description=
    "Offline eligibility interactions from Invoca tracking."),

  hint_offline_invoca_order FLOAT64 OPTIONS(description=
    "Offline order conversions tracked via Invoca."),

  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description=
    "Real-time offline order events from Invoca integration."),

  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description=
    "Offline sales opportunity events from Invoca."),

  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description=
    "Marketing automation Home Internet eligibility check events."),

  -- ============================================================
  -- FIBER METRICS
  -- ============================================================

  fiber_activations FLOAT64 OPTIONS(description=
    "Fiber activation events."),

  fiber_pre_order FLOAT64 OPTIONS(description=
    "Fiber pre-order events."),

  fiber_waitlist_sign_up FLOAT64 OPTIONS(description=
    "Fiber waitlist sign-up events."),

  fiber_web_orders FLOAT64 OPTIONS(description=
    "Fiber web order events."),

  fiber_ec FLOAT64 OPTIONS(description=
    "Fiber e-commerce conversions."),

  fiber_ec_dda FLOAT64 OPTIONS(description=
    "Fiber e-commerce conversions attributed via DDA."),

  fiber_sec FLOAT64 OPTIONS(description=
    "Fiber secondary eligibility check events."),

  fiber_sec_dda FLOAT64 OPTIONS(description=
    "Fiber secondary eligibility checks attributed via DDA."),

  -- ============================================================
  -- METRO METRICS
  -- ============================================================

  metro_top_funnel_prospect FLOAT64 OPTIONS(description=
    "Metro top-of-funnel prospect events."),

  metro_upper_funnel_prospect FLOAT64 OPTIONS(description=
    "Metro upper-funnel prospect events."),

  metro_mid_funnel_prospect FLOAT64 OPTIONS(description=
    "Metro mid-funnel prospect events."),

  metro_low_funnel_cs FLOAT64 OPTIONS(description=
    "Metro low-funnel customer sign-up events."),

  metro_qt FLOAT64 OPTIONS(description=
    "Metro qualified traffic events."),

  metro_hint_qt FLOAT64 OPTIONS(description=
    "Metro Home Internet qualified traffic events."),

  -- ============================================================
  -- TMO METRICS
  -- ============================================================

  tmo_top_funnel_prospect FLOAT64 OPTIONS(description=
    "TMO top-of-funnel prospect events."),

  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description=
    "TMO upper-funnel prospect events."),

  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description=
    "TMO prepaid low-funnel prospect events."),

  -- ============================================================
  -- TFB METRICS
  -- ============================================================

  tfb_credit_check FLOAT64 OPTIONS(description=
    "TFB credit check events."),

  tfb_invoca_sales_calls FLOAT64 OPTIONS(description=
    "TFB sales calls tracked via Invoca."),

  tfb_leads FLOAT64 OPTIONS(description=
    "TFB lead generation events."),

  tfb_quality_traffic FLOAT64 OPTIONS(description=
    "TFB qualified traffic events."),

  tfb_hint_ec FLOAT64 OPTIONS(description=
    "TFB Home Internet eligibility check events."),

  total_tfb_conversions FLOAT64 OPTIONS(description=
    "Total TFB conversion events."),

  -- ============================================================
  -- OTHER METRICS
  -- ============================================================

  magenta_pqt FLOAT64 OPTIONS(description=
    "Magenta pre-qualification tool completion events."),

  -- ============================================================
  -- METADATA
  -- ============================================================

  file_load_datetime DATETIME OPTIONS(description=
    "Source file load timestamp from Bronze layer."),

  silver_inserted_at TIMESTAMP OPTIONS(description=
    "Timestamp when the record was inserted or updated in Silver layer.")

)
PARTITION BY date
CLUSTER BY account_id, campaign_id, campaign_type;
