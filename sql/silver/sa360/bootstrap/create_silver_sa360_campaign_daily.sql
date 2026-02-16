/*
===============================================================================
FILE: 00_create_sdi_silver_sa360_campaign_daily.sql
LAYER: Silver
TABLE: sdi_silver_sa360_campaign_daily

PURPOSE:
  Business-ready enriched daily campaign fact table for SA360 Paid Search.

GRAIN:
  account_id + campaign_id + date

SOURCE TABLES (Silver reads these Bronze tables):
  - prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily
  - prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

DESIGN PRINCIPLES:
  - Preserve business-relevant dimensions from Bronze (customer, client manager, resource_name)
  - Preserve meaningful metrics from Bronze (incl. bi / intent / connect / cart_start / digital_gross_add)
  - Enrich with latest campaign metadata from entity snapshot
  - Canonicalize TBG + TFB (tbg and tfb mean same) into unified "tfb_*" fields while keeping total_tfb_conversions
  - No ingestion-only raw fields (e.g., cost_micros not present in Silver)
  - Partition by date for performance; clustered for common filters

NOTES:
  - This file ONLY creates the Silver table schema with column descriptions.
  - The incremental MERGE (separate file) populates it.

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
(
  -- ============================================================
  -- GRAIN IDENTIFIERS
  -- ============================================================
  account_id STRING OPTIONS(description="SA360 advertiser account identifier."),
  account_name STRING OPTIONS(description="Advertiser account name associated with account_id."),
  campaign_id STRING OPTIONS(description="Unique campaign identifier within the SA360 account."),
  campaign_name STRING OPTIONS(description="Latest campaign name from Campaign Entity snapshot."),
  date DATE OPTIONS(description="Daily performance date derived from Bronze date_yyyymmdd (stored as DATE)."),

  -- ============================================================
  -- BUSINESS DIMENSIONS (not ingestion-only; used downstream)
  -- ============================================================
  customer_id STRING OPTIONS(description="Engine customer ID (Google Ads customer ID)."),
  customer_name STRING OPTIONS(description="Customer/account name."),
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID in SA360."),
  client_manager_name STRING OPTIONS(description="Client manager name in SA360."),
  resource_name STRING OPTIONS(description="Google Ads API resource name for the campaign (traceability)."),

  -- ============================================================
  -- CAMPAIGN CLASSIFICATION (derived/enriched)
  -- ============================================================
  campaign_type STRING OPTIONS(description="Derived campaign classification based on campaign_name pattern (Brand, Generic, Shopping, PMax, DemandGen, Unclassified)."),
  advertising_channel_type STRING OPTIONS(description="Primary advertising channel type (e.g., SEARCH, PERFORMANCE_MAX)."),
  advertising_channel_sub_type STRING OPTIONS(description="Advertising channel sub-type (may be empty)."),
  bidding_strategy_type STRING OPTIONS(description="Bidding strategy type (e.g., TARGET_ROAS, MAXIMIZE_CONVERSIONS)."),
  campaign_status STRING OPTIONS(description="Campaign status from entity snapshot (e.g., ENABLED, PAUSED, REMOVED)."),
  serving_status STRING OPTIONS(description="Campaign serving status indicating eligibility to deliver impressions."),

  -- ============================================================
  -- CORE PERFORMANCE METRICS
  -- ============================================================
  impressions FLOAT64 OPTIONS(description="Total ad impressions for the campaign on the given date."),
  clicks FLOAT64 OPTIONS(description="Total clicks for the campaign on the given date."),
  cost FLOAT64 OPTIONS(description="Campaign cost in account currency (Bronze cost_micros / 1,000,000)."),
  all_conversions FLOAT64 OPTIONS(description="Total conversions (includes modeled/cross-device where applicable)."),

  -- ============================================================
  -- INTENT / QUALITY / GENERIC BUSINESS METRICS (present in Bronze)
  -- ============================================================
  bi FLOAT64 OPTIONS(description="BI metric from source (business intent / internal score)."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent signal/score."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic metric."),
  digital_gross_add FLOAT64 OPTIONS(description="Digital gross adds."),
  cart_start FLOAT64 OPTIONS(description="Overall cart start events attributed to campaign."),

  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low-funnel visits attributed to campaign."),
  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low-funnel prospects attributed to campaign."),
  connect_qt FLOAT64 OPTIONS(description="Connect qualified traffic."),

  -- ============================================================
  -- POSTPAID METRICS
  -- ============================================================
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart start events attributed to campaign."),
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV events."),
  aal FLOAT64 OPTIONS(description="Add-a-Line related conversions/score."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-Line conversion events."),

  -- ============================================================
  -- HINT (Home Internet)
  -- ============================================================
  hint_ec FLOAT64 OPTIONS(description="Home Internet eligibility check events."),
  hint_sec FLOAT64 OPTIONS(description="Home Internet secondary eligibility check events."),
  hint_web_orders FLOAT64 OPTIONS(description="Home Internet web orders."),
  hint_invoca_calls FLOAT64 OPTIONS(description="Home Internet Invoca call events."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="Home Internet offline Invoca calls."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="Home Internet offline Invoca eligibility events."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="Home Internet offline Invoca order events."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="Home Internet offline Invoca real-time order events."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="Home Internet offline Invoca sales opportunities."),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="Marketing automation Home Internet eligibility checks."),

  -- ============================================================
  -- FIBER METRICS
  -- ============================================================
  fiber_activations FLOAT64 OPTIONS(description="Fiber activations."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-orders."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-ups."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber e-commerce orders/conversions."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber e-commerce conversions attributed via DDA."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber secondary eligibility checks."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber secondary eligibility checks attributed via DDA."),

  -- ============================================================
  -- METRO METRICS
  -- ============================================================
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top-of-funnel prospect events."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper-funnel prospect events."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid-funnel prospect events."),
  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low-funnel customer sign-up events."),
  metro_qt FLOAT64 OPTIONS(description="Metro qualified traffic."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro Home Internet qualified traffic."),

  -- ============================================================
  -- TMO METRICS
  -- ============================================================
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top-of-funnel prospects."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper-funnel prospects."),
  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="TMO prepaid low-funnel prospects."),

  -- ============================================================
  -- TFB METRICS (Canonical: includes TBG mappings)
  -- ============================================================
  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit check events."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB Invoca sales calls."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB HINT eligibility checks."),
  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low-funnel conversions (canonicalized from tbg__low__funnel when present)."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submits (canonicalized from tbg__lead__form__submit when present)."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB Invoca sales intent (DDA), canonicalized from tbg__invoca__sales__intent_dda when present."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB Invoca orders (DDA), canonicalized from tbg__invoca__order_dda when present."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions."),

  -- ============================================================
  -- OTHER METRICS
  -- ============================================================
  magenta_pqt FLOAT64 OPTIONS(description="Magenta pre-qualification tool completions."),

  -- ============================================================
  -- METADATA
  -- ============================================================
  file_load_datetime DATETIME OPTIONS(description="Bronze file load timestamp (used as lineage / freshness)."),
  silver_inserted_at TIMESTAMP OPTIONS(description="Timestamp when the record was inserted/updated in Silver.")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, campaign_type;



