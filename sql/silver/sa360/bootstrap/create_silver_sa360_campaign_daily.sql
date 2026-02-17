/*
===============================================================================
FILE: 00_create_sdi_silver_sa360_campaign_daily.sql
LAYER: Silver
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:  sdi_silver_sa360_campaign_daily

PURPOSE:
  Business-ready DAILY campaign fact table for SA360 Paid Search.

  - Metrics come from Bronze Daily (performance snapshot).
  - Campaign metadata comes from Bronze Entity (settings snapshot).
  - Entity enrichment uses AS-OF join (entity.date <= daily.date) to prevent
    future metadata leakage.
  - campaign_name uses safe fallback:
      If AS-OF campaign_name is NULL/blank -> use latest NON-NULL name for that campaign_id.
      If entity has no rows for that campaign_id -> campaign_name remains NULL.

GRAIN:
  account_id + campaign_id + date

SOURCES:
  - Bronze Daily:
      prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily
  - Bronze Entity:
      prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

PARTITION / CLUSTER:
  - PARTITION BY date
  - CLUSTER BY account_id, campaign_id, lob, ad_platform
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
(
  account_id STRING OPTIONS(description="SA360 advertiser account ID (from Bronze Daily)."),
  campaign_id STRING OPTIONS(description="Campaign ID (from Bronze Daily)."),
  date DATE OPTIONS(description="Canonical reporting DATE (from Bronze Daily)."),
  date_yyyymmdd STRING OPTIONS(description="Snapshot key in YYYYMMDD (from Bronze Daily; lineage/debug)."),

  account_name STRING OPTIONS(description="Account/advertiser name (from Bronze Daily)."),

  lob STRING OPTIONS(description=
    "LOB derived from account_name. Mapping: "
    "Postpaid Google/Postpaid Bing/BTS Google/BTS Bing -> Postpaid; "
    "Broadband Google/Broadband Bing -> HSI; "
    "Fiber Google/Fiber Bing -> Fiber; "
    "Metro Google/Metro Bing -> Metro; "
    "TFB Google/TFB Bing -> TFB; "
    "else Unclassified."
  ),

  ad_platform STRING OPTIONS(description=
    "Ad platform derived from account_name: contains 'Google' -> Google; contains 'Bing' -> Bing; else Unknown."
  ),

  campaign_name STRING OPTIONS(description="Campaign name from entity (AS-OF) with safe fallback to latest non-null."),

  campaign_type STRING OPTIONS(description=
    "Derived classification from campaign_name using regex patterns "
    "(Brand | Generic | Shopping | PMax | DemandGen | Unclassified)."
  ),

  advertising_channel_type STRING OPTIONS(description="Entity: advertising_channel_type."),
  advertising_channel_sub_type STRING OPTIONS(description="Entity: advertising_channel_sub_type."),
  bidding_strategy_type STRING OPTIONS(description="Entity: bidding_strategy_type."),
  serving_status STRING OPTIONS(description="Entity: serving_status."),

  customer_id STRING OPTIONS(description="Google Ads customer ID (from Bronze Daily)."),
  customer_name STRING OPTIONS(description="Customer name (from Bronze Daily)."),
  resource_name STRING OPTIONS(description="Google Ads API resource name (from Bronze Daily)."),
  segments_date STRING OPTIONS(description="Segments.date as string (from Bronze Daily)."),
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID (from Bronze Daily)."),
  client_manager_name STRING OPTIONS(description="Client manager name (from Bronze Daily)."),

  impressions FLOAT64 OPTIONS(description="Daily impressions (from Bronze Daily)."),
  clicks FLOAT64 OPTIONS(description="Daily clicks (from Bronze Daily)."),
  cost FLOAT64 OPTIONS(description="Daily cost (from Bronze Daily)."),
  all_conversions FLOAT64 OPTIONS(description="Daily all_conversions (from Bronze Daily)."),

  bi FLOAT64 OPTIONS(description="BI metric (from Bronze Daily)."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent metric (from Bronze Daily)."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic (from Bronze Daily)."),
  digital_gross_add FLOAT64 OPTIONS(description="Digital gross adds (from Bronze Daily)."),
  magenta_pqt FLOAT64 OPTIONS(description="Magenta PQT completions (from Bronze Daily)."),

  cart_start FLOAT64 OPTIONS(description="Cart start (from Bronze Daily)."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart start (from Bronze Daily)."),
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV (from Bronze Daily)."),
  aal FLOAT64 OPTIONS(description="AAL metric (from Bronze Daily)."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-line metric (from Bronze Daily)."),

  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low-funnel prospect (from Bronze Daily)."),
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low-funnel visit (from Bronze Daily)."),
  connect_qt FLOAT64 OPTIONS(description="Connect qualified traffic (from Bronze Daily)."),

  hint_ec FLOAT64 OPTIONS(description="HINT EC (from Bronze Daily)."),
  hint_sec FLOAT64 OPTIONS(description="HINT SEC (from Bronze Daily)."),
  hint_web_orders FLOAT64 OPTIONS(description="HINT web orders (from Bronze Daily)."),
  hint_invoca_calls FLOAT64 OPTIONS(description="HINT invoca calls (from Bronze Daily)."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="HINT offline invoca calls (from Bronze Daily)."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="HINT offline eligibility (from Bronze Daily)."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="HINT offline order (from Bronze Daily)."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="HINT offline order RT (from Bronze Daily)."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="HINT offline sales opp (from Bronze Daily)."),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="MA HINT EC eligibility check (from Bronze Daily)."),

  fiber_activations FLOAT64 OPTIONS(description="Fiber activations (from Bronze Daily)."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-order (from Bronze Daily)."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-up (from Bronze Daily)."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders (from Bronze Daily)."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber EC (from Bronze Daily)."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber EC DDA (from Bronze Daily)."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber SEC (from Bronze Daily)."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber SEC DDA (from Bronze Daily)."),

  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low funnel CS (from Bronze Daily)."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid funnel prospect (from Bronze Daily)."),
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top funnel prospect (from Bronze Daily)."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper funnel prospect (from Bronze Daily)."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro HINT QT (from Bronze Daily)."),
  metro_qt FLOAT64 OPTIONS(description="Metro QT (from Bronze Daily)."),

  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="TMO prepaid low funnel prospect (from Bronze Daily; standardized naming)."),
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top funnel prospect (from Bronze Daily)."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper funnel prospect (from Bronze Daily)."),

  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low funnel (standardized from TBG in Bronze)."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submit (standardized from TBG in Bronze)."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB invoca sales intent DDA (standardized from TBG in Bronze)."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB invoca order DDA (standardized from TBG in Bronze)."),

  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit check (from Bronze Daily)."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB HINT EC (from Bronze Daily)."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB invoca sales calls (from Bronze Daily)."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads (from Bronze Daily)."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic (from Bronze Daily)."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions (from Bronze Daily)."),

  file_load_datetime DATETIME OPTIONS(description="Bronze Daily load timestamp (lineage)."),
  silver_inserted_at TIMESTAMP OPTIONS(description="Timestamp when this row was inserted/updated in Silver.")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, lob, ad_platform
OPTIONS(
  description = "Silver SA360 campaign daily fact table. Metrics from Bronze Daily, campaign metadata from Bronze Entity via AS-OF join. Partitioned by date; clustered by account_id/campaign_id/lob/ad_platform."
);
