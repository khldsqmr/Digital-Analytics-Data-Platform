/*
===============================================================================
FILE: 00_create_sdi_silver_sa360_campaign_daily.sql
LAYER: Silver
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:  sdi_silver_sa360_campaign_daily

PURPOSE:
  Business-ready DAILY campaign fact table:
    - Metrics from Bronze Daily
    - Metadata from Bronze Entity via AS-OF join (entity.date <= daily.date)
    - campaign_name fallback: latest non-null per (account_id, campaign_id)

GRAIN:
  account_id + campaign_id + date

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, campaign_id, lob, ad_platform
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

  lob STRING OPTIONS(description="LOB derived from account_name in Silver logic."),
  ad_platform STRING OPTIONS(description="Ad platform derived from account_name (Google/Bing/Unknown)."),

  campaign_name STRING OPTIONS(description="Campaign name from entity (AS-OF) with safe fallback to latest non-null."),
  campaign_type STRING OPTIONS(description="Derived classification from campaign_name (Brand/Generic/Shopping/PMax/DemandGen/Unclassified)."),

  advertising_channel_type STRING OPTIONS(description="Entity: advertising_channel_type (AS-OF)."),
  advertising_channel_sub_type STRING OPTIONS(description="Entity: advertising_channel_sub_type (AS-OF)."),
  bidding_strategy_type STRING OPTIONS(description="Entity: bidding_strategy_type (AS-OF)."),
  serving_status STRING OPTIONS(description="Entity: serving_status (AS-OF)."),

  customer_id STRING OPTIONS(description="Google Ads customer ID (from Bronze Daily)."),
  customer_name STRING OPTIONS(description="Customer name (from Bronze Daily)."),
  resource_name STRING OPTIONS(description="Resource name (from Bronze Daily)."),
  segments_date STRING OPTIONS(description="Segments.date as string (from Bronze Daily)."),
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID (from Bronze Daily)."),
  client_manager_name STRING OPTIONS(description="Client manager name (from Bronze Daily)."),

  impressions FLOAT64 OPTIONS(description="Daily impressions."),
  clicks FLOAT64 OPTIONS(description="Daily clicks."),
  cost FLOAT64 OPTIONS(description="Daily cost."),
  all_conversions FLOAT64 OPTIONS(description="Daily all_conversions."),

  bi FLOAT64,
  buying_intent FLOAT64,
  bts_quality_traffic FLOAT64,
  digital_gross_add FLOAT64,
  magenta_pqt FLOAT64,

  cart_start FLOAT64,
  postpaid_cart_start FLOAT64,
  postpaid_pspv FLOAT64,
  aal FLOAT64,
  add_a_line FLOAT64,

  connect_low_funnel_prospect FLOAT64,
  connect_low_funnel_visit FLOAT64,
  connect_qt FLOAT64,

  hint_ec FLOAT64,
  hint_sec FLOAT64,
  hint_web_orders FLOAT64,
  hint_invoca_calls FLOAT64,
  hint_offline_invoca_calls FLOAT64,
  hint_offline_invoca_eligibility FLOAT64,
  hint_offline_invoca_order FLOAT64,
  hint_offline_invoca_order_rt FLOAT64,
  hint_offline_invoca_sales_opp FLOAT64,
  ma_hint_ec_eligibility_check FLOAT64,

  fiber_activations FLOAT64,
  fiber_pre_order FLOAT64,
  fiber_waitlist_sign_up FLOAT64,
  fiber_web_orders FLOAT64,
  fiber_ec FLOAT64,
  fiber_ec_dda FLOAT64,
  fiber_sec FLOAT64,
  fiber_sec_dda FLOAT64,

  metro_low_funnel_cs FLOAT64,
  metro_mid_funnel_prospect FLOAT64,
  metro_top_funnel_prospect FLOAT64,
  metro_upper_funnel_prospect FLOAT64,
  metro_hint_qt FLOAT64,
  metro_qt FLOAT64,

  tmo_prepaid_low_funnel_prospect FLOAT64,
  tmo_top_funnel_prospect FLOAT64,
  tmo_upper_funnel_prospect FLOAT64,

  tfb_low_funnel FLOAT64,
  tfb_lead_form_submit FLOAT64,
  tfb_invoca_sales_intent_dda FLOAT64,
  tfb_invoca_order_dda FLOAT64,

  tfb_credit_check FLOAT64,
  tfb_hint_ec FLOAT64,
  tfb_invoca_sales_calls FLOAT64,
  tfb_leads FLOAT64,
  tfb_quality_traffic FLOAT64,
  total_tfb_conversions FLOAT64,

  file_load_datetime DATETIME OPTIONS(description="Bronze Daily load timestamp (lineage)."),
  silver_inserted_at TIMESTAMP OPTIONS(description="Timestamp when this row was inserted/updated in Silver.")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, lob, ad_platform
OPTIONS(description="Silver SA360 campaign daily fact. Metrics from Bronze Daily, entity enrichment via AS-OF join, stable BI-ready schema.");
