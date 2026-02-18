/*
===============================================================================
FILE: 00_create_sdi_gold_sa360_campaign_daily.sql
LAYER: Gold
TABLE: sdi_gold_sa360_campaign_daily
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  Silver fact: sdi_silver_sa360_campaign_daily

PURPOSE:
  "Tableau-ready" DAILY fact:
    - Keep the business-friendly fields + metrics needed for reporting
    - Remove raw ingestion clutter (still traceable via file_load_datetime)
    - Partition by date for performance

GRAIN:
  (account_id, campaign_id, date)

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY lob, ad_platform, campaign_type, account_id, campaign_id
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
(
  -- Keys
  account_id STRING OPTIONS(description="SA360 advertiser account ID."),
  campaign_id STRING OPTIONS(description="Campaign ID."),
  date DATE OPTIONS(description="Canonical reporting date (partition key)."),

  -- Reporting dimensions
  lob STRING OPTIONS(description="Line of business derived in Silver."),
  ad_platform STRING OPTIONS(description="Ad platform derived in Silver (Google/Bing/Unknown)."),
  account_name STRING OPTIONS(description="Account/advertiser name."),
  campaign_name STRING OPTIONS(description="Campaign name (entity AS-OF; fallback applied in Silver)."),
  campaign_type STRING OPTIONS(description="Derived classification from campaign_name."),

  -- Optional settings (useful slicers)
  advertising_channel_type STRING OPTIONS(description="Entity AS-OF setting."),
  advertising_channel_sub_type STRING OPTIONS(description="Entity AS-OF setting."),
  bidding_strategy_type STRING OPTIONS(description="Entity AS-OF setting."),
  serving_status STRING OPTIONS(description="Entity AS-OF setting."),

  -- Core metrics
  impressions FLOAT64 OPTIONS(description="Daily impressions."),
  clicks FLOAT64 OPTIONS(description="Daily clicks."),
  cost FLOAT64 OPTIONS(description="Daily cost (standard units)."),
  all_conversions FLOAT64 OPTIONS(description="Daily all conversions."),

  -- Supporting metrics (kept 1:1 with Silver)
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

  -- Lineage
  file_load_datetime DATETIME OPTIONS(description="Original Bronze Daily file load timestamp for traceability."),
  gold_inserted_at TIMESTAMP OPTIONS(description="Timestamp when row was inserted/updated in Gold.")
)
PARTITION BY date
CLUSTER BY lob, ad_platform, campaign_type, account_id, campaign_id
OPTIONS(description="Gold SA360 campaign daily fact. Reporting-ready schema derived from Silver.");
