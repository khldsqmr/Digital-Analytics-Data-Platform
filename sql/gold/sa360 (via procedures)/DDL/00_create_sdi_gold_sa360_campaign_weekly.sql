/*
===============================================================================
FILE: 00_create_sdi_gold_sa360_campaign_weekly.sql
LAYER: Gold
TABLE: sdi_gold_sa360_campaign_weekly

SOURCE:
  Gold Daily: sdi_gold_sa360_campaign_daily

PURPOSE:
  Weekly rollup for reporting:
    - weekend_date = week ending Saturday
    - week_yyyymmdd string for easy joins / labels
    - metrics are sums of daily metrics to guarantee reconciliation

GRAIN:
  (account_id, campaign_id, weekend_date)

PARTITION / CLUSTER:
  PARTITION BY weekend_date
  CLUSTER BY lob, ad_platform, campaign_type, account_id, campaign_id
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
(
  -- Keys
  account_id STRING OPTIONS(description="SA360 advertiser account ID."),
  campaign_id STRING OPTIONS(description="Campaign ID."),
  weekend_date DATE OPTIONS(description="Week ending Saturday (partition key)."),
  week_yyyymmdd STRING OPTIONS(description="Formatted weekend_date as YYYYMMDD."),

  -- Reporting dimensions (picked as latest in the week)
  lob STRING OPTIONS(description="LOB (latest value in week)."),
  ad_platform STRING OPTIONS(description="Ad platform (latest value in week)."),
  account_name STRING OPTIONS(description="Account name (latest in week)."),
  campaign_name STRING OPTIONS(description="Campaign name (latest in week)."),
  campaign_type STRING OPTIONS(description="Campaign type (latest in week)."),

  advertising_channel_type STRING OPTIONS(description="Latest in week."),
  advertising_channel_sub_type STRING OPTIONS(description="Latest in week."),
  bidding_strategy_type STRING OPTIONS(description="Latest in week."),
  serving_status STRING OPTIONS(description="Latest in week."),

  -- Weekly sums (must reconcile to daily sums)
  impressions FLOAT64,
  clicks FLOAT64,
  cost FLOAT64,
  all_conversions FLOAT64,

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

  gold_weekly_inserted_at TIMESTAMP OPTIONS(description="Timestamp when week row was inserted/updated.")
)
PARTITION BY weekend_date
CLUSTER BY lob, account_id, campaign_id
OPTIONS(description="Gold SA360 campaign weekly rollup derived as sums of Gold Daily with stable week-ending Saturday.");
