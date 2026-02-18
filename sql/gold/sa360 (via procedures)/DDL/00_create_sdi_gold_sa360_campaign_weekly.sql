/*
===============================================================================
FILE: 00_create_sdi_gold_sa360_campaign_weekly.sql
LAYER: Gold
TABLE: sdi_gold_sa360_campaign_weekly

SOURCE:
  Gold Daily: sdi_gold_sa360_campaign_daily

PURPOSE:
  QGP week rollup for reporting:
    - qgp_week = period end date where:
        * normally: week ending Saturday
        * if quarter-end occurs before that Saturday:
            - dates up to quarter-end roll into qgp_week = quarter_end (partial)
            - remaining dates roll into qgp_week = Saturday
    - qgp_week_yyyymmdd string for labels/joins
    - metrics are SUM(daily metrics) to guarantee reconciliation

GRAIN:
  (account_id, campaign_id, qgp_week)

PARTITION / CLUSTER:
  PARTITION BY qgp_week
  CLUSTER BY lob, account_id, campaign_id
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
(
  -- Keys
  account_id STRING OPTIONS(description="SA360 advertiser account ID."),
  campaign_id STRING OPTIONS(description="Campaign ID."),
  qgp_week DATE OPTIONS(description="Canonical period end date: Saturday week-end OR quarter-end partial (partition key)."),
  qgp_week_yyyymmdd STRING OPTIONS(description="Formatted qgp_week as YYYYMMDD."),
  period_type STRING OPTIONS(description="WEEKLY or QUARTER_END_PARTIAL (quarter split)."),

  -- Reporting dimensions (latest-in-period)
  lob STRING OPTIONS(description="LOB (latest value in period)."),
  ad_platform STRING OPTIONS(description="Ad platform (latest in period)."),
  account_name STRING OPTIONS(description="Account name (latest in period)."),
  campaign_name STRING OPTIONS(description="Campaign name (latest in period)."),
  campaign_type STRING OPTIONS(description="Campaign type (latest in period)."),

  advertising_channel_type STRING OPTIONS(description="Latest in period."),
  advertising_channel_sub_type STRING OPTIONS(description="Latest in period."),
  bidding_strategy_type STRING OPTIONS(description="Latest in period."),
  serving_status STRING OPTIONS(description="Latest in period."),

  -- Period sums (reconciliation-safe)
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

  gold_weekly_inserted_at TIMESTAMP OPTIONS(description="Timestamp when period row was inserted/updated.")
)
PARTITION BY qgp_week
CLUSTER BY lob, account_id, campaign_id
OPTIONS(description="Gold SA360 campaign QGP-week rollup derived as sums of Gold Daily; qgp_week may be Saturday or quarter-end partial.");
