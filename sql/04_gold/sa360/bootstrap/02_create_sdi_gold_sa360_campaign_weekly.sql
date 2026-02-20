/*
===============================================================================
FILE: 03_create_sdi_gold_sa360_campaign_weekly.sql
LAYER: Gold (Weekly)
TARGET:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly

SOURCE:
  Gold Daily

TIME GRAIN:
  qgp_week = bucket end date:
    - Saturday for standard Sun..Sat weeks
    - Quarter-end date for quarter-end partial tail (after last Sat of quarter)

GRAIN:
  account_id + campaign_id + qgp_week
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_weekly`
(
  account_id STRING,
  account_name STRING,
  campaign_id STRING,
  campaign_name STRING,
  qgp_week DATE,

  lob STRING,
  ad_platform STRING,
  campaign_type STRING,

  advertising_channel_type STRING,
  advertising_channel_sub_type STRING,
  bidding_strategy_type STRING,
  serving_status STRING,

  customer_id STRING,
  customer_name STRING,
  resource_name STRING,
  client_manager_id FLOAT64,
  client_manager_name STRING,

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

  metro_top_funnel_prospect FLOAT64,
  metro_upper_funnel_prospect FLOAT64,
  metro_mid_funnel_prospect FLOAT64,
  metro_low_funnel_cs FLOAT64,
  metro_qt FLOAT64,
  metro_hint_qt FLOAT64,

  tmo_top_funnel_prospect FLOAT64,
  tmo_upper_funnel_prospect FLOAT64,
  tmo_prepaid_low_funnel_prospect FLOAT64,

  tfb_credit_check FLOAT64,
  tfb_invoca_sales_calls FLOAT64,
  tfb_leads FLOAT64,
  tfb_quality_traffic FLOAT64,
  tfb_hint_ec FLOAT64,
  total_tfb_conversions FLOAT64,
  tfb_low_funnel FLOAT64,
  tfb_lead_form_submit FLOAT64,
  tfb_invoca_sales_intent_dda FLOAT64,
  tfb_invoca_order_dda FLOAT64,

  file_load_datetime DATETIME,
  gold_inserted_at TIMESTAMP
)
PARTITION BY qgp_week
CLUSTER BY lob, ad_platform, account_id, campaign_id
OPTIONS(description="Gold SA360 Weekly table rolled up from Gold Daily using QGP week logic.");
