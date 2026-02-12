/*
===============================================================================
BOOTSTRAP | BRONZE | SA 360 | CAMPAIGN DAILY (ONE-TIME)
===============================================================================


-- PURPOSE:
--   Bootstrap creation of Bronze SA360 Campaign Daily table.
--   - Cleans column names
--   - Preserves tmo naming
--   - Derives date from date_yyyymmdd
--   - Adds cost (converted from micros)
--   - Includes schema descriptions
--
-- FILE: create_campaign_daily.sql
--
-- TARGET TABLE:
--   prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily
--
-- SOURCE TABLE:
--   prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo
--
-- GRAIN
-- account_id + campaign_id + date

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
(
  -- ============================================================
  -- IDENTIFIERS
  -- ============================================================

  account_id STRING OPTIONS(description='Search Ads 360 advertiser account ID.'),
  account_name STRING OPTIONS(description='Account or advertiser name.'),
  campaign_id STRING OPTIONS(description='Campaign ID aligned to resource_name.'),
  resource_name STRING OPTIONS(description='Google Ads API resource name.'),
  customer_id STRING OPTIONS(description='Google Ads customer ID.'),
  customer_name STRING OPTIONS(description='Customer account name.'),
  client_manager_id FLOAT64 OPTIONS(description='Client manager ID in SA360.'),
  client_manager_name STRING OPTIONS(description='Client manager name.'),

  -- ============================================================
  -- DATE FIELDS
  -- ============================================================

  date_yyyymmdd STRING OPTIONS(description='Reporting date in YYYYMMDD format.'),
  date DATE OPTIONS(description='Parsed DATE derived from date_yyyymmdd.'),
  segments_date DATE OPTIONS(description='Google Ads segments.date dimension.'),

  raw_numeric_date INT64 OPTIONS(description='Raw numeric date value from source.'),
  __insert_date INT64 OPTIONS(description='Technical insert date identifier from source.'),

  -- ============================================================
  -- LOAD METADATA
  -- ============================================================

  file_load_datetime DATETIME OPTIONS(description='ETL file load timestamp.'),
  filename STRING OPTIONS(description='Source file path used for ingestion.'),
  bronze_inserted_at TIMESTAMP OPTIONS(description='Bronze ingestion timestamp.'),

  -- ============================================================
  -- CORE PERFORMANCE METRICS
  -- ============================================================

  clicks FLOAT64 OPTIONS(description='Total clicks.'),
  impressions FLOAT64 OPTIONS(description='Total impressions.'),
  cost_micros FLOAT64 OPTIONS(description='Cost in micros (1e6 micros = 1 currency unit).'),
  cost FLOAT64 OPTIONS(description='Cost converted from micros to currency unit.'),
  all_conversions FLOAT64 OPTIONS(description='All conversions including cross-device.'),

  -- ============================================================
  -- POSTPAID
  -- ============================================================

  postpaid_cart_start FLOAT64 OPTIONS(description='Postpaid cart start events.'),
  postpaid_pspv FLOAT64 OPTIONS(description='Postpaid PSPV metric.'),
  aal FLOAT64 OPTIONS(description='Add-a-line related conversions.'),
  add_a_line FLOAT64 OPTIONS(description='Add-a-line conversions.'),

  -- ============================================================
  -- HINT (Home Internet)
  -- ============================================================

  hint_ec FLOAT64 OPTIONS(description='Home Internet eligibility checks.'),
  hint_sec FLOAT64 OPTIONS(description='Home Internet secondary eligibility checks.'),
  hint_web_orders FLOAT64 OPTIONS(description='HINT web orders.'),
  hint_invoca_calls FLOAT64 OPTIONS(description='HINT Invoca calls.'),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description='HINT offline Invoca calls.'),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description='HINT offline eligibility events.'),
  hint_offline_invoca_order FLOAT64 OPTIONS(description='HINT offline order events.'),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description='HINT offline real-time order events.'),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description='HINT offline sales opportunities.'),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description='Marketing automation HINT eligibility checks.'),

  -- ============================================================
  -- FIBER
  -- ============================================================

  fiber_activations FLOAT64 OPTIONS(description='Fiber activations.'),
  fiber_pre_order FLOAT64 OPTIONS(description='Fiber pre-orders.'),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description='Fiber waitlist sign-ups.'),
  fiber_web_orders FLOAT64 OPTIONS(description='Fiber web orders.'),
  fiber_ec FLOAT64 OPTIONS(description='Fiber e-commerce orders.'),
  fiber_ec_dda FLOAT64 OPTIONS(description='Fiber e-commerce DDA attributed.'),
  fiber_sec FLOAT64 OPTIONS(description='Fiber secondary eligibility checks.'),
  fiber_sec_dda FLOAT64 OPTIONS(description='Fiber secondary eligibility DDA attributed.'),

  -- ============================================================
  -- METRO
  -- ============================================================

  metro_top_funnel_prospect FLOAT64 OPTIONS(description='Metro top funnel prospects.'),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description='Metro upper funnel prospects.'),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description='Metro mid funnel prospects.'),
  metro_low_funnel_cs FLOAT64 OPTIONS(description='Metro low funnel customer signups.'),
  metro_qt FLOAT64 OPTIONS(description='Metro qualified traffic.'),
  metro_hint_qt FLOAT64 OPTIONS(description='Metro HINT qualified traffic.'),

  -- ============================================================
  -- TMO
  -- ============================================================

  tmo_top_funnel_prospect FLOAT64 OPTIONS(description='TMO top funnel prospects.'),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description='TMO upper funnel prospects.'),
  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description='TMO prepaid low funnel prospects.'),

  -- ============================================================
  -- TFB
  -- ============================================================

  tfb_credit_check FLOAT64 OPTIONS(description='TFB credit check events.'),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description='TFB Invoca sales calls.'),
  tfb_leads FLOAT64 OPTIONS(description='TFB leads.'),
  tfb_quality_traffic FLOAT64 OPTIONS(description='TFB quality traffic.'),
  tfb_hint_ec FLOAT64 OPTIONS(description='TFB HINT eligibility checks.'),
  total_tfb_conversions FLOAT64 OPTIONS(description='Total TFB conversions.'),

  -- ============================================================
  -- OTHER
  -- ============================================================

  magenta_pqt FLOAT64 OPTIONS(description='Magenta pre-qualification tool completions.')

)
PARTITION BY date
CLUSTER BY account_id, campaign_id;
