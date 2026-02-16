/*
===============================================================================
ONE-TIME | BRONZE | SA360 | CAMPAIGN DAILY (PERFORMANCE SNAPSHOT)
===============================================================================

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

TARGET (BRONZE):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily

GRAIN:
  One row per (account_id, campaign_id, date_yyyymmdd) snapshot.

CRITICAL REQUIREMENTS YOU GAVE:
  - Use parsed date from date_yyyymmdd and name it "date".
  - Fix bad/raw column names with clean aliases:
      __insert_date -> insert_date
      _ma_hint_ec__eligibility__check_ -> ma_hint_ec_eligibility_check
      double underscores -> single underscore
      trailing underscores removed
  - tbg and tfb mean the same:
      we standardize to tfb_* for the tbg_* metrics too.

DUPLICATE PREVENTION:
  - Implemented in the incremental MERGE query (next section) via dedup + MERGE keys.

===============================================================================
*/

CREATE OR REPLACE TABLE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
(
  -- -----------------------------
  -- Keys / Snapshot
  -- -----------------------------
  account_id STRING OPTIONS(description='Search Ads 360 / advertiser account ID.'),
  campaign_id STRING OPTIONS(description='Campaign ID aligned to resource_name.'),
  date_yyyymmdd STRING OPTIONS(description='Reporting/snapshot date in YYYYMMDD.'),

  -- Parsed snapshot date (per your requirement)
  date DATE OPTIONS(description='Parsed snapshot date derived from date_yyyymmdd (YYYYMMDD). Field intentionally named "date".'),

  -- Raw "date" column from Improvado (INT64) kept but renamed to avoid collision
  date_serial INT64 OPTIONS(description='Raw numeric date field from source (kept for traceability).'),

  -- -----------------------------
  -- Core dimensions
  -- -----------------------------
  account_name STRING OPTIONS(description='Account/advertiser name.'),
  customer_id STRING OPTIONS(description='Google Ads customer ID.'),
  customer_name STRING OPTIONS(description='Customer (account) name.'),
  resource_name STRING OPTIONS(description='Google Ads API resource name for the campaign.'),
  segments_date STRING OPTIONS(description='Segments.date dimension as YYYY-MM-DD string (Google Ads).'),

  client_manager_id FLOAT64 OPTIONS(description='Client manager ID in SA360 (source type FLOAT64).'),
  client_manager_name STRING OPTIONS(description='Client manager name.'),

  -- -----------------------------
  -- Cleaned technical fields
  -- -----------------------------
  insert_date INT64 OPTIONS(description='Technical load integer (raw __insert_date renamed).'),
  file_load_datetime DATETIME OPTIONS(description='ETL load timestamp (file landed time).'),
  filename STRING OPTIONS(description='Source file path/name for this daily snapshot.'),

  -- -----------------------------
  -- Metrics (cleaned names)
  -- -----------------------------
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description='Marketing automation HINT eligibility checks (count).'),

  aal FLOAT64 OPTIONS(description='Add-a-line related conversions/score (likely count).'),
  add_a_line FLOAT64 OPTIONS(description='Add-a-line conversions (count).'),
  all_conversions FLOAT64 OPTIONS(description='All conversions including modeled/cross-device where applicable.'),
  bi FLOAT64 OPTIONS(description='Business Intent (or similar internal score).'),
  bts_quality_traffic FLOAT64 OPTIONS(description='BTS quality traffic metric.'),
  buying_intent FLOAT64 OPTIONS(description='Buying intent signal/score.'),

  clicks FLOAT64 OPTIONS(description='Total clicks.'),
  impressions FLOAT64 OPTIONS(description='Impressions.'),

  cost_micros FLOAT64 OPTIONS(description='Advertiser cost in micros of currency (1e6 micros = 1 unit).'),
  cost FLOAT64 OPTIONS(description='Derived cost in standard currency units (cost_micros / 1e6).'),

  cart_start FLOAT64 OPTIONS(description='Cart start events attributed to campaign.'),
  postpaid_cart_start FLOAT64 OPTIONS(description='Postpaid cart start events (count).'),
  postpaid_pspv FLOAT64 OPTIONS(description='Postpaid PSPV metric.'),

  connect_low_funnel_prospect FLOAT64 OPTIONS(description='Connect low-funnel prospects (count).'),
  connect_low_funnel_visit FLOAT64 OPTIONS(description='Connect low-funnel visits (count).'),
  connect_qt FLOAT64 OPTIONS(description='Connect qualified traffic.'),

  digital_gross_add FLOAT64 OPTIONS(description='Digital gross adds.'),

  fiber_activations FLOAT64 OPTIONS(description='Fiber activations (count).'),
  fiber_pre_order FLOAT64 OPTIONS(description='Fiber pre-orders (count).'),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description='Fiber waitlist sign-ups (count).'),
  fiber_web_orders FLOAT64 OPTIONS(description='Fiber web orders (count).'),
  fiber_ec FLOAT64 OPTIONS(description='Fiber e-commerce orders (count).'),
  fiber_ec_dda FLOAT64 OPTIONS(description='Fiber e-commerce (DDA attributed).'),
  fiber_sec FLOAT64 OPTIONS(description='Fiber secondary eligibility checks (count).'),
  fiber_sec_dda FLOAT64 OPTIONS(description='Fiber secondary eligibility checks (DDA attributed).'),

  hint_invoca_calls FLOAT64 OPTIONS(description='HINT Invoca calls (count).'),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description='HINT offline Invoca calls (count).'),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description='HINT offline Invoca eligibility events (count).'),
  hint_offline_invoca_order FLOAT64 OPTIONS(description='HINT offline Invoca order events (count).'),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description='HINT offline Invoca real-time order events (count).'),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description='HINT offline Invoca sales opportunities (count).'),
  hint_web_orders FLOAT64 OPTIONS(description='HINT web orders (count).'),
  hint_ec FLOAT64 OPTIONS(description='Home Internet (HINT) eligibility checks (count).'),
  hint_sec FLOAT64 OPTIONS(description='HINT secondary eligibility checks (count).'),

  magenta_pqt FLOAT64 OPTIONS(description='Magenta pre-qualification tool completions (count).'),

  metro_low_funnel_cs FLOAT64 OPTIONS(description='Metro low-funnel customer signups (count).'),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description='Metro mid-funnel prospects (count).'),
  metro_top_funnel_prospect FLOAT64 OPTIONS(description='Metro top-funnel prospects (count).'),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description='Metro upper-funnel prospects (count).'),
  metro_hint_qt FLOAT64 OPTIONS(description='Metro HINT qualified traffic (count).'),
  metro_qt FLOAT64 OPTIONS(description='Metro qualified traffic (count).'),

  t_mobile_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description='Prepaid low-funnel prospects (count).'),
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description='TMO top-funnel prospects (count).'),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description='TMO upper-funnel prospects (count).'),

  -- Standardize TBG -> TFB (your requirement: tfb and tbg mean the same)
  tfb_low_funnel FLOAT64 OPTIONS(description='TFB low-funnel conversions (includes raw tbg__low__funnel).'),
  tfb_lead_form_submit FLOAT64 OPTIONS(description='TFB lead form submissions (includes raw tbg__lead__form__submit).'),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description='TFB Invoca sales intent (DDA) (includes raw tbg__invoca__sales__intent_dda).'),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description='TFB Invoca orders (DDA) (includes raw tbg__invoca__order_dda).'),

  tfb_credit_check FLOAT64 OPTIONS(description='TFB credit check events (count).'),
  tfb_hint_ec FLOAT64 OPTIONS(description='TFB HINT eligibility checks (count).'),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description='TFB Invoca sales calls (count).'),
  tfb_leads FLOAT64 OPTIONS(description='TFB leads (count).'),
  tfb_quality_traffic FLOAT64 OPTIONS(description='TFB quality traffic (count/index).'),
  total_tfb_conversions FLOAT64 OPTIONS(description='Total TFB conversions.')
)
PARTITION BY date
CLUSTER BY account_id, campaign_id;
