/*
===============================================================================
FILE: 00_create_sdi_silver_sa360_campaign_daily.sql
LAYER: Silver
TABLE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily

PURPOSE:
  Business-ready enriched daily campaign fact table for SA360 Paid Search.
  - Metrics come from Bronze Daily (performance snapshot)
  - Latest campaign metadata comes from Bronze Entity (settings snapshot)
  - Adds business dimensions derived from account_name:
      (1) lob         = Postpaid | HSI | Fiber | Metro | TFB
      (2) ad_platform = Google | Bing
  - Explicitly EXCLUDES any campaign status fields in Silver (per requirement).

GRAIN:
  account_id + campaign_id + date

SOURCES:
  - Bronze Daily:
      prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily
  - Bronze Entity:
      prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

TYPE SAFETY (IMPORTANT):
  - Silver field types are aligned to Bronze sources:
      • From Bronze Daily: same types (STRING/FLOAT64/DATE/DATETIME/INT64)
      • From Entity: selected fields are STRING
  - We do not cast any underlying Bronze fields in Silver to avoid type mismatches.

PARTITION / CLUSTER (BigQuery rules):
  - PARTITION BY date  (best for time-series pruning + cost control)
  - CLUSTER BY account_id, campaign_id, lob, ad_platform (MAX 4 fields)

METRIC COVERAGE (from Bronze Daily):
  - Core: impressions, clicks, cost, all_conversions
  - Intent/Quality: bi, buying_intent, bts_quality_traffic, digital_gross_add, magenta_pqt
  - Cart/Postpaid/PSPV: cart_start, postpaid_cart_start, postpaid_pspv, aal, add_a_line
  - Connect: connect_low_funnel_prospect, connect_low_funnel_visit, connect_qt
  - HINT/HSI: hint_ec, hint_sec, hint_web_orders, invoca + offline invoca, ma_hint_ec_eligibility_check
  - Fiber: activations, pre_order, waitlist_sign_up, web_orders, ec/sec (+ DDA)
  - Metro: top/upper/mid/low funnel + metro_qt + metro_hint_qt
  - TMO: tmo_top_funnel_prospect, tmo_upper_funnel_prospect, t_mobile_prepaid_low_funnel_prospect
  - TFB: credit_check, invoca_sales_calls, leads, quality_traffic, tfb_hint_ec, total_tfb_conversions
  - TBG already standardized into TFB family in Bronze:
      tfb_low_funnel, tfb_lead_form_submit, tfb_invoca_sales_intent_dda, tfb_invoca_order_dda

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily`
(
  -- ============================================================
  -- GRAIN IDENTIFIERS (MANDATORY)
  -- ============================================================
  account_id STRING OPTIONS(description="SA360 advertiser account ID (from Bronze Daily: account_id)."),
  campaign_id STRING OPTIONS(description="Campaign ID (from Bronze Daily: campaign_id)."),
  date DATE OPTIONS(description="Canonical reporting date parsed from date_yyyymmdd (from Bronze Daily: date)."),

  -- Keep snapshot key from Bronze Daily (useful for lineage/debugging)
  date_yyyymmdd STRING OPTIONS(description="Snapshot date in YYYYMMDD (from Bronze Daily: date_yyyymmdd)."),

  -- ============================================================
  -- BUSINESS DIMENSIONS (DERIVED)
  -- ============================================================
  account_name STRING OPTIONS(description="Account/advertiser name (from Bronze Daily: account_name)."),

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

  -- ============================================================
  -- CAMPAIGN METADATA (LATEST SNAPSHOT FROM ENTITY)
  -- NOTE: campaign_status is NOT included in Silver (per requirement).
  -- ============================================================
  campaign_name STRING OPTIONS(description="Latest campaign name from Bronze Entity (entity.campaign_name)."),

  campaign_type STRING OPTIONS(description=
    "Derived campaign classification from campaign_name using regex patterns "
    "(Brand | Generic | Shopping | PMax | DemandGen | Unclassified)."
  ),

  advertising_channel_type STRING OPTIONS(description="Entity: advertising_channel_type (e.g., SEARCH, PERFORMANCE_MAX)."),
  advertising_channel_sub_type STRING OPTIONS(description="Entity: advertising_channel_sub_type (additional classification)."),
  bidding_strategy_type STRING OPTIONS(description="Entity: bidding_strategy_type (e.g., TARGET_ROAS, TARGET_CPA)."),
  serving_status STRING OPTIONS(description="Entity: serving_status (eligibility to serve)."),

  -- ============================================================
  -- OPTIONAL BUSINESS DIMENSIONS FROM BRONZE DAILY (TYPES RETAINED)
  -- ============================================================
  customer_id STRING OPTIONS(description="Google Ads customer ID (from Bronze Daily: customer_id)."),
  customer_name STRING OPTIONS(description="Customer name (from Bronze Daily: customer_name)."),
  resource_name STRING OPTIONS(description="Google Ads API resource name (from Bronze Daily: resource_name)."),
  segments_date STRING OPTIONS(description="Segments.date as YYYY-MM-DD string (from Bronze Daily: segments_date)."),
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID (from Bronze Daily: client_manager_id)."),
  client_manager_name STRING OPTIONS(description="Client manager name (from Bronze Daily: client_manager_name)."),

  -- ============================================================
  -- CORE PERFORMANCE METRICS (MANDATORY)
  -- ============================================================
  impressions FLOAT64 OPTIONS(description="Daily impressions (from Bronze Daily: impressions)."),
  clicks FLOAT64 OPTIONS(description="Daily clicks (from Bronze Daily: clicks)."),
  cost FLOAT64 OPTIONS(description="Daily cost in standard currency units (from Bronze Daily: cost)."),
  all_conversions FLOAT64 OPTIONS(description="Daily all conversions (from Bronze Daily: all_conversions)."),

  -- ============================================================
  -- INTENT / QUALITY / GENERIC METRICS
  -- ============================================================
  bi FLOAT64 OPTIONS(description="BI metric (from Bronze Daily: bi)."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent metric (from Bronze Daily: buying_intent)."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic (from Bronze Daily: bts_quality_traffic)."),
  digital_gross_add FLOAT64 OPTIONS(description="Digital gross adds (from Bronze Daily: digital_gross_add)."),
  magenta_pqt FLOAT64 OPTIONS(description="Magenta PQT completions (from Bronze Daily: magenta_pqt)."),

  -- ============================================================
  -- CART + POSTPAID / PSPV
  -- ============================================================
  cart_start FLOAT64 OPTIONS(description="Generic cart start (from Bronze Daily: cart_start)."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart start (from Bronze Daily: postpaid_cart_start)."),
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV (from Bronze Daily: postpaid_pspv)."),
  aal FLOAT64 OPTIONS(description="Add-a-line related conversions (from Bronze Daily: aal)."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-line conversions (from Bronze Daily: add_a_line)."),

  -- ============================================================
  -- CONNECT METRICS
  -- ============================================================
  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low-funnel prospect (from Bronze Daily: connect_low_funnel_prospect)."),
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low-funnel visit (from Bronze Daily: connect_low_funnel_visit)."),
  connect_qt FLOAT64 OPTIONS(description="Connect qualified traffic (from Bronze Daily: connect_qt)."),

  -- ============================================================
  -- HINT / HSI METRICS
  -- ============================================================
  hint_ec FLOAT64 OPTIONS(description="HINT eligibility checks (from Bronze Daily: hint_ec)."),
  hint_sec FLOAT64 OPTIONS(description="HINT secondary eligibility checks (from Bronze Daily: hint_sec)."),
  hint_web_orders FLOAT64 OPTIONS(description="HINT web orders (from Bronze Daily: hint_web_orders)."),
  hint_invoca_calls FLOAT64 OPTIONS(description="HINT Invoca calls (from Bronze Daily: hint_invoca_calls)."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="HINT offline Invoca calls (from Bronze Daily: hint_offline_invoca_calls)."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="HINT offline eligibility (from Bronze Daily: hint_offline_invoca_eligibility)."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="HINT offline order (from Bronze Daily: hint_offline_invoca_order)."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="HINT offline order RT (from Bronze Daily: hint_offline_invoca_order_rt)."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="HINT offline sales opp (from Bronze Daily: hint_offline_invoca_sales_opp)."),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="MA HINT EC eligibility check (from Bronze Daily: ma_hint_ec_eligibility_check)."),

  -- ============================================================
  -- FIBER METRICS
  -- ============================================================
  fiber_activations FLOAT64 OPTIONS(description="Fiber activations (from Bronze Daily: fiber_activations)."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-orders (from Bronze Daily: fiber_pre_order)."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-up (from Bronze Daily: fiber_waitlist_sign_up)."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders (from Bronze Daily: fiber_web_orders)."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber EC (from Bronze Daily: fiber_ec)."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber EC DDA (from Bronze Daily: fiber_ec_dda)."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber SEC (from Bronze Daily: fiber_sec)."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber SEC DDA (from Bronze Daily: fiber_sec_dda)."),

  -- ============================================================
  -- METRO METRICS
  -- ============================================================
  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low funnel CS (from Bronze Daily: metro_low_funnel_cs)."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid funnel prospect (from Bronze Daily: metro_mid_funnel_prospect)."),
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top funnel prospect (from Bronze Daily: metro_top_funnel_prospect)."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper funnel prospect (from Bronze Daily: metro_upper_funnel_prospect)."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro HINT QT (from Bronze Daily: metro_hint_qt)."),
  metro_qt FLOAT64 OPTIONS(description="Metro QT (from Bronze Daily: metro_qt)."),

  -- ============================================================
  -- TMO METRICS
  -- ============================================================
  t_mobile_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="T-Mobile prepaid low-funnel prospect (from Bronze Daily: t_mobile_prepaid_low_funnel_prospect)."),
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top funnel prospect (from Bronze Daily: tmo_top_funnel_prospect)."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper funnel prospect (from Bronze Daily: tmo_upper_funnel_prospect)."),

  -- ============================================================
  -- TFB + (TBG standardized into TFB family in Bronze)
  -- ============================================================
  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low funnel (standardized from TBG in Bronze: tfb_low_funnel)."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submit (standardized from TBG in Bronze: tfb_lead_form_submit)."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB invoca sales intent DDA (standardized from TBG in Bronze: tfb_invoca_sales_intent_dda)."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB invoca order DDA (standardized from TBG in Bronze: tfb_invoca_order_dda)."),

  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit check (from Bronze Daily: tfb_credit_check)."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB HINT EC (from Bronze Daily: tfb_hint_ec)."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB invoca sales calls (from Bronze Daily: tfb_invoca_sales_calls)."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads (from Bronze Daily: tfb_leads)."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic (from Bronze Daily: tfb_quality_traffic)."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions (from Bronze Daily: total_tfb_conversions)."),

  -- ============================================================
  -- LINEAGE / TECHNICAL METADATA
  -- ============================================================
  file_load_datetime DATETIME OPTIONS(description="Bronze daily load timestamp (from Bronze Daily: file_load_datetime)."),
  silver_inserted_at TIMESTAMP OPTIONS(description="Timestamp when this row was inserted/updated in Silver.")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id, lob, ad_platform
OPTIONS(
  description = "Silver SA360 campaign daily fact table. Enriched with latest entity metadata + derived lob/ad_platform. Partitioned by date; clustered by account_id/campaign_id/lob/ad_platform."
);
