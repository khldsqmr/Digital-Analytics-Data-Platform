/*
===============================================================================
BOOTSTRAP | BRONZE | SA360 | CAMPAIGN DAILY
===============================================================================

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

TARGET (BRONZE):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily

PURPOSE:
  Create the Bronze Campaign Daily table with:
    - Clean, standardized column names (Bronze naming)
    - Strong column-level descriptions
    - Partitioning/clustering for efficient querying

KEY DESIGN CHOICES:
  1) "date" (DATE) is parsed from date_yyyymmdd (per your requirement).
  2) Raw source field `date` (INT64) is kept as `date_serial` to avoid collision.
  3) Raw column names that are "messy" are standardized in Bronze:
       __insert_date                    -> insert_date
       _ma_hint_ec__eligibility__check_ -> ma_hint_ec_eligibility_check
       double underscores               -> single underscore
       trailing underscore              -> removed (e.g., cart__start_ -> cart_start)

  4) TBG and TFB mean the same:
       We standardize TBG metrics into the TFB family in Bronze:
         tfb_low_funnel               <- tbg__low__funnel
         tfb_lead_form_submit         <- tbg__lead__form__submit
         tfb_invoca_sales_intent_dda  <- tbg__invoca__sales__intent_dda
         tfb_invoca_order_dda         <- tbg__invoca__order_dda

PARTITION / CLUSTER:
  - Partition by date (DATE)
  - Cluster by account_id, campaign_id

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
(
  -- -----------------------------
  -- Keys / Snapshot
  -- -----------------------------
  account_id STRING OPTIONS(description="Search Ads 360 / advertiser account ID (raw: account_id)."),
  campaign_id STRING OPTIONS(description="Campaign ID aligned to resource_name (raw: campaign_id)."),
  date_yyyymmdd STRING OPTIONS(description="Reporting snapshot date in YYYYMMDD (raw: date_yyyymmdd)."),
  date DATE OPTIONS(description="Parsed DATE from date_yyyymmdd (required canonical date for reporting)."),
  date_serial INT64 OPTIONS(description="Raw numeric date from source (raw column name: date). Kept separately to avoid collision with parsed DATE."),

  -- -----------------------------
  -- Dimensions
  -- -----------------------------
  account_name STRING OPTIONS(description="Account/advertiser name (raw: account_name)."),
  customer_id STRING OPTIONS(description="Google Ads customer ID (raw: customer_id)."),
  customer_name STRING OPTIONS(description="Customer (account) name (raw: customer_name)."),
  resource_name STRING OPTIONS(description="Google Ads API resource name for the campaign (raw: resource_name)."),
  segments_date STRING OPTIONS(description="Segments.date dimension as YYYY-MM-DD string (raw: segments_date)."),
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID in SA360 (raw: client_manager_id)."),
  client_manager_name STRING OPTIONS(description="Client manager name (raw: client_manager_name)."),

  -- -----------------------------
  -- Ingestion / Technical
  -- -----------------------------
  insert_date INT64 OPTIONS(description="Technical load integer (raw: __insert_date)."),
  file_load_datetime DATETIME OPTIONS(description="ETL load timestamp (raw: File_Load_datetime)."),
  filename STRING OPTIONS(description="Source file path/name for this snapshot (raw: Filename)."),

  -- -----------------------------
  -- Metrics (standardized naming)
  -- -----------------------------
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="Marketing automation HINT eligibility checks (raw: _ma_hint_ec__eligibility__check_)."),

  aal FLOAT64 OPTIONS(description="Add-a-line related conversions/score (raw: aal)."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-line conversions count (raw: add_a__line)."),
  all_conversions FLOAT64 OPTIONS(description="All conversions incl cross-device/modeled (raw: all_conversions)."),
  bi FLOAT64 OPTIONS(description="Business intent or internal score (raw: bi)."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic metric (raw: bts__quality__traffic)."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent signal/score (raw: buying__intent)."),

  clicks FLOAT64 OPTIONS(description="Total clicks (raw: clicks)."),
  impressions FLOAT64 OPTIONS(description="Total impressions (raw: impressions)."),

  cost_micros FLOAT64 OPTIONS(description="Advertiser cost in micros (raw: cost_micros). 1e6 micros = 1 unit."),
  cost FLOAT64 OPTIONS(description="Cost in standard currency units: cost_micros / 1e6 (derived)."),

  cart_start FLOAT64 OPTIONS(description="Cart start events attributed to campaign (raw: cart__start_)."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart start events (raw: postpaid__cart__start_)."),
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV metric (raw: postpaid_pspv_)."),

  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low-funnel prospects (raw: connect__low__funnel__prospect)."),
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low-funnel visits (raw: connect__low__funnel__visit)."),
  connect_qt FLOAT64 OPTIONS(description="Connect qualified traffic (raw: connect_qt)."),

  digital_gross_add FLOAT64 OPTIONS(description="Digital gross adds (raw: digital__gross__add)."),

  fiber_activations FLOAT64 OPTIONS(description="Fiber activations (raw: fiber__activations)."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-orders (raw: fiber__pre__order)."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-ups (raw: fiber__waitlist__sign__up)."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders (raw: fiber__web__orders)."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber e-commerce orders (raw: fiber_ec)."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber e-commerce orders (DDA attributed) (raw: fiber_ec_dda)."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber secondary eligibility checks conversion (raw: fiber_sec)."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber secondary eligibility checks (DDA attributed) (raw: fiber_sec_dda)."),

  hint_invoca_calls FLOAT64 OPTIONS(description="HINT Invoca calls (raw: hint__invoca__calls)."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="HINT offline Invoca calls (raw: hint__offline__invoca__calls)."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="HINT offline Invoca eligibility events (raw: hint__offline__invoca__eligibility)."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="HINT offline Invoca order events (raw: hint__offline__invoca__order)."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="HINT offline Invoca real-time order events (raw: hint__offline__invoca__order_rt_)."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="HINT offline Invoca sales opportunities (raw: hint__offline__invoca__sales__opp)."),
  hint_web_orders FLOAT64 OPTIONS(description="HINT web orders (raw: hint__web__orders)."),
  hint_ec FLOAT64 OPTIONS(description="Home Internet (HINT) eligibility checks (raw: hint_ec)."),
  hint_sec FLOAT64 OPTIONS(description="HINT secondary eligibility checks (raw: hint_sec)."),

  magenta_pqt FLOAT64 OPTIONS(description="Magenta pre-qualification tool completions (raw: magenta_pqt)."),

  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low-funnel customer signups (raw: metro__low__funnel_cs_)."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid-funnel prospects (raw: metro__mid__funnel__prospect)."),
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top-funnel prospects (raw: metro__top__funnel__prospect)."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper-funnel prospects (raw: metro__upper__funnel__prospect)."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro HINT qualified traffic (raw: metro_hint_qt)."),
  metro_qt FLOAT64 OPTIONS(description="Metro qualified traffic (raw: metro_qt)."),

  t_mobile_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="Prepaid low-funnel prospects (raw: t__mobile__prepaid__low__funnel__prospect)."),
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top-funnel prospects (raw: tmo__top__funnel__prospect)."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper-funnel prospects (raw: tmo__upper__funnel__prospect)."),

  -- TBG standardized into TFB (per your rule)
  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low-funnel conversions standardized from TBG (raw: tbg__low__funnel)."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submissions standardized from TBG (raw: tbg__lead__form__submit)."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB Invoca sales intent (DDA) standardized from TBG (raw: tbg__invoca__sales__intent_dda)."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB Invoca orders (DDA) standardized from TBG (raw: tbg__invoca__order_dda)."),

  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit check events (raw: tfb__credit__check)."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB HINT eligibility checks (raw: tfb_hint_ec)."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB Invoca sales calls (raw: tfb__invoca__sales__calls)."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads (raw: tfb__leads)."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic (raw: tfb__quality__traffic)."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions (raw: total_tfb__conversions).")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id
OPTIONS(
  description = "Bronze SA360 campaign daily performance snapshot. Standardized naming + column descriptions. Partitioned by parsed date."
);
