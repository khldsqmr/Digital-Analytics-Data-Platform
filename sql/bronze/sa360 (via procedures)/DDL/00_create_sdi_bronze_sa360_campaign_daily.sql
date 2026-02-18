/*
===============================================================================
FILE: 00_create_sdi_bronze_sa360_campaign_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:   sdi_bronze_sa360_campaign_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

PURPOSE:
  Create the canonical Bronze daily campaign snapshot table:
    - Canonical date (DATE) parsed from date_yyyymmdd (STRING)
    - Strong column descriptions for downstream usability
    - Partitioned by date, clustered by account_id/campaign_id

DESIGN / BEST PRACTICES:
  - Keep date_yyyymmdd for lineage/debug; use date as partition key
  - All metrics FLOAT64 for consistency across layers
  - IDs are STRING
  - No raw INT64 `date` field retained (prevents confusion)

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, campaign_id
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
(
  -- =====================================================================
  -- GRAIN KEYS (DAILY SNAPSHOT)
  -- =====================================================================
  account_id STRING OPTIONS(description="SA360 advertiser account ID (raw: account_id)."),
  campaign_id STRING OPTIONS(description="Campaign ID (raw: campaign_id)."),
  date_yyyymmdd STRING OPTIONS(description="Snapshot date in YYYYMMDD (raw: date_yyyymmdd)."),
  date DATE OPTIONS(description="Canonical DATE parsed from date_yyyymmdd. Partition key."),

  -- =====================================================================
  -- DIMENSIONS
  -- =====================================================================
  account_name STRING OPTIONS(description="Account/advertiser name (raw: account_name)."),
  customer_id STRING OPTIONS(description="Google Ads customer ID (raw: customer_id)."),
  customer_name STRING OPTIONS(description="Customer/account name (raw: customer_name)."),
  resource_name STRING OPTIONS(description="Google Ads API resource name (raw: resource_name)."),
  segments_date STRING OPTIONS(description="Segments.date as YYYY-MM-DD string (raw: segments_date)."),
  client_manager_id FLOAT64 OPTIONS(description="Client manager ID (raw: client_manager_id)."),
  client_manager_name STRING OPTIONS(description="Client manager name (raw: client_manager_name)."),

  -- =====================================================================
  -- INGESTION / LINEAGE
  -- =====================================================================
  insert_date INT64 OPTIONS(description="Technical load integer (raw: __insert_date)."),
  file_load_datetime DATETIME OPTIONS(description="Ingestion load timestamp (raw: File_Load_datetime)."),
  filename STRING OPTIONS(description="Source file name/path (raw: Filename)."),

  -- =====================================================================
  -- METRICS (STANDARDIZED)
  -- =====================================================================
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="MA HINT eligibility checks (raw: _ma_hint_ec__eligibility__check_)."),

  aal FLOAT64 OPTIONS(description="AAL metric (raw: aal)."),
  add_a_line FLOAT64 OPTIONS(description="Add-a-line metric (raw: add_a__line)."),
  all_conversions FLOAT64 OPTIONS(description="All conversions (raw: all_conversions)."),
  bi FLOAT64 OPTIONS(description="BI metric (raw: bi)."),
  bts_quality_traffic FLOAT64 OPTIONS(description="BTS quality traffic (raw: bts__quality__traffic)."),
  buying_intent FLOAT64 OPTIONS(description="Buying intent (raw: buying__intent)."),

  clicks FLOAT64 OPTIONS(description="Clicks (raw: clicks)."),
  impressions FLOAT64 OPTIONS(description="Impressions (raw: impressions)."),

  cost_micros FLOAT64 OPTIONS(description="Cost in micros (raw: cost_micros)."),
  cost FLOAT64 OPTIONS(description="Cost in standard units derived as cost_micros / 1e6."),

  cart_start FLOAT64 OPTIONS(description="Cart starts (raw: cart__start_)."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Postpaid cart starts (raw: postpaid__cart__start_)."),
  postpaid_pspv FLOAT64 OPTIONS(description="Postpaid PSPV (raw: postpaid_pspv_)."),

  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Connect low-funnel prospect (raw: connect__low__funnel__prospect)."),
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Connect low-funnel visit (raw: connect__low__funnel__visit)."),
  connect_qt FLOAT64 OPTIONS(description="Connect qualified traffic (raw: connect_qt)."),

  digital_gross_add FLOAT64 OPTIONS(description="Digital gross adds (raw: digital__gross__add)."),

  fiber_activations FLOAT64 OPTIONS(description="Fiber activations (raw: fiber__activations)."),
  fiber_pre_order FLOAT64 OPTIONS(description="Fiber pre-order (raw: fiber__pre__order)."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Fiber waitlist sign-up (raw: fiber__waitlist__sign__up)."),
  fiber_web_orders FLOAT64 OPTIONS(description="Fiber web orders (raw: fiber__web__orders)."),
  fiber_ec FLOAT64 OPTIONS(description="Fiber EC (raw: fiber_ec)."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Fiber EC DDA (raw: fiber_ec_dda)."),
  fiber_sec FLOAT64 OPTIONS(description="Fiber SEC (raw: fiber_sec)."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Fiber SEC DDA (raw: fiber_sec_dda)."),

  hint_invoca_calls FLOAT64 OPTIONS(description="HINT Invoca calls (raw: hint__invoca__calls)."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="HINT offline Invoca calls (raw: hint__offline__invoca__calls)."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="HINT offline Invoca eligibility (raw: hint__offline__invoca__eligibility)."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="HINT offline Invoca order (raw: hint__offline__invoca__order)."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="HINT offline Invoca order RT (raw: hint__offline__invoca__order_rt_)."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="HINT offline Invoca sales opp (raw: hint__offline__invoca__sales__opp)."),
  hint_web_orders FLOAT64 OPTIONS(description="HINT web orders (raw: hint__web__orders)."),
  hint_ec FLOAT64 OPTIONS(description="HINT eligibility checks (raw: hint_ec)."),
  hint_sec FLOAT64 OPTIONS(description="HINT secondary eligibility checks (raw: hint_sec)."),

  magenta_pqt FLOAT64 OPTIONS(description="Magenta PQT (raw: magenta_pqt)."),

  metro_low_funnel_cs FLOAT64 OPTIONS(description="Metro low funnel CS (raw: metro__low__funnel_cs_)."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Metro mid funnel prospect (raw: metro__mid__funnel__prospect)."),
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Metro top funnel prospect (raw: metro__top__funnel__prospect)."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Metro upper funnel prospect (raw: metro__upper__funnel__prospect)."),
  metro_hint_qt FLOAT64 OPTIONS(description="Metro HINT QT (raw: metro_hint_qt)."),
  metro_qt FLOAT64 OPTIONS(description="Metro QT (raw: metro_qt)."),

  -- TMO naming enforced
  tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="TMO prepaid low funnel prospect (raw: t__mobile__prepaid__low__funnel__prospect)."),
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="TMO top funnel prospect (raw: tmo__top__funnel__prospect)."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="TMO upper funnel prospect (raw: tmo__upper__funnel__prospect)."),

  -- TBG standardized into TFB
  tfb_low_funnel FLOAT64 OPTIONS(description="TFB low funnel standardized from TBG (raw: tbg__low__funnel)."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="TFB lead form submit standardized from TBG (raw: tbg__lead__form__submit)."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="TFB invoca sales intent DDA standardized from TBG (raw: tbg__invoca__sales__intent_dda)."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="TFB invoca order DDA standardized from TBG (raw: tbg__invoca__order_dda)."),

  tfb_credit_check FLOAT64 OPTIONS(description="TFB credit checks (raw: tfb__credit__check)."),
  tfb_hint_ec FLOAT64 OPTIONS(description="TFB HINT EC (raw: tfb_hint_ec)."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="TFB invoca sales calls (raw: tfb__invoca__sales__calls)."),
  tfb_leads FLOAT64 OPTIONS(description="TFB leads (raw: tfb__leads)."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="TFB quality traffic (raw: tfb__quality__traffic)."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Total TFB conversions (raw: total_tfb__conversions).")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id
OPTIONS(
  description = "Bronze SA360 campaign daily snapshot. Cleaned naming, strong schema descriptions, partitioned by canonical date."
);
