/*
===============================================================================
BOOTSTRAP | BRONZE | SA360 | CAMPAIGN DAILY (ONE-TIME)
===============================================================================

PURPOSE
-------
Create a Bronze Campaign Daily table from the raw Improvado table:
`prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`

This table:
- Uses normalized (clean) column names for analysis & governance
- Preserves the original raw `date` INT64 as date_int64
- Creates `date` as DATE derived from `date_yyyymmdd` (per your requirement)
- Adds column descriptions (OPTIONS(description=...))
- Is partitioned by `date` (DATE)
- Is clustered by (account_id, campaign_id)

PRIMARY KEY (logical)
---------------------
(account_id, campaign_id, date_yyyymmdd)

TBG == TFB RULE
---------------
You stated: tbg and tfb mean the same. We standardize to tfb.
So we create tfb_* unified columns from the TBG raw columns:
  tbg__low__funnel               -> tfb_low_funnel
  tbg__lead__form__submit        -> tfb_lead_form_submit
  tbg__invoca__sales__intent_dda -> tfb_invoca_sales_intent_dda
  tbg__invoca__order_dda         -> tfb_invoca_order_dda

IMPORTANT RAW COLUMN RULE
-------------------------
We only reference raw columns that exist in your INFORMATION_SCHEMA list.
We also backtick columns with "bad" names like:
  `__insert_date`
  `_ma_hint_ec__eligibility__check_`
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
(
  /* =========================
     INGESTION METADATA
  ========================== */
  file_load_datetime DATETIME OPTIONS(description="Raw: File_Load_datetime. ETL ingestion load timestamp."),
  filename STRING OPTIONS(description="Raw: Filename. Source file path/name used to ingest snapshot."),
  insert_date INT64 OPTIONS(description="Raw: __insert_date. Technical load integer."),

  /* =========================
     KEYS / IDENTIFIERS
  ========================== */
  account_id STRING OPTIONS(description="Raw: account_id. SA360 advertiser/account ID."),
  account_name STRING OPTIONS(description="Raw: account_name. Account/advertiser name."),
  customer_id STRING OPTIONS(description="Raw: customer_id. Google Ads customer ID."),
  customer_name STRING OPTIONS(description="Raw: customer_name. Customer (account) name."),
  campaign_id STRING OPTIONS(description="Raw: campaign_id. Campaign ID."),
  resource_name STRING OPTIONS(description="Raw: resource_name. Google Ads API resource name for the campaign."),

  /* =========================
     DATES
  ========================== */
  date_int64 INT64 OPTIONS(description="Raw: date (INT64). Preserved raw numeric date/serial."),
  date_yyyymmdd STRING OPTIONS(description="Raw: date_yyyymmdd. Reporting date in YYYYMMDD."),
  date DATE OPTIONS(description="DATE derived from date_yyyymmdd (per requirement)."),
  segments_date DATE OPTIONS(description="Raw: segments_date parsed as DATE (can be null)."),

  /* =========================
     OPTIONAL DIMENSIONS
  ========================== */
  client_manager_id FLOAT64 OPTIONS(description="Raw: client_manager_id."),
  client_manager_name STRING OPTIONS(description="Raw: client_manager_name."),

  /* =========================
     DELIVERY / COST
  ========================== */
  impressions FLOAT64 OPTIONS(description="Raw: impressions."),
  clicks FLOAT64 OPTIONS(description="Raw: clicks."),
  cost_micros FLOAT64 OPTIONS(description="Raw: cost_micros. Cost in micros."),
  cost FLOAT64 OPTIONS(description="Derived: cost_micros / 1e6."),

  /* =========================
     CORE METRICS
  ========================== */
  all_conversions FLOAT64 OPTIONS(description="Raw: all_conversions."),
  bi FLOAT64 OPTIONS(description="Raw: bi."),
  bts_quality_traffic FLOAT64 OPTIONS(description="Raw: bts__quality__traffic."),
  buying_intent FLOAT64 OPTIONS(description="Raw: buying__intent."),
  aal FLOAT64 OPTIONS(description="Raw: aal."),
  add_a_line FLOAT64 OPTIONS(description="Raw: add_a__line."),
  digital_gross_add FLOAT64 OPTIONS(description="Raw: digital__gross__add."),

  /* =========================
     CART / PSPV
  ========================== */
  cart_start FLOAT64 OPTIONS(description="Raw: cart__start_."),
  postpaid_cart_start FLOAT64 OPTIONS(description="Raw: postpaid__cart__start_."),
  postpaid_pspv FLOAT64 OPTIONS(description="Raw: postpaid_pspv_."),

  /* =========================
     CONNECT
  ========================== */
  connect_low_funnel_prospect FLOAT64 OPTIONS(description="Raw: connect__low__funnel__prospect."),
  connect_low_funnel_visit FLOAT64 OPTIONS(description="Raw: connect__low__funnel__visit."),
  connect_qt FLOAT64 OPTIONS(description="Raw: connect_qt."),

  /* =========================
     METRO
  ========================== */
  metro_low_funnel_cs FLOAT64 OPTIONS(description="Raw: metro__low__funnel_cs_."),
  metro_mid_funnel_prospect FLOAT64 OPTIONS(description="Raw: metro__mid__funnel__prospect."),
  metro_top_funnel_prospect FLOAT64 OPTIONS(description="Raw: metro__top__funnel__prospect."),
  metro_upper_funnel_prospect FLOAT64 OPTIONS(description="Raw: metro__upper__funnel__prospect."),
  metro_hint_qt FLOAT64 OPTIONS(description="Raw: metro_hint_qt."),
  metro_qt FLOAT64 OPTIONS(description="Raw: metro_qt."),

  /* =========================
     FIBER
  ========================== */
  fiber_activations FLOAT64 OPTIONS(description="Raw: fiber__activations."),
  fiber_pre_order FLOAT64 OPTIONS(description="Raw: fiber__pre__order."),
  fiber_waitlist_sign_up FLOAT64 OPTIONS(description="Raw: fiber__waitlist__sign__up."),
  fiber_web_orders FLOAT64 OPTIONS(description="Raw: fiber__web__orders."),
  fiber_ec FLOAT64 OPTIONS(description="Raw: fiber_ec."),
  fiber_ec_dda FLOAT64 OPTIONS(description="Raw: fiber_ec_dda."),
  fiber_sec FLOAT64 OPTIONS(description="Raw: fiber_sec."),
  fiber_sec_dda FLOAT64 OPTIONS(description="Raw: fiber_sec_dda."),

  /* =========================
     HINT + INVOCA
  ========================== */
  hint_ec FLOAT64 OPTIONS(description="Raw: hint_ec."),
  hint_sec FLOAT64 OPTIONS(description="Raw: hint_sec."),
  ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description="Raw: _ma_hint_ec__eligibility__check_."),

  hint_invoca_calls FLOAT64 OPTIONS(description="Raw: hint__invoca__calls."),
  hint_offline_invoca_calls FLOAT64 OPTIONS(description="Raw: hint__offline__invoca__calls."),
  hint_offline_invoca_eligibility FLOAT64 OPTIONS(description="Raw: hint__offline__invoca__eligibility."),
  hint_offline_invoca_order FLOAT64 OPTIONS(description="Raw: hint__offline__invoca__order."),
  hint_offline_invoca_order_rt FLOAT64 OPTIONS(description="Raw: hint__offline__invoca__order_rt_."),
  hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description="Raw: hint__offline__invoca__sales__opp."),
  hint_web_orders FLOAT64 OPTIONS(description="Raw: hint__web__orders."),

  /* =========================
     PREPAID + TMO FUNNEL
  ========================== */
  tmobile_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description="Raw: t__mobile__prepaid__low__funnel__prospect."),
  tmo_top_funnel_prospect FLOAT64 OPTIONS(description="Raw: tmo__top__funnel__prospect."),
  tmo_upper_funnel_prospect FLOAT64 OPTIONS(description="Raw: tmo__upper__funnel__prospect."),

  /* =========================
     TFB (Unified for TBG + TFB)
  ========================== */
  tfb_low_funnel FLOAT64 OPTIONS(description="Unified from raw tbg__low__funnel (TBG==TFB)."),
  tfb_lead_form_submit FLOAT64 OPTIONS(description="Unified from raw tbg__lead__form__submit (TBG==TFB)."),
  tfb_invoca_sales_intent_dda FLOAT64 OPTIONS(description="Unified from raw tbg__invoca__sales__intent_dda (TBG==TFB)."),
  tfb_invoca_order_dda FLOAT64 OPTIONS(description="Unified from raw tbg__invoca__order_dda (TBG==TFB)."),

  tfb_credit_check FLOAT64 OPTIONS(description="Raw: tfb__credit__check."),
  tfb_invoca_sales_calls FLOAT64 OPTIONS(description="Raw: tfb__invoca__sales__calls."),
  tfb_leads FLOAT64 OPTIONS(description="Raw: tfb__leads."),
  tfb_quality_traffic FLOAT64 OPTIONS(description="Raw: tfb__quality__traffic."),
  tfb_hint_ec FLOAT64 OPTIONS(description="Raw: tfb_hint_ec."),
  total_tfb_conversions FLOAT64 OPTIONS(description="Raw: total_tfb__conversions."),

  /* =========================
     OTHER
  ========================== */
  magenta_pqt FLOAT64 OPTIONS(description="Raw: magenta_pqt."),

  bronze_updated_at TIMESTAMP OPTIONS(description="System timestamp when row was inserted/updated in Bronze.")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id;
