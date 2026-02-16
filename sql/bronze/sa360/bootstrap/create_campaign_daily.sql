/*
===============================================================================
BOOTSTRAP | BRONZE | SA360 | CAMPAIGN DAILY (NORMALIZED)
===============================================================================

PURPOSE
-------
Create normalized Bronze Campaign Daily table.

This table:
  • Preserves all raw data
  • Normalizes column names
  • Removes double underscores
  • Removes leading/trailing underscores
  • Documents original raw column names
  • Converts cost_micros into cost
  • Adds bronze_inserted_at timestamp

SOURCE
------
google_search_ads_360_campaigns_tmo

GRAIN
-----
account_id + campaign_id + date_yyyymmdd

PARTITION
---------
date_serial (numeric date from source)

CLUSTER
-------
account_id, campaign_id
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
(

/* ============================================================================
   IDENTIFIERS
============================================================================ */

account_id STRING OPTIONS(description='Raw: account_id. SA360 advertiser account ID.'),

account_name STRING OPTIONS(description='Raw: account_name. Advertiser account name.'),

campaign_id STRING OPTIONS(description='Raw: campaign_id. Unique campaign identifier.'),

resource_name STRING OPTIONS(description='Raw: resource_name. Google Ads API resource path.'),

customer_id STRING OPTIONS(description='Raw: customer_id. Google Ads engine customer ID.'),

customer_name STRING OPTIONS(description='Raw: customer_name. Customer account name.'),

client_manager_id FLOAT64 OPTIONS(description='Raw: client_manager_id. Client manager numeric ID.'),

client_manager_name STRING OPTIONS(description='Raw: client_manager_name. Client manager name.'),

/* ============================================================================
   DATE FIELDS
============================================================================ */

date_yyyymmdd STRING OPTIONS(description='Raw: date_yyyymmdd. Reporting date in YYYYMMDD.'),

date_serial INT64 OPTIONS(description='Raw: date. Numeric date identifier from source.'),

segments_date STRING OPTIONS(description='Raw: segments_date. Google Ads segments date.'),

insert_date_id INT64 OPTIONS(description='Raw: __insert_date. Technical load identifier.'),

/* ============================================================================
   LOAD METADATA
============================================================================ */

file_load_datetime DATETIME OPTIONS(description='Raw: File_Load_datetime. ETL ingestion timestamp.'),

filename STRING OPTIONS(description='Raw: Filename. Source file path.'),

bronze_inserted_at TIMESTAMP OPTIONS(description='Timestamp when record inserted into Bronze table.'),

/* ============================================================================
   CORE PERFORMANCE
============================================================================ */

clicks FLOAT64 OPTIONS(description='Raw: clicks. Total clicks.'),

impressions FLOAT64 OPTIONS(description='Raw: impressions. Total impressions.'),

cost_micros FLOAT64 OPTIONS(description='Raw: cost_micros. Cost in micros.'),

cost FLOAT64 OPTIONS(description='Derived: cost_micros / 1e6. Cost in currency units.'),

all_conversions FLOAT64 OPTIONS(description='Raw: all_conversions. All conversions metric.'),

/* ============================================================================
   GENERAL METRICS
============================================================================ */

aal FLOAT64 OPTIONS(description='Raw: aal. Add-a-line related conversions.'),

add_a_line FLOAT64 OPTIONS(description='Raw: add_a__line. Add-a-line conversions.'),

bi FLOAT64 OPTIONS(description='Raw: bi. Business intent metric.'),

bts_quality_traffic FLOAT64 OPTIONS(description='Raw: bts__quality__traffic. BTS quality traffic metric.'),

buying_intent FLOAT64 OPTIONS(description='Raw: buying__intent. Buying intent score.'),

digital_gross_add FLOAT64 OPTIONS(description='Raw: digital__gross__add. Digital gross adds.'),

/* ============================================================================
   CART / POSTPAID
============================================================================ */

cart_start FLOAT64 OPTIONS(description='Raw: cart__start_. Cart start events.'),

postpaid_cart_start FLOAT64 OPTIONS(description='Raw: postpaid__cart__start_. Postpaid cart starts.'),

postpaid_pspv FLOAT64 OPTIONS(description='Raw: postpaid_pspv_. Postpaid PSPV metric.'),

/* ============================================================================
   CONNECT
============================================================================ */

connect_low_funnel_prospect FLOAT64 OPTIONS(description='Raw: connect__low__funnel__prospect.'),

connect_low_funnel_visit FLOAT64 OPTIONS(description='Raw: connect__low__funnel__visit.'),

connect_qt FLOAT64 OPTIONS(description='Raw: connect_qt. Connect qualified traffic.'),

/* ============================================================================
   HINT
============================================================================ */

hint_ec FLOAT64 OPTIONS(description='Raw: hint_ec. HINT eligibility checks.'),

hint_sec FLOAT64 OPTIONS(description='Raw: hint_sec. HINT secondary eligibility checks.'),

hint_web_orders FLOAT64 OPTIONS(description='Raw: hint__web__orders.'),

hint_invoca_calls FLOAT64 OPTIONS(description='Raw: hint__invoca__calls.'),

hint_offline_invoca_calls FLOAT64 OPTIONS(description='Raw: hint__offline__invoca__calls.'),

hint_offline_invoca_eligibility FLOAT64 OPTIONS(description='Raw: hint__offline__invoca__eligibility.'),

hint_offline_invoca_order FLOAT64 OPTIONS(description='Raw: hint__offline__invoca__order.'),

hint_offline_invoca_order_rt FLOAT64 OPTIONS(description='Raw: hint__offline__invoca__order_rt_.'),

hint_offline_invoca_sales_opp FLOAT64 OPTIONS(description='Raw: hint__offline__invoca__sales__opp.'),

ma_hint_ec_eligibility_check FLOAT64 OPTIONS(description='Raw: _ma_hint_ec__eligibility__check_.'),

/* ============================================================================
   FIBER
============================================================================ */

fiber_activations FLOAT64 OPTIONS(description='Raw: fiber__activations.'),

fiber_pre_order FLOAT64 OPTIONS(description='Raw: fiber__pre__order.'),

fiber_waitlist_sign_up FLOAT64 OPTIONS(description='Raw: fiber__waitlist__sign__up.'),

fiber_web_orders FLOAT64 OPTIONS(description='Raw: fiber__web__orders.'),

fiber_ec FLOAT64 OPTIONS(description='Raw: fiber_ec.'),

fiber_ec_dda FLOAT64 OPTIONS(description='Raw: fiber_ec_dda.'),

fiber_sec FLOAT64 OPTIONS(description='Raw: fiber_sec.'),

fiber_sec_dda FLOAT64 OPTIONS(description='Raw: fiber_sec_dda.'),

/* ============================================================================
   METRO
============================================================================ */

metro_low_funnel_cs FLOAT64 OPTIONS(description='Raw: metro__low__funnel_cs_.'),

metro_mid_funnel_prospect FLOAT64 OPTIONS(description='Raw: metro__mid__funnel__prospect.'),

metro_top_funnel_prospect FLOAT64 OPTIONS(description='Raw: metro__top__funnel__prospect.'),

metro_upper_funnel_prospect FLOAT64 OPTIONS(description='Raw: metro__upper__funnel__prospect.'),

metro_hint_qt FLOAT64 OPTIONS(description='Raw: metro_hint_qt.'),

metro_qt FLOAT64 OPTIONS(description='Raw: metro_qt.'),

/* ============================================================================
   TMO
============================================================================ */

tmo_top_funnel_prospect FLOAT64 OPTIONS(description='Raw: tmo__top__funnel__prospect.'),

tmo_upper_funnel_prospect FLOAT64 OPTIONS(description='Raw: tmo__upper__funnel__prospect.'),

tmo_prepaid_low_funnel_prospect FLOAT64 OPTIONS(description='Raw: t__mobile__prepaid__low__funnel__prospect.'),

/* ============================================================================
   TFB / TBG
============================================================================ */

tbg_low_funnel FLOAT64 OPTIONS(description='Raw: tbg__low__funnel.'),

tbg_lead_form_submit FLOAT64 OPTIONS(description='Raw: tbg__lead__form__submit.'),

tbg_invoca_sales_intent_dda FLOAT64 OPTIONS(description='Raw: tbg__invoca__sales__intent_dda.'),

tbg_invoca_order_dda FLOAT64 OPTIONS(description='Raw: tbg__invoca__order_dda.'),

tfb_credit_check FLOAT64 OPTIONS(description='Raw: tfb__credit__check.'),

tfb_invoca_sales_calls FLOAT64 OPTIONS(description='Raw: tfb__invoca__sales__calls.'),

tfb_leads FLOAT64 OPTIONS(description='Raw: tfb__leads.'),

tfb_quality_traffic FLOAT64 OPTIONS(description='Raw: tfb__quality__traffic.'),

tfb_hint_ec FLOAT64 OPTIONS(description='Raw: tfb_hint_ec.'),

total_tfb_conversions FLOAT64 OPTIONS(description='Raw: total_tfb__conversions.'),

/* ============================================================================
   OTHER
============================================================================ */

magenta_pqt FLOAT64 OPTIONS(description='Raw: magenta_pqt.')

)
PARTITION BY date_serial
CLUSTER BY account_id, campaign_id;
