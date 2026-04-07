/* =================================================================================================
FILE: 04_vw_sdi_tsd_bronze_sa360Perf_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_sa360Perf_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Perf_daily

PURPOSE:
  Canonical Bronze SA360 performance daily view for the Total Search Dashboard.
  This view deduplicates the SA360 campaign daily performance snapshot and preserves
  the campaign-level daily metrics needed downstream.

BUSINESS GRAIN:
  One row per:
      account_id
      campaign_id
      event_date

DEDUPE LOGIC:
  Latest row per:
      account_id + campaign_id + date_yyyymmdd
  ordered by:
      File_Load_datetime DESC,
      Filename DESC,
      __insert_date DESC

KEY MODELING NOTES:
  - This is a source-close Bronze layer object
  - No brand / nonbrand logic is applied here
  - No joins to entity/settings metadata are applied here
  - LOB and channel are not forced here; they are standardized in Silver

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Perf_daily`
AS

WITH ranked AS (
    SELECT
        raw.account_id,
        raw.campaign_id,
        CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING)) AS event_date,

        raw.account_name,
        raw.customer_id,
        raw.customer_name,
        raw.resource_name,
        raw.segments_date,
        raw.client_manager_id,
        raw.client_manager_name,

        SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
        DATETIME(raw.File_Load_datetime) AS file_load_datetime,
        raw.Filename AS filename,

        raw._ma_hint_ec__eligibility__check_ AS ma_hint_ec_eligibility_check,

        raw.aal,
        raw.add_a__line AS add_a_line,
        raw.all_conversions,
        raw.bi,
        raw.bts__quality__traffic AS bts_quality_traffic,
        raw.buying__intent AS buying_intent,

        SAFE_CAST(raw.clicks AS FLOAT64) AS clicks,
        SAFE_CAST(raw.impressions AS FLOAT64) AS impressions,

        SAFE_CAST(raw.cost_micros AS FLOAT64) AS cost_micros,
        SAFE_CAST(raw.cost_micros AS FLOAT64) / 1000000 AS cost,

        raw.cart__start_ AS cart_start,
        raw.postpaid__cart__start_ AS postpaid_cart_start,
        raw.postpaid_pspv_ AS postpaid_pspv,

        raw.connect__low__funnel__prospect AS connect_low_funnel_prospect,
        raw.connect__low__funnel__visit AS connect_low_funnel_visit,
        raw.connect_qt,

        raw.digital__gross__add AS digital_gross_add,

        raw.fiber__activations AS fiber_activations,
        raw.fiber__pre__order AS fiber_pre_order,
        raw.fiber__waitlist__sign__up AS fiber_waitlist_sign_up,
        raw.fiber__web__orders AS fiber_web_orders,
        raw.fiber_ec,
        raw.fiber_ec_dda,
        raw.fiber_sec,
        raw.fiber_sec_dda,

        raw.hint__invoca__calls AS hint_invoca_calls,
        raw.hint__offline__invoca__calls AS hint_offline_invoca_calls,
        raw.hint__offline__invoca__eligibility AS hint_offline_invoca_eligibility,
        raw.hint__offline__invoca__order AS hint_offline_invoca_order,
        raw.hint__offline__invoca__order_rt_ AS hint_offline_invoca_order_rt,
        raw.hint__offline__invoca__sales__opp AS hint_offline_invoca_sales_opp,
        raw.hint__web__orders AS hint_web_orders,
        raw.hint_ec,
        raw.hint_sec,

        raw.magenta_pqt,

        raw.metro__low__funnel_cs_ AS metro_low_funnel_cs,
        raw.metro__mid__funnel__prospect AS metro_mid_funnel_prospect,
        raw.metro__top__funnel__prospect AS metro_top_funnel_prospect,
        raw.metro__upper__funnel__prospect AS metro_upper_funnel_prospect,
        raw.metro_hint_qt,
        raw.metro_qt,

        raw.t__mobile__prepaid__low__funnel__prospect AS tmo_prepaid_low_funnel_prospect,
        raw.tmo__top__funnel__prospect AS tmo_top_funnel_prospect,
        raw.tmo__upper__funnel__prospect AS tmo_upper_funnel_prospect,

        raw.tbg__low__funnel AS tfb_low_funnel,
        raw.tbg__lead__form__submit AS tfb_lead_form_submit,
        raw.tbg__invoca__sales__intent_dda AS tfb_invoca_sales_intent_dda,
        raw.tbg__invoca__order_dda AS tfb_invoca_order_dda,

        raw.tfb__credit__check AS tfb_credit_check,
        raw.tfb_hint_ec,
        raw.tfb__invoca__sales__calls AS tfb_invoca_sales_calls,
        raw.tfb__leads AS tfb_leads,
        raw.tfb__quality__traffic AS tfb_quality_traffic,
        raw.total_tfb__conversions AS total_tfb_conversions,

        ROW_NUMBER() OVER (
            PARTITION BY raw.account_id, raw.campaign_id, CAST(raw.date_yyyymmdd AS STRING)
            ORDER BY
                DATETIME(raw.File_Load_datetime) DESC,
                raw.Filename DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` raw
)

SELECT
    account_id,
    campaign_id,
    date_yyyymmdd,
    event_date,

    account_name,
    customer_id,
    customer_name,
    resource_name,
    segments_date,
    client_manager_id,
    client_manager_name,

    insert_date,
    file_load_datetime,
    filename,

    ma_hint_ec_eligibility_check,
    aal,
    add_a_line,
    all_conversions,
    bi,
    bts_quality_traffic,
    buying_intent,
    clicks,
    impressions,
    cost_micros,
    cost,
    cart_start,
    postpaid_cart_start,
    postpaid_pspv,
    connect_low_funnel_prospect,
    connect_low_funnel_visit,
    connect_qt,
    digital_gross_add,
    fiber_activations,
    fiber_pre_order,
    fiber_waitlist_sign_up,
    fiber_web_orders,
    fiber_ec,
    fiber_ec_dda,
    fiber_sec,
    fiber_sec_dda,
    hint_invoca_calls,
    hint_offline_invoca_calls,
    hint_offline_invoca_eligibility,
    hint_offline_invoca_order,
    hint_offline_invoca_order_rt,
    hint_offline_invoca_sales_opp,
    hint_web_orders,
    hint_ec,
    hint_sec,
    magenta_pqt,
    metro_low_funnel_cs,
    metro_mid_funnel_prospect,
    metro_top_funnel_prospect,
    metro_upper_funnel_prospect,
    metro_hint_qt,
    metro_qt,
    tmo_prepaid_low_funnel_prospect,
    tmo_top_funnel_prospect,
    tmo_upper_funnel_prospect,
    tfb_low_funnel,
    tfb_lead_form_submit,
    tfb_invoca_sales_intent_dda,
    tfb_invoca_order_dda,
    tfb_credit_check,
    tfb_hint_ec,
    tfb_invoca_sales_calls,
    tfb_leads,
    tfb_quality_traffic,
    total_tfb_conversions
FROM ranked
WHERE rn = 1;