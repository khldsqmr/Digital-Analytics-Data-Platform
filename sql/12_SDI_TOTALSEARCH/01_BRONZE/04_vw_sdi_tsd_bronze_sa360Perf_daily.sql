/* =================================================================================================
FILE: 04_vw_sdi_tsd_bronze_sa360Perf_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_sa360Perf_daily

SOURCE:
  Replace with your SA360 performance/source table

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Perf_daily

PURPOSE:
  Canonical Bronze SA360 performance daily view for the Total Search Dashboard.
  This view deduplicates the SA360 campaign daily performance snapshot and standardizes
  the key daily metrics needed downstream.

BUSINESS GRAIN:
  One row per:
      account_id
      campaign_id
      event_date

DEDUPE LOGIC:
  Latest row per:
      account_id + campaign_id + date_yyyymmdd
  ordered by:
      file_load_datetime DESC,
      filename DESC,
      insert_date DESC

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Perf_daily`
AS

WITH ranked AS (
    SELECT
        account_id,
        campaign_id,
        date_yyyymmdd,
        DATE(date) AS event_date,

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
        total_tfb_conversions,

        ROW_NUMBER() OVER (
            PARTITION BY account_id, campaign_id, date_yyyymmdd
            ORDER BY
                file_load_datetime DESC,
                filename DESC,
                insert_date DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
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