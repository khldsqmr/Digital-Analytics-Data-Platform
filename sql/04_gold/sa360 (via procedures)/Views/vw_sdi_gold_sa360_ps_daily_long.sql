/*
===============================================================================
VIEW: vw_gold_sa360_daily_long_reporting
SOURCE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily

GRAIN (UNIQUE ROW):
  - account_name
  - campaign_type
  - date
  - lob
  - ad_platform
  - metric_name

PURPOSE:
  Reporting-friendly daily long view (metric_name / metric_value), using only
  requested dimensions + all metrics.

NOTES:
  - UNPIVOT from Gold Daily wide
  - Aggregates after unpivot to ensure correct grain when dropping campaign_id/account_id
===============================================================================
*/

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_gold_sa360_daily_long_reporting` AS
WITH src AS (
  SELECT
    account_name,
    campaign_type,
    date,
    lob,
    ad_platform,

    impressions, clicks, cost, all_conversions,
    bi, buying_intent, bts_quality_traffic, digital_gross_add, magenta_pqt,
    cart_start, postpaid_cart_start, postpaid_pspv, aal, add_a_line,
    connect_low_funnel_prospect, connect_low_funnel_visit, connect_qt,
    hint_ec, hint_sec, hint_web_orders, hint_invoca_calls, hint_offline_invoca_calls,
    hint_offline_invoca_eligibility, hint_offline_invoca_order, hint_offline_invoca_order_rt,
    hint_offline_invoca_sales_opp, ma_hint_ec_eligibility_check,
    fiber_activations, fiber_pre_order, fiber_waitlist_sign_up, fiber_web_orders,
    fiber_ec, fiber_ec_dda, fiber_sec, fiber_sec_dda,
    metro_low_funnel_cs, metro_mid_funnel_prospect, metro_top_funnel_prospect, metro_upper_funnel_prospect,
    metro_hint_qt, metro_qt,
    tmo_prepaid_low_funnel_prospect, tmo_top_funnel_prospect, tmo_upper_funnel_prospect,
    tfb_low_funnel, tfb_lead_form_submit, tfb_invoca_sales_intent_dda, tfb_invoca_order_dda,
    tfb_credit_check, tfb_hint_ec, tfb_invoca_sales_calls, tfb_leads, tfb_quality_traffic,
    total_tfb_conversions
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily`
  WHERE date IS NOT NULL
),
longified AS (
  SELECT
    account_name,
    campaign_type,
    date,
    lob,
    ad_platform,
    metric_name,
    CAST(metric_value AS FLOAT64) AS metric_value
  FROM src
  UNPIVOT EXCLUDE NULLS (
    metric_value FOR metric_name IN (
      impressions, clicks, cost, all_conversions,
      bi, buying_intent, bts_quality_traffic, digital_gross_add, magenta_pqt,
      cart_start, postpaid_cart_start, postpaid_pspv, aal, add_a_line,
      connect_low_funnel_prospect, connect_low_funnel_visit, connect_qt,
      hint_ec, hint_sec, hint_web_orders, hint_invoca_calls, hint_offline_invoca_calls,
      hint_offline_invoca_eligibility, hint_offline_invoca_order, hint_offline_invoca_order_rt,
      hint_offline_invoca_sales_opp, ma_hint_ec_eligibility_check,
      fiber_activations, fiber_pre_order, fiber_waitlist_sign_up, fiber_web_orders,
      fiber_ec, fiber_ec_dda, fiber_sec, fiber_sec_dda,
      metro_low_funnel_cs, metro_mid_funnel_prospect, metro_top_funnel_prospect, metro_upper_funnel_prospect,
      metro_hint_qt, metro_qt,
      tmo_prepaid_low_funnel_prospect, tmo_top_funnel_prospect, tmo_upper_funnel_prospect,
      tfb_low_funnel, tfb_lead_form_submit, tfb_invoca_sales_intent_dda, tfb_invoca_order_dda,
      tfb_credit_check, tfb_hint_ec, tfb_invoca_sales_calls, tfb_leads, tfb_quality_traffic,
      total_tfb_conversions
    )
  )
)

SELECT
  account_name AS `Account Name`,
  campaign_type AS `Campaign Type`,
  date AS `Date`,
  lob AS `Lob`,
  ad_platform AS `Ad Platform`,
  metric_name,
  SUM(COALESCE(metric_value, 0)) AS metric_value
FROM longified
GROUP BY 1,2,3,4,5,6;