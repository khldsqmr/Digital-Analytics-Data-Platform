/* =================================================================================================
FILE: 03_vw_sdi_tsd_gold_unified_monthly.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_gold_unified_monthly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly

PURPOSE:
  Unified Gold monthly reporting mart for the Total Search Dashboard.

BUSINESS GRAIN:
  One row per:
      monthStart
      lob
      channel

PERIOD LOGIC:
  - monthStart = first day of the month

KEY MODELING NOTES:
  - Built by aggregating the unified daily gold
  - ProFound is intentionally excluded and remains available in separate gold views

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly`
AS

SELECT
    DATE_TRUNC(event_date, MONTH) AS monthStart,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,

    SUM(COALESCE(adobe_entries, 0)) AS adobe_entries,
    SUM(COALESCE(adobe_pspv_actuals, 0)) AS adobe_pspv_actuals,
    SUM(COALESCE(adobe_cart_starts, 0)) AS adobe_cart_starts,
    SUM(COALESCE(adobe_cart_start_plus, 0)) AS adobe_cart_start_plus,
    SUM(COALESCE(adobe_cart_checkout_visits, 0)) AS adobe_cart_checkout_visits,
    SUM(COALESCE(adobe_checkout_review_visits, 0)) AS adobe_checkout_review_visits,
    SUM(COALESCE(adobe_postpaid_orders_tsr, 0)) AS adobe_postpaid_orders_tsr,
    SUM(COALESCE(adobe_orders_web_unassisted, 0)) AS adobe_orders_web_unassisted,
    SUM(COALESCE(adobe_orders_web_assisted, 0)) AS adobe_orders_web_assisted,
    SUM(COALESCE(adobe_orders_app_unassisted, 0)) AS adobe_orders_app_unassisted,
    SUM(COALESCE(adobe_orders_app_assisted, 0)) AS adobe_orders_app_assisted,
    SUM(COALESCE(adobe_orders_web_all, 0)) AS adobe_orders_web_all,
    SUM(COALESCE(adobe_orders_app_all, 0)) AS adobe_orders_app_all,
    SUM(COALESCE(adobe_orders_fully_unassisted, 0)) AS adobe_orders_fully_unassisted,
    SUM(COALESCE(adobe_orders_fully_assisted, 0)) AS adobe_orders_fully_assisted,
    SUM(COALESCE(adobe_orders_all, 0)) AS adobe_orders_all,

    SUM(COALESCE(sa360_clicks_brand, 0)) AS sa360_clicks_brand,
    SUM(COALESCE(sa360_clicks_nonbrand, 0)) AS sa360_clicks_nonbrand,
    SUM(COALESCE(sa360_clicks_all, 0)) AS sa360_clicks_all,
    SUM(COALESCE(sa360_cart_start_plus_brand, 0)) AS sa360_cart_start_plus_brand,
    SUM(COALESCE(sa360_cart_start_plus_nonbrand, 0)) AS sa360_cart_start_plus_nonbrand,
    SUM(COALESCE(sa360_cart_start_plus_all, 0)) AS sa360_cart_start_plus_all,

    SUM(COALESCE(gsc_clicks_brand, 0)) AS gsc_clicks_brand,
    SUM(COALESCE(gsc_clicks_nonbrand, 0)) AS gsc_clicks_nonbrand,
    SUM(COALESCE(gsc_clicks_all, 0)) AS gsc_clicks_all,
    SUM(COALESCE(gsc_impressions_brand, 0)) AS gsc_impressions_brand,
    SUM(COALESCE(gsc_impressions_nonbrand, 0)) AS gsc_impressions_nonbrand,
    SUM(COALESCE(gsc_impressions_all, 0)) AS gsc_impressions_all,

    SUM(COALESCE(platform_spend, 0)) AS platform_spend,

    SUM(COALESCE(gmb_search_impressions_all, 0)) AS gmb_search_impressions_all,
    SUM(COALESCE(gmb_maps_impressions_all, 0)) AS gmb_maps_impressions_all,
    SUM(COALESCE(gmb_impressions_all, 0)) AS gmb_impressions_all,
    SUM(COALESCE(gmb_call_clicks, 0)) AS gmb_call_clicks,
    SUM(COALESCE(gmb_website_clicks, 0)) AS gmb_website_clicks,
    SUM(COALESCE(gmb_directions_clicks, 0)) AS gmb_directions_clicks

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
GROUP BY
    monthStart,
    lob,
    channel
;