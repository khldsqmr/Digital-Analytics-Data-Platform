/* =================================================================================================
FILE: 02_vw_sdi_tsd_gold_unifiedSunSat_weekly.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_gold_unifiedSunSat_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly

PURPOSE:
  Unified Gold weekly reporting mart for the Total Search Dashboard using
  Sunday-to-Saturday weeks.

BUSINESS GRAIN:
  One row per:
      weekSunToSat
      lob
      channel

KEY MODELING NOTES:
  - Uses NULL-aware aggregation
  - If a metric is NULL for all contributing daily rows, weekly result stays NULL
  - Prevents fake zero rows for non-applicable source/channel combinations
  - Adobe T-Life App Visits is aggregated from Gold daily using the same NULL-aware weekly logic

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unifiedSunSat_weekly`
AS

SELECT
    DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY) AS weekSunToSat,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,

    CASE WHEN COUNT(adobe_entries) = 0 THEN NULL ELSE SUM(adobe_entries) END AS adobe_entries,
    CASE WHEN COUNT(adobe_pspv_actuals) = 0 THEN NULL ELSE SUM(adobe_pspv_actuals) END AS adobe_pspv_actuals,
    CASE WHEN COUNT(adobe_cart_starts) = 0 THEN NULL ELSE SUM(adobe_cart_starts) END AS adobe_cart_starts,
    CASE WHEN COUNT(adobe_cart_start_plus) = 0 THEN NULL ELSE SUM(adobe_cart_start_plus) END AS adobe_cart_start_plus,
    CASE WHEN COUNT(adobe_cart_checkout_visits) = 0 THEN NULL ELSE SUM(adobe_cart_checkout_visits) END AS adobe_cart_checkout_visits,
    CASE WHEN COUNT(adobe_checkout_review_visits) = 0 THEN NULL ELSE SUM(adobe_checkout_review_visits) END AS adobe_checkout_review_visits,
    CASE WHEN COUNT(adobe_postpaid_orders_tsr) = 0 THEN NULL ELSE SUM(adobe_postpaid_orders_tsr) END AS adobe_postpaid_orders_tsr,
    CASE WHEN COUNT(adobe_orders_web_unassisted) = 0 THEN NULL ELSE SUM(adobe_orders_web_unassisted) END AS adobe_orders_web_unassisted,
    CASE WHEN COUNT(adobe_orders_web_assisted) = 0 THEN NULL ELSE SUM(adobe_orders_web_assisted) END AS adobe_orders_web_assisted,
    CASE WHEN COUNT(adobe_orders_app_unassisted) = 0 THEN NULL ELSE SUM(adobe_orders_app_unassisted) END AS adobe_orders_app_unassisted,
    CASE WHEN COUNT(adobe_orders_app_assisted) = 0 THEN NULL ELSE SUM(adobe_orders_app_assisted) END AS adobe_orders_app_assisted,
    CASE WHEN COUNT(adobe_orders_web_all) = 0 THEN NULL ELSE SUM(adobe_orders_web_all) END AS adobe_orders_web_all,
    CASE WHEN COUNT(adobe_orders_app_all) = 0 THEN NULL ELSE SUM(adobe_orders_app_all) END AS adobe_orders_app_all,
    CASE WHEN COUNT(adobe_orders_fully_unassisted) = 0 THEN NULL ELSE SUM(adobe_orders_fully_unassisted) END AS adobe_orders_fully_unassisted,
    CASE WHEN COUNT(adobe_orders_fully_assisted) = 0 THEN NULL ELSE SUM(adobe_orders_fully_assisted) END AS adobe_orders_fully_assisted,
    CASE WHEN COUNT(adobe_orders_all) = 0 THEN NULL ELSE SUM(adobe_orders_all) END AS adobe_orders_all,
    CASE WHEN COUNT(adobe_storelocator_visits) = 0 THEN NULL ELSE SUM(adobe_storelocator_visits) END AS adobe_storelocator_visits,
    CASE WHEN COUNT(adobeTLifeAppVisits) = 0 THEN NULL ELSE SUM(adobeTLifeAppVisits) END AS adobeTLifeAppVisits,

    CASE WHEN COUNT(sa360_clicks_brand) = 0 THEN NULL ELSE SUM(sa360_clicks_brand) END AS sa360_clicks_brand,
    CASE WHEN COUNT(sa360_clicks_nonbrand) = 0 THEN NULL ELSE SUM(sa360_clicks_nonbrand) END AS sa360_clicks_nonbrand,
    CASE WHEN COUNT(sa360_clicks_all) = 0 THEN NULL ELSE SUM(sa360_clicks_all) END AS sa360_clicks_all,
    CASE WHEN COUNT(sa360_cart_start_plus_brand) = 0 THEN NULL ELSE SUM(sa360_cart_start_plus_brand) END AS sa360_cart_start_plus_brand,
    CASE WHEN COUNT(sa360_cart_start_plus_nonbrand) = 0 THEN NULL ELSE SUM(sa360_cart_start_plus_nonbrand) END AS sa360_cart_start_plus_nonbrand,
    CASE WHEN COUNT(sa360_cart_start_plus_all) = 0 THEN NULL ELSE SUM(sa360_cart_start_plus_all) END AS sa360_cart_start_plus_all,

    CASE WHEN COUNT(gsc_clicks_brand) = 0 THEN NULL ELSE SUM(gsc_clicks_brand) END AS gsc_clicks_brand,
    CASE WHEN COUNT(gsc_clicks_nonbrand) = 0 THEN NULL ELSE SUM(gsc_clicks_nonbrand) END AS gsc_clicks_nonbrand,
    CASE WHEN COUNT(gsc_clicks_all) = 0 THEN NULL ELSE SUM(gsc_clicks_all) END AS gsc_clicks_all,
    CASE WHEN COUNT(gsc_impressions_brand) = 0 THEN NULL ELSE SUM(gsc_impressions_brand) END AS gsc_impressions_brand,
    CASE WHEN COUNT(gsc_impressions_nonbrand) = 0 THEN NULL ELSE SUM(gsc_impressions_nonbrand) END AS gsc_impressions_nonbrand,
    CASE WHEN COUNT(gsc_impressions_all) = 0 THEN NULL ELSE SUM(gsc_impressions_all) END AS gsc_impressions_all,

    CASE WHEN COUNT(platform_spend) = 0 THEN NULL ELSE SUM(platform_spend) END AS platform_spend,

    CASE WHEN COUNT(gmb_search_impressions_all) = 0 THEN NULL ELSE SUM(gmb_search_impressions_all) END AS gmb_search_impressions_all,
    CASE WHEN COUNT(gmb_maps_impressions_all) = 0 THEN NULL ELSE SUM(gmb_maps_impressions_all) END AS gmb_maps_impressions_all,
    CASE WHEN COUNT(gmb_impressions_all) = 0 THEN NULL ELSE SUM(gmb_impressions_all) END AS gmb_impressions_all,
    CASE WHEN COUNT(gmb_call_clicks) = 0 THEN NULL ELSE SUM(gmb_call_clicks) END AS gmb_call_clicks,
    CASE WHEN COUNT(gmb_website_clicks) = 0 THEN NULL ELSE SUM(gmb_website_clicks) END AS gmb_website_clicks,
    CASE WHEN COUNT(gmb_directions_clicks) = 0 THEN NULL ELSE SUM(gmb_directions_clicks) END AS gmb_directions_clicks

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
GROUP BY
    weekSunToSat,
    lob,
    channel
;