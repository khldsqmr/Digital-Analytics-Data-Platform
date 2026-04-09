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

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_monthly`
AS

SELECT
    DATE_TRUNC(event_date, MONTH) AS monthStart,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,

    SUM(adobe_entries) AS adobe_entries,
    SUM(adobe_pspv_actuals) AS adobe_pspv_actuals,
    SUM(adobe_cart_starts) AS adobe_cart_starts,
    SUM(adobe_cart_start_plus) AS adobe_cart_start_plus,
    SUM(adobe_cart_checkout_visits) AS adobe_cart_checkout_visits,
    SUM(adobe_checkout_review_visits) AS adobe_checkout_review_visits,
    SUM(adobe_postpaid_orders_tsr) AS adobe_postpaid_orders_tsr,
    SUM(adobe_orders_web_unassisted) AS adobe_orders_web_unassisted,
    SUM(adobe_orders_web_assisted) AS adobe_orders_web_assisted,
    SUM(adobe_orders_app_unassisted) AS adobe_orders_app_unassisted,
    SUM(adobe_orders_app_assisted) AS adobe_orders_app_assisted,
    SUM(adobe_orders_web_all) AS adobe_orders_web_all,
    SUM(adobe_orders_app_all) AS adobe_orders_app_all,
    SUM(adobe_orders_fully_unassisted) AS adobe_orders_fully_unassisted,
    SUM(adobe_orders_fully_assisted) AS adobe_orders_fully_assisted,
    SUM(adobe_orders_all) AS adobe_orders_all,
    SUM(adobe_storelocator_visits) AS adobe_storelocator_visits,

    SUM(sa360_clicks_brand) AS sa360_clicks_brand,
    SUM(sa360_clicks_nonbrand) AS sa360_clicks_nonbrand,
    SUM(sa360_clicks_all) AS sa360_clicks_all,
    SUM(sa360_cart_start_plus_brand) AS sa360_cart_start_plus_brand,
    SUM(sa360_cart_start_plus_nonbrand) AS sa360_cart_start_plus_nonbrand,
    SUM(sa360_cart_start_plus_all) AS sa360_cart_start_plus_all,

    SUM(gsc_clicks_brand) AS gsc_clicks_brand,
    SUM(gsc_clicks_nonbrand) AS gsc_clicks_nonbrand,
    SUM(gsc_clicks_all) AS gsc_clicks_all,
    SUM(gsc_impressions_brand) AS gsc_impressions_brand,
    SUM(gsc_impressions_nonbrand) AS gsc_impressions_nonbrand,
    SUM(gsc_impressions_all) AS gsc_impressions_all,

    SUM(platform_spend) AS platform_spend,

    SUM(gmb_search_impressions_all) AS gmb_search_impressions_all,
    SUM(gmb_maps_impressions_all) AS gmb_maps_impressions_all,
    SUM(gmb_impressions_all) AS gmb_impressions_all,
    SUM(gmb_call_clicks) AS gmb_call_clicks,
    SUM(gmb_website_clicks) AS gmb_website_clicks,
    SUM(gmb_directions_clicks) AS gmb_directions_clicks

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
GROUP BY
    monthStart,
    lob,
    channel;