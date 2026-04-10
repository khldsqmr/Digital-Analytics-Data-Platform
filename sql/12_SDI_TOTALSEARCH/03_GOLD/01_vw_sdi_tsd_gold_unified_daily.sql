/* =================================================================================================
FILE: 01_vw_sdi_tsd_gold_unified_daily.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_gold_unified_daily

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_sa360_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gsc_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_platformSpend_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gmb_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily

PURPOSE:
  Unified Gold daily source mart for the Total Search Dashboard.

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

KEY MODELING NOTES:
  - Uses a DISTINCT key spine from all daily silvers
  - Uses LEFT JOIN from the spine to each silver
  - Assumes each silver is already unique at event_date + lob + channel
  - Each source CTE is re-aggregated defensively to guarantee uniqueness before the join
  - Source-specific metrics remain NULL when not applicable for that source/channel/day
  - This preserves true sparsity and avoids fake zero-valued rows downstream
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_gold_unified_daily`
AS

WITH key_spine AS (
    SELECT DISTINCT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily`

    UNION DISTINCT

    SELECT DISTINCT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_sa360_daily`

    UNION DISTINCT

    SELECT DISTINCT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gsc_daily`

    UNION DISTINCT

    SELECT DISTINCT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_platformSpend_daily`

    UNION DISTINCT

    SELECT DISTINCT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gmb_daily`
),

adobe AS (
    SELECT
        event_date,
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
        SUM(adobe_storelocator_visits) AS adobe_storelocator_visits
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily`
    GROUP BY 1, 2, 3
),

sa360 AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        SUM(sa360_clicks_brand) AS sa360_clicks_brand,
        SUM(sa360_clicks_nonbrand) AS sa360_clicks_nonbrand,
        SUM(sa360_clicks_all) AS sa360_clicks_all,
        SUM(sa360_cart_start_plus_brand) AS sa360_cart_start_plus_brand,
        SUM(sa360_cart_start_plus_nonbrand) AS sa360_cart_start_plus_nonbrand,
        SUM(sa360_cart_start_plus_all) AS sa360_cart_start_plus_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_sa360_daily`
    GROUP BY 1, 2, 3
),

gsc AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        SUM(gsc_clicks_brand) AS gsc_clicks_brand,
        SUM(gsc_clicks_nonbrand) AS gsc_clicks_nonbrand,
        SUM(gsc_clicks_all) AS gsc_clicks_all,
        SUM(gsc_impressions_brand) AS gsc_impressions_brand,
        SUM(gsc_impressions_nonbrand) AS gsc_impressions_nonbrand,
        SUM(gsc_impressions_all) AS gsc_impressions_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gsc_daily`
    GROUP BY 1, 2, 3
),

spend AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        SUM(platform_spend) AS platform_spend
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_platformSpend_daily`
    GROUP BY 1, 2, 3
),

gmb AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,

        SUM(gmb_search_impressions_all) AS gmb_search_impressions_all,
        SUM(gmb_maps_impressions_all) AS gmb_maps_impressions_all,
        SUM(gmb_impressions_all) AS gmb_impressions_all,
        SUM(gmb_call_clicks) AS gmb_call_clicks,
        SUM(gmb_website_clicks) AS gmb_website_clicks,
        SUM(gmb_directions_clicks) AS gmb_directions_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gmb_daily`
    GROUP BY 1, 2, 3
)

SELECT
    ks.event_date,
    ks.lob,
    ks.channel,

    a.adobe_entries,
    a.adobe_pspv_actuals,
    a.adobe_cart_starts,
    a.adobe_cart_start_plus,
    a.adobe_cart_checkout_visits,
    a.adobe_checkout_review_visits,
    a.adobe_postpaid_orders_tsr,
    a.adobe_orders_web_unassisted,
    a.adobe_orders_web_assisted,
    a.adobe_orders_app_unassisted,
    a.adobe_orders_app_assisted,
    a.adobe_orders_web_all,
    a.adobe_orders_app_all,
    a.adobe_orders_fully_unassisted,
    a.adobe_orders_fully_assisted,
    a.adobe_orders_all,
    a.adobe_storelocator_visits,

    sa.sa360_clicks_brand,
    sa.sa360_clicks_nonbrand,
    sa.sa360_clicks_all,
    sa.sa360_cart_start_plus_brand,
    sa.sa360_cart_start_plus_nonbrand,
    sa.sa360_cart_start_plus_all,

    g.gsc_clicks_brand,
    g.gsc_clicks_nonbrand,
    g.gsc_clicks_all,
    g.gsc_impressions_brand,
    g.gsc_impressions_nonbrand,
    g.gsc_impressions_all,

    sp.platform_spend,

    m.gmb_search_impressions_all,
    m.gmb_maps_impressions_all,
    m.gmb_impressions_all,
    m.gmb_call_clicks,
    m.gmb_website_clicks,
    m.gmb_directions_clicks

FROM key_spine ks
LEFT JOIN adobe a
  ON ks.event_date = a.event_date
 AND ks.lob        = a.lob
 AND ks.channel    = a.channel
LEFT JOIN sa360 sa
  ON ks.event_date = sa.event_date
 AND ks.lob        = sa.lob
 AND ks.channel    = sa.channel
LEFT JOIN gsc g
  ON ks.event_date = g.event_date
 AND ks.lob        = g.lob
 AND ks.channel    = g.channel
LEFT JOIN spend sp
  ON ks.event_date = sp.event_date
 AND ks.lob        = sp.lob
 AND ks.channel    = sp.channel
LEFT JOIN gmb m
  ON ks.event_date = m.event_date
 AND ks.lob        = m.lob
 AND ks.channel    = m.channel
;