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
        adobe_entries,
        adobe_pspv_actuals,
        adobe_cart_starts,
        adobe_cart_start_plus,
        adobe_cart_checkout_visits,
        adobe_checkout_review_visits,
        adobe_postpaid_orders_tsr,
        adobe_orders_web_unassisted,
        adobe_orders_web_assisted,
        adobe_orders_app_unassisted,
        adobe_orders_app_assisted,
        adobe_orders_web_all,
        adobe_orders_app_all,
        adobe_orders_fully_unassisted,
        adobe_orders_fully_assisted,
        adobe_orders_all,
        adobe_storelocator_visits
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily`
),

sa360 AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        sa360_clicks_brand,
        sa360_clicks_nonbrand,
        sa360_clicks_all,
        sa360_cart_start_plus_brand,
        sa360_cart_start_plus_nonbrand,
        sa360_cart_start_plus_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_sa360_daily`
),

gsc AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        gsc_clicks_brand,
        gsc_clicks_nonbrand,
        gsc_clicks_all,
        gsc_impressions_brand,
        gsc_impressions_nonbrand,
        gsc_impressions_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gsc_daily`
),

spend AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        platform_spend
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_platformSpend_daily`
),

gmb AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        gmb_search_impressions_all,
        gmb_maps_impressions_all,
        gmb_impressions_all,
        gmb_call_clicks,
        gmb_website_clicks,
        gmb_directions_clicks
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_gmb_daily`
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