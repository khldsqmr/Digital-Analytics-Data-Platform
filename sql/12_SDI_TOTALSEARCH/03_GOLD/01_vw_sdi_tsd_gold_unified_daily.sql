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
  This view combines all daily-compatible search dashboard sources into one
  reporting-ready wide table at:
      event_date + lob + channel

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

KEY MODELING NOTES:
  - Uses a DISTINCT key spine from all daily silvers
  - Uses LEFT JOIN from the spine to each silver
  - Assumes each silver is already unique at event_date + lob + channel
  - Source-specific metrics are intentionally left as NULL when that source does not
    exist for the given event_date + lob + channel row
  - platform_spend remains COALESCE(..., 0) because spend is often expected to be
    additive and easier to consume as zero when absent
  - ProFound sources are intentionally excluded from this unified daily gold
  - This design prevents the misleading interpretation that SA360 / GSC / GMB / Adobe
    belong to every channel in the unified spine

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
        adobe_orders_all
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

    /* Adobe metrics: NULL when Adobe does not exist for this spine row */
    a.adobe_entries AS adobe_entries,
    a.adobe_pspv_actuals AS adobe_pspv_actuals,
    a.adobe_cart_starts AS adobe_cart_starts,
    a.adobe_cart_start_plus AS adobe_cart_start_plus,
    a.adobe_cart_checkout_visits AS adobe_cart_checkout_visits,
    a.adobe_checkout_review_visits AS adobe_checkout_review_visits,
    a.adobe_postpaid_orders_tsr AS adobe_postpaid_orders_tsr,
    a.adobe_orders_web_unassisted AS adobe_orders_web_unassisted,
    a.adobe_orders_web_assisted AS adobe_orders_web_assisted,
    a.adobe_orders_app_unassisted AS adobe_orders_app_unassisted,
    a.adobe_orders_app_assisted AS adobe_orders_app_assisted,
    a.adobe_orders_web_all AS adobe_orders_web_all,
    a.adobe_orders_app_all AS adobe_orders_app_all,
    a.adobe_orders_fully_unassisted AS adobe_orders_fully_unassisted,
    a.adobe_orders_fully_assisted AS adobe_orders_fully_assisted,
    a.adobe_orders_all AS adobe_orders_all,

    /* SA360 metrics: NULL when SA360 does not exist for this spine row */
    sa.sa360_clicks_brand AS sa360_clicks_brand,
    sa.sa360_clicks_nonbrand AS sa360_clicks_nonbrand,
    sa.sa360_clicks_all AS sa360_clicks_all,
    sa.sa360_cart_start_plus_brand AS sa360_cart_start_plus_brand,
    sa.sa360_cart_start_plus_nonbrand AS sa360_cart_start_plus_nonbrand,
    sa.sa360_cart_start_plus_all AS sa360_cart_start_plus_all,

    /* GSC metrics: NULL when GSC does not exist for this spine row */
    g.gsc_clicks_brand AS gsc_clicks_brand,
    g.gsc_clicks_nonbrand AS gsc_clicks_nonbrand,
    g.gsc_clicks_all AS gsc_clicks_all,
    g.gsc_impressions_brand AS gsc_impressions_brand,
    g.gsc_impressions_nonbrand AS gsc_impressions_nonbrand,
    g.gsc_impressions_all AS gsc_impressions_all,

    /* Spend: keep zero when absent */
    COALESCE(sp.platform_spend, 0) AS platform_spend,

    /* GMB metrics: NULL when GMB does not exist for this spine row */
    m.gmb_search_impressions_all AS gmb_search_impressions_all,
    m.gmb_maps_impressions_all AS gmb_maps_impressions_all,
    m.gmb_impressions_all AS gmb_impressions_all,
    m.gmb_call_clicks AS gmb_call_clicks,
    m.gmb_website_clicks AS gmb_website_clicks,
    m.gmb_directions_clicks AS gmb_directions_clicks

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