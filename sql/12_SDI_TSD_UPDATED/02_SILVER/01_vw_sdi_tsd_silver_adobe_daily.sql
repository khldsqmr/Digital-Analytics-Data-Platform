/* =================================================================================================
FILE: 01_vw_sdi_tsd_silver_adobe_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_silver_adobe_daily

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeV2_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeOrders_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeCartStartPlus_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeStoreLocator_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeTLifeAppVisits_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily

PURPOSE:
  Unified Silver Adobe daily source mart for the Total Search Dashboard.

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

KEY MODELING NOTES:
  - Adobe source channels are conformed before joining
  - NATURAL SEARCH / ORGANIC SEARCH is standardized to ORGANIC SEARCH
  - Adobe paid search child channels are rolled up into PAID SEARCH
  - Each source is re-aggregated after channel mapping to protect the reporting grain
  - Sources are joined using a unioned keyset to avoid duplicate expansion
  - Nulls are preserved; no new zeroes are introduced
  - Adobe T-Life App Visits is incorporated as a separate Adobe source family

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily`
AS

WITH adobe_v2_mapped AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        CASE
            WHEN UPPER(TRIM(channel)) IN (
                'PAID SEARCH: BRAND',
                'PAID SEARCH: NON-BRAND',
                'PAID SEARCH: PLAS',
                'PERFORMANCE MAX',
                'PAID SEARCH'
            ) THEN 'PAID SEARCH'
            WHEN UPPER(TRIM(channel)) IN ('NATURAL SEARCH', 'ORGANIC SEARCH') THEN 'ORGANIC SEARCH'
            ELSE UPPER(TRIM(channel))
        END AS channel,
        SUM(adobe_entries)                 AS adobe_entries,
        SUM(adobe_pspv_actuals)            AS adobe_pspv_actuals,
        SUM(adobe_cart_starts)             AS adobe_cart_starts,
        SUM(adobe_cart_checkout_visits)    AS adobe_cart_checkout_visits,
        SUM(adobe_checkout_review_visits)  AS adobe_checkout_review_visits,
        SUM(adobe_postpaid_orders_tsr)     AS adobe_postpaid_orders_tsr
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeV2_daily`
    GROUP BY 1, 2, 3
),

adobe_orders_mapped AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        CASE
            WHEN UPPER(TRIM(channel)) IN (
                'PAID SEARCH: BRAND',
                'PAID SEARCH: NON-BRAND',
                'PAID SEARCH: PLAS',
                'PERFORMANCE MAX',
                'PAID SEARCH'
            ) THEN 'PAID SEARCH'
            WHEN UPPER(TRIM(channel)) IN ('NATURAL SEARCH', 'ORGANIC SEARCH') THEN 'ORGANIC SEARCH'
            ELSE UPPER(TRIM(channel))
        END AS channel,
        SUM(adobe_orders_web_unassisted)   AS adobe_orders_web_unassisted,
        SUM(adobe_orders_web_assisted)     AS adobe_orders_web_assisted,
        SUM(adobe_orders_app_unassisted)   AS adobe_orders_app_unassisted,
        SUM(adobe_orders_app_assisted)     AS adobe_orders_app_assisted,
        SUM(adobe_orders_web_all)          AS adobe_orders_web_all,
        SUM(adobe_orders_app_all)          AS adobe_orders_app_all,
        SUM(adobe_orders_fully_unassisted) AS adobe_orders_fully_unassisted,
        SUM(adobe_orders_fully_assisted)   AS adobe_orders_fully_assisted,
        SUM(adobe_orders_all)              AS adobe_orders_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeOrders_daily`
    GROUP BY 1, 2, 3
),

adobe_cartplus_mapped AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        CASE
            WHEN UPPER(TRIM(channel)) IN (
                'PAID SEARCH: BRAND',
                'PAID SEARCH: NON-BRAND',
                'PAID SEARCH: PLAS',
                'PERFORMANCE MAX',
                'PAID SEARCH'
            ) THEN 'PAID SEARCH'
            WHEN UPPER(TRIM(channel)) IN ('NATURAL SEARCH', 'ORGANIC SEARCH') THEN 'ORGANIC SEARCH'
            ELSE UPPER(TRIM(channel))
        END AS channel,
        SUM(adobe_cart_start_plus) AS adobe_cart_start_plus
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeCartStartPlus_daily`
    GROUP BY 1, 2, 3
),

adobe_storelocator_mapped AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        CASE
            WHEN UPPER(TRIM(channel)) IN (
                'PAID SEARCH: BRAND',
                'PAID SEARCH: NON-BRAND',
                'PAID SEARCH: PLAS',
                'PERFORMANCE MAX',
                'PAID SEARCH'
            ) THEN 'PAID SEARCH'
            WHEN UPPER(TRIM(channel)) IN ('NATURAL SEARCH', 'ORGANIC SEARCH') THEN 'ORGANIC SEARCH'
            ELSE UPPER(TRIM(channel))
        END AS channel,
        SUM(adobe_storelocator_visits) AS adobe_storelocator_visits
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeStoreLocator_daily`
    GROUP BY 1, 2, 3
),

adobe_tlifeappvisits_mapped AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        CASE
            WHEN UPPER(TRIM(channel)) IN (
                'PAID SEARCH: BRAND',
                'PAID SEARCH: NON-BRAND',
                'PAID SEARCH: PLAS',
                'PERFORMANCE MAX',
                'PAID SEARCH'
            ) THEN 'PAID SEARCH'
            WHEN UPPER(TRIM(channel)) IN ('NATURAL SEARCH', 'ORGANIC SEARCH') THEN 'ORGANIC SEARCH'
            ELSE UPPER(TRIM(channel))
        END AS channel,
        SUM(adobeTLifeAppVisits) AS adobeTLifeAppVisits
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeTLifeAppVisits_daily`
    GROUP BY 1, 2, 3
),

all_keys AS (
    SELECT event_date, lob, channel FROM adobe_v2_mapped
    UNION DISTINCT
    SELECT event_date, lob, channel FROM adobe_orders_mapped
    UNION DISTINCT
    SELECT event_date, lob, channel FROM adobe_cartplus_mapped
    UNION DISTINCT
    SELECT event_date, lob, channel FROM adobe_storelocator_mapped
    UNION DISTINCT
    SELECT event_date, lob, channel FROM adobe_tlifeappvisits_mapped
)

SELECT
    k.event_date,
    UPPER(TRIM(k.lob)) AS lob,
    UPPER(TRIM(k.channel)) AS channel,

    v2.adobe_entries,
    v2.adobe_pspv_actuals,
    v2.adobe_cart_starts,
    cp.adobe_cart_start_plus,
    v2.adobe_cart_checkout_visits,
    v2.adobe_checkout_review_visits,
    v2.adobe_postpaid_orders_tsr,

    ord.adobe_orders_web_unassisted,
    ord.adobe_orders_web_assisted,
    ord.adobe_orders_app_unassisted,
    ord.adobe_orders_app_assisted,
    ord.adobe_orders_web_all,
    ord.adobe_orders_app_all,
    ord.adobe_orders_fully_unassisted,
    ord.adobe_orders_fully_assisted,
    ord.adobe_orders_all,

    sl.adobe_storelocator_visits,
    tl.adobeTLifeAppVisits
FROM all_keys k
LEFT JOIN adobe_v2_mapped v2
    ON k.event_date = v2.event_date
   AND k.lob        = v2.lob
   AND k.channel    = v2.channel
LEFT JOIN adobe_orders_mapped ord
    ON k.event_date = ord.event_date
   AND k.lob        = ord.lob
   AND k.channel    = ord.channel
LEFT JOIN adobe_cartplus_mapped cp
    ON k.event_date = cp.event_date
   AND k.lob        = cp.lob
   AND k.channel    = cp.channel
LEFT JOIN adobe_storelocator_mapped sl
    ON k.event_date = sl.event_date
   AND k.lob        = sl.lob
   AND k.channel    = sl.channel
LEFT JOIN adobe_tlifeappvisits_mapped tl
    ON k.event_date = tl.event_date
   AND k.lob        = tl.lob
   AND k.channel    = tl.channel
;