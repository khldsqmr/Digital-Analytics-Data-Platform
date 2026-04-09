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

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily

PURPOSE:
  Unified Silver Adobe daily source mart for the Total Search Dashboard.
  This view combines:
      - Adobe V2 funnel metrics
      - Adobe digital order metrics
      - Adobe Cart Start Plus
      - Adobe Store Locator visits
  into one standardized Adobe daily reporting layer.

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

METRICS INCLUDED:
  - adobe_entries
  - adobe_pspv_actuals
  - adobe_cart_starts
  - adobe_cart_start_plus
  - adobe_cart_checkout_visits
  - adobe_checkout_review_visits
  - adobe_postpaid_orders_tsr
  - adobe_orders_web_unassisted
  - adobe_orders_web_assisted
  - adobe_orders_app_unassisted
  - adobe_orders_app_assisted
  - adobe_orders_web_all
  - adobe_orders_app_all
  - adobe_orders_fully_unassisted
  - adobe_orders_fully_assisted
  - adobe_orders_all
  - adobe_storelocator_visits

KEY MODELING NOTES:
  - Adobe source channels are conformed before joining
  - ORGANIC/NATURAL SEARCH is standardized to ORGANIC SEARCH
  - Adobe paid search child channels are rolled up into PAID SEARCH
  - Each source is re-aggregated after channel mapping to protect the reporting grain
  - FULL OUTER JOIN is applied only after each source is uniquely at event_date + lob + channel

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
                'PERFORMANCE MAX'
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
    GROUP BY event_date, lob, channel
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
                'PERFORMANCE MAX'
            ) THEN 'PAID SEARCH'
            WHEN UPPER(TRIM(channel)) IN ('NATURAL SEARCH', 'ORGANIC SEARCH') THEN 'ORGANIC SEARCH'
            ELSE UPPER(TRIM(channel))
        END AS channel,
        SUM(adobe_orders_web_unassisted)    AS adobe_orders_web_unassisted,
        SUM(adobe_orders_web_assisted)      AS adobe_orders_web_assisted,
        SUM(adobe_orders_app_unassisted)    AS adobe_orders_app_unassisted,
        SUM(adobe_orders_app_assisted)      AS adobe_orders_app_assisted,
        SUM(adobe_orders_web_all)           AS adobe_orders_web_all,
        SUM(adobe_orders_app_all)           AS adobe_orders_app_all,
        SUM(adobe_orders_fully_unassisted)  AS adobe_orders_fully_unassisted,
        SUM(adobe_orders_fully_assisted)    AS adobe_orders_fully_assisted,
        SUM(adobe_orders_all)               AS adobe_orders_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeOrders_daily`
    GROUP BY event_date, lob, channel
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
                'PERFORMANCE MAX'
            ) THEN 'PAID SEARCH'
            WHEN UPPER(TRIM(channel)) IN ('NATURAL SEARCH', 'ORGANIC SEARCH') THEN 'ORGANIC SEARCH'
            ELSE UPPER(TRIM(channel))
        END AS channel,
        SUM(adobe_cart_start_plus) AS adobe_cart_start_plus
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeCartStartPlus_daily`
    GROUP BY event_date, lob, channel
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
                'PERFORMANCE MAX'
            ) THEN 'PAID SEARCH'
            WHEN UPPER(TRIM(channel)) IN ('NATURAL SEARCH', 'ORGANIC SEARCH') THEN 'ORGANIC SEARCH'
            ELSE UPPER(TRIM(channel))
        END AS channel,
        SUM(adobe_storelocator_visits) AS adobe_storelocator_visits
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeStoreLocator_daily`
    GROUP BY event_date, lob, channel
),

joined AS (
    SELECT
        COALESCE(v2.event_date, ord.event_date, cp.event_date, sl.event_date) AS event_date,
        COALESCE(v2.lob, ord.lob, cp.lob, sl.lob) AS lob,
        COALESCE(v2.channel, ord.channel, cp.channel, sl.channel) AS channel,

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

        sl.adobe_storelocator_visits
    FROM adobe_v2_mapped v2
    FULL OUTER JOIN adobe_orders_mapped ord
        ON v2.event_date = ord.event_date
       AND v2.lob        = ord.lob
       AND v2.channel    = ord.channel
    FULL OUTER JOIN adobe_cartplus_mapped cp
        ON COALESCE(v2.event_date, ord.event_date) = cp.event_date
       AND COALESCE(v2.lob, ord.lob)               = cp.lob
       AND COALESCE(v2.channel, ord.channel)       = cp.channel
    FULL OUTER JOIN adobe_storelocator_mapped sl
        ON COALESCE(v2.event_date, ord.event_date, cp.event_date) = sl.event_date
       AND COALESCE(v2.lob, ord.lob, cp.lob)                      = sl.lob
       AND COALESCE(v2.channel, ord.channel, cp.channel)          = sl.channel
)

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
FROM joined;