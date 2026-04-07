/* =================================================================================================
FILE: 01_vw_sdi_tsd_silver_adobe_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_silver_adobe_daily

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeV2_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeOrders_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeCartStartPlus_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily

PURPOSE:
  Unified Silver Adobe daily source mart for the Total Search Dashboard.
  This view combines:
      - Adobe V2 funnel metrics
      - Adobe digital order metrics
      - Adobe Cart Start Plus
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

KEY MODELING NOTES:
  - LOB is already standardized in all Bronze sources as UPPER(TRIM(...))
  - Uses FULL OUTER JOIN to preserve valid keys from any Adobe Bronze source
  - Common business keys are coalesced across all source marts
  - Null metrics are standardized to 0 for reporting convenience

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_silver_adobe_daily`
AS

WITH adobe_v2 AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        adobe_entries,
        adobe_pspv_actuals,
        adobe_cart_starts,
        adobe_cart_checkout_visits,
        adobe_checkout_review_visits,
        adobe_postpaid_orders_tsr
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeV2_daily`
),

adobe_orders AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        adobe_orders_web_unassisted,
        adobe_orders_web_assisted,
        adobe_orders_app_unassisted,
        adobe_orders_app_assisted,
        adobe_orders_web_all,
        adobe_orders_app_all,
        adobe_orders_fully_unassisted,
        adobe_orders_fully_assisted,
        adobe_orders_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeOrders_daily`
),

adobe_cartplus AS (
    SELECT
        event_date,
        UPPER(TRIM(lob)) AS lob,
        UPPER(TRIM(channel)) AS channel,
        adobe_cart_start_plus
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeCartStartPlus_daily`
),

joined AS (
    SELECT
        COALESCE(v2.event_date, ord.event_date, cp.event_date) AS event_date,
        UPPER(TRIM(COALESCE(v2.lob, ord.lob, cp.lob))) AS lob,
        UPPER(TRIM(COALESCE(v2.channel, ord.channel, cp.channel))) AS channel,

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
        ord.adobe_orders_all

    FROM adobe_v2 v2
    FULL OUTER JOIN adobe_orders ord
        ON v2.event_date = ord.event_date
       AND v2.lob        = ord.lob
       AND v2.channel    = ord.channel
    FULL OUTER JOIN adobe_cartplus cp
        ON COALESCE(v2.event_date, ord.event_date) = cp.event_date
       AND COALESCE(v2.lob, ord.lob)               = cp.lob
       AND COALESCE(v2.channel, ord.channel)       = cp.channel
)

SELECT
    event_date,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel)) AS channel,

    COALESCE(adobe_entries, 0)                    AS adobe_entries,
    COALESCE(adobe_pspv_actuals, 0)              AS adobe_pspv_actuals,
    COALESCE(adobe_cart_starts, 0)               AS adobe_cart_starts,
    COALESCE(adobe_cart_start_plus, 0)           AS adobe_cart_start_plus,
    COALESCE(adobe_cart_checkout_visits, 0)      AS adobe_cart_checkout_visits,
    COALESCE(adobe_checkout_review_visits, 0)    AS adobe_checkout_review_visits,
    COALESCE(adobe_postpaid_orders_tsr, 0)       AS adobe_postpaid_orders_tsr,

    COALESCE(adobe_orders_web_unassisted, 0)     AS adobe_orders_web_unassisted,
    COALESCE(adobe_orders_web_assisted, 0)       AS adobe_orders_web_assisted,
    COALESCE(adobe_orders_app_unassisted, 0)     AS adobe_orders_app_unassisted,
    COALESCE(adobe_orders_app_assisted, 0)       AS adobe_orders_app_assisted,
    COALESCE(adobe_orders_web_all, 0)            AS adobe_orders_web_all,
    COALESCE(adobe_orders_app_all, 0)            AS adobe_orders_app_all,
    COALESCE(adobe_orders_fully_unassisted, 0)   AS adobe_orders_fully_unassisted,
    COALESCE(adobe_orders_fully_assisted, 0)     AS adobe_orders_fully_assisted,
    COALESCE(adobe_orders_all, 0)                AS adobe_orders_all
FROM joined;