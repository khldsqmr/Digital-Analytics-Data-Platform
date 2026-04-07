/* =================================================================================================
FILE: 02_vw_sdi_tsd_bronze_adobeOrders_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_adobeOrders_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeOrders_daily

PURPOSE:
  Canonical Bronze Adobe Orders daily view for the Total Search Dashboard.
  This view standardizes Adobe digital order metrics to the reporting grain:
      event_date + lob + channel

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

METRICS INCLUDED:
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
  - Source already contains cleaned Adobe Orders logic
  - LOB is standardized to 'Postpaid'
  - Channel is standardized from last_touch_channel

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeOrders_daily`
AS

SELECT
    DATE(event_date) AS event_date,
    'Postpaid' AS lob,
    UPPER(TRIM(last_touch_channel)) AS channel,

    SUM(COALESCE(orders_web_unassisted, 0))   AS adobe_orders_web_unassisted,
    SUM(COALESCE(orders_web_assisted, 0))     AS adobe_orders_web_assisted,
    SUM(COALESCE(orders_app_unassisted, 0))   AS adobe_orders_app_unassisted,
    SUM(COALESCE(orders_app_assisted, 0))     AS adobe_orders_app_assisted,
    SUM(COALESCE(orders_web_all, 0))          AS adobe_orders_web_all,
    SUM(COALESCE(orders_app_all, 0))          AS adobe_orders_app_all,
    SUM(COALESCE(orders_fully_unassisted, 0)) AS adobe_orders_fully_unassisted,
    SUM(COALESCE(orders_fully_assisted, 0))   AS adobe_orders_fully_assisted,
    SUM(COALESCE(orders_all, 0))              AS adobe_orders_all

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily`
WHERE last_touch_channel IS NOT NULL
GROUP BY
    event_date,
    lob,
    channel;