/* =================================================================================================
FILE: 01_vw_sdi_adobePpPulsePro_gold_orders_weekly.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobePpPulsePro_gold_orders_weekly_by_channel

SOURCE:
  vw_sdi_adobePpPulsePro_silver_orders_daily
  fn_qgp_week(date)

PURPOSE:
  Gold weekly wide view for Adobe PP Pulse Pro orders with:
  - one row per qgp_week + last_touch_channel
  - all order metrics as columns
  - reporting-ready weekly aggregation using shared QGP week logic

BUSINESS GRAIN:
  qgp_week + last_touch_channel
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_weekly`
AS

SELECT
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date) AS qgp_week,
  last_touch_channel,

  SUM(orders_web_unassisted) AS orders_web_unassisted,
  SUM(orders_web_assisted) AS orders_web_assisted,
  SUM(orders_app_unassisted) AS orders_app_unassisted,
  SUM(orders_app_assisted) AS orders_app_assisted,

  SUM(orders_web_all) AS orders_web_all,
  SUM(orders_app_all) AS orders_app_all,
  SUM(orders_fully_unassisted) AS orders_fully_unassisted,
  SUM(orders_fully_assisted) AS orders_fully_assisted,
  SUM(orders_all) AS orders_all,

  MAX(file_load_datetime) AS file_load_datetime

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_silver_orders_daily`
GROUP BY
  qgp_week,
  last_touch_channel;