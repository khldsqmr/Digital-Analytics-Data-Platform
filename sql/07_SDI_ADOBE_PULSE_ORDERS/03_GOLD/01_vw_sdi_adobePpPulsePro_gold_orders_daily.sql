/* =================================================================================================
FILE: 01_vw_sdi_adobePpPulsePro_gold_orders_daily.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobePpPulsePro_gold_orders_daily

SOURCE:
  vw_sdi_adobePpPulsePro_silver_orders_daily

PURPOSE:
  Gold daily wide view for Adobe PP Pulse Pro orders with:
  - one row per event_date + last_touch_channel
  - all order metrics as columns
  - LOB fixed as 'POSTPAID'

BUSINESS GRAIN:
  event_date + last_touch_channel
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily`
AS

SELECT
  date,
  date_yyyymmdd,
  'POSTPAID' AS LOB,
  last_touch_channel,

  orders_web_unassisted,
  orders_web_assisted,
  orders_app_unassisted,
  orders_app_assisted,

  orders_web_all,
  orders_app_all,
  orders_fully_unassisted,
  orders_fully_assisted,
  orders_all,

  file_load_datetime,
  filename_web_unassisted,
  filename_web_assisted,
  filename_app_unassisted,
  filename_app_assisted

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_silver_orders_daily`;