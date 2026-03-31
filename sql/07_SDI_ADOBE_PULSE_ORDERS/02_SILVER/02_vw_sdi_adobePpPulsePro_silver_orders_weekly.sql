/* =================================================================================================
FILE: 07_vw_sdi_adobePpPulsePro_silver_orders_weekly.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobePpPulsePro_silver_orders_weekly

SOURCE:
  vw_sdi_adobePpPulsePro_silver_orders_daily
  fn_qgp_week(date)

PURPOSE:
  Weekly Silver view for Adobe PP Pulse Pro orders using shared QGP week logic.

BUSINESS GRAIN:
  account_id + last_touch_channel + qgp_week
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_silver_orders_weekly`
AS

WITH mapped AS (
  SELECT
    account_id,
    account_name,
    last_touch_channel,
    date,
    date_yyyymmdd,

    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date) AS qgp_week,

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
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_silver_orders_daily`
)

SELECT
  account_id,
  ANY_VALUE(account_name) AS account_name,
  last_touch_channel,
  qgp_week,

  MIN(date) AS week_start_date,
  MAX(date) AS week_end_date,

  SUM(orders_web_unassisted) AS orders_web_unassisted,
  SUM(orders_web_assisted) AS orders_web_assisted,
  SUM(orders_app_unassisted) AS orders_app_unassisted,
  SUM(orders_app_assisted) AS orders_app_assisted,

  SUM(orders_web_all) AS orders_web_all,
  SUM(orders_app_all) AS orders_app_all,
  SUM(orders_fully_unassisted) AS orders_fully_unassisted,
  SUM(orders_fully_assisted) AS orders_fully_assisted,
  SUM(orders_all) AS orders_all,

  MAX(file_load_datetime) AS file_load_datetime,

  ARRAY_AGG(
    filename_web_unassisted IGNORE NULLS
    ORDER BY file_load_datetime DESC, filename_web_unassisted DESC
    LIMIT 1
  )[OFFSET(0)] AS filename_web_unassisted,

  ARRAY_AGG(
    filename_web_assisted IGNORE NULLS
    ORDER BY file_load_datetime DESC, filename_web_assisted DESC
    LIMIT 1
  )[OFFSET(0)] AS filename_web_assisted,

  ARRAY_AGG(
    filename_app_unassisted IGNORE NULLS
    ORDER BY file_load_datetime DESC, filename_app_unassisted DESC
    LIMIT 1
  )[OFFSET(0)] AS filename_app_unassisted,

  ARRAY_AGG(
    filename_app_assisted IGNORE NULLS
    ORDER BY file_load_datetime DESC, filename_app_assisted DESC
    LIMIT 1
  )[OFFSET(0)] AS filename_app_assisted

FROM mapped
GROUP BY
  account_id,
  last_touch_channel,
  qgp_week;