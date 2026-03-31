/* =================================================================================================
FILE: 01_vw_sdi_adobePpPulsePro_silver_orders_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobePpPulsePro_silver_orders_daily

SOURCE:
  Bronze Adobe PP Pulse Pro order tables

PURPOSE:
  Canonical Silver daily Adobe PP Pulse Pro orders view in wide format.

BUSINESS GRAIN:
  account_id + last_touch_channel + date_yyyymmdd
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_silver_orders_daily`
AS

WITH web_unassisted AS (
  SELECT
    account_id,
    ANY_VALUE(account_name) AS account_name,
    date,
    date_yyyymmdd,
    last_touch_channel,
    SUM(orders) AS orders_web_unassisted,
    MAX(file_load_datetime) AS file_load_datetime_web_unassisted,
    ARRAY_AGG(
      filename
      ORDER BY file_load_datetime DESC, insert_date DESC, filename DESC
      LIMIT 1
    )[OFFSET(0)] AS filename_web_unassisted
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_adobePpPulsePro_bronze_ordersWebUnassisted_daily`
  GROUP BY account_id, date, date_yyyymmdd, last_touch_channel
),

web_assisted AS (
  SELECT
    account_id,
    ANY_VALUE(account_name) AS account_name,
    date,
    date_yyyymmdd,
    last_touch_channel,
    SUM(orders) AS orders_web_assisted,
    MAX(file_load_datetime) AS file_load_datetime_web_assisted,
    ARRAY_AGG(
      filename
      ORDER BY file_load_datetime DESC, insert_date DESC, filename DESC
      LIMIT 1
    )[OFFSET(0)] AS filename_web_assisted
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_adobePpPulsePro_bronze_ordersWebAssisted_daily`
  GROUP BY account_id, date, date_yyyymmdd, last_touch_channel
),

app_unassisted AS (
  SELECT
    account_id,
    ANY_VALUE(account_name) AS account_name,
    date,
    date_yyyymmdd,
    last_touch_channel,
    SUM(orders) AS orders_app_unassisted,
    MAX(file_load_datetime) AS file_load_datetime_app_unassisted,
    ARRAY_AGG(
      filename
      ORDER BY file_load_datetime DESC, insert_date DESC, filename DESC
      LIMIT 1
    )[OFFSET(0)] AS filename_app_unassisted
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_adobePpPulsePro_bronze_ordersAppUnassisted_daily`
  GROUP BY account_id, date, date_yyyymmdd, last_touch_channel
),

app_assisted AS (
  SELECT
    account_id,
    ANY_VALUE(account_name) AS account_name,
    date,
    date_yyyymmdd,
    last_touch_channel,
    SUM(orders) AS orders_app_assisted,
    MAX(file_load_datetime) AS file_load_datetime_app_assisted,
    ARRAY_AGG(
      filename
      ORDER BY file_load_datetime DESC, insert_date DESC, filename DESC
      LIMIT 1
    )[OFFSET(0)] AS filename_app_assisted
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_adobePpPulsePro_bronze_ordersAppAssisted_daily`
  GROUP BY account_id, date, date_yyyymmdd, last_touch_channel
),

spine AS (
  SELECT account_id, account_name, date, date_yyyymmdd, last_touch_channel FROM web_unassisted
  UNION DISTINCT
  SELECT account_id, account_name, date, date_yyyymmdd, last_touch_channel FROM web_assisted
  UNION DISTINCT
  SELECT account_id, account_name, date, date_yyyymmdd, last_touch_channel FROM app_unassisted
  UNION DISTINCT
  SELECT account_id, account_name, date, date_yyyymmdd, last_touch_channel FROM app_assisted
)

SELECT
  s.account_id,
  s.account_name,
  s.last_touch_channel,
  s.date,
  s.date_yyyymmdd,

  COALESCE(wu.orders_web_unassisted, 0) AS orders_web_unassisted,
  COALESCE(wa.orders_web_assisted, 0) AS orders_web_assisted,
  COALESCE(au.orders_app_unassisted, 0) AS orders_app_unassisted,
  COALESCE(aa.orders_app_assisted, 0) AS orders_app_assisted,

  COALESCE(wu.orders_web_unassisted, 0) + COALESCE(wa.orders_web_assisted, 0) AS orders_web_all,
  COALESCE(au.orders_app_unassisted, 0) + COALESCE(aa.orders_app_assisted, 0) AS orders_app_all,
  COALESCE(wu.orders_web_unassisted, 0) + COALESCE(au.orders_app_unassisted, 0) AS orders_fully_unassisted,
  COALESCE(wa.orders_web_assisted, 0) + COALESCE(aa.orders_app_assisted, 0) AS orders_fully_assisted,
  COALESCE(wu.orders_web_unassisted, 0)
    + COALESCE(wa.orders_web_assisted, 0)
    + COALESCE(au.orders_app_unassisted, 0)
    + COALESCE(aa.orders_app_assisted, 0) AS orders_all,

  GREATEST(
    COALESCE(wu.file_load_datetime_web_unassisted, DATETIME '1970-01-01 00:00:00'),
    COALESCE(wa.file_load_datetime_web_assisted, DATETIME '1970-01-01 00:00:00'),
    COALESCE(au.file_load_datetime_app_unassisted, DATETIME '1970-01-01 00:00:00'),
    COALESCE(aa.file_load_datetime_app_assisted, DATETIME '1970-01-01 00:00:00')
  ) AS file_load_datetime,

  wu.filename_web_unassisted,
  wa.filename_web_assisted,
  au.filename_app_unassisted,
  aa.filename_app_assisted

FROM spine s
LEFT JOIN web_unassisted wu
  USING (account_id, date, date_yyyymmdd, last_touch_channel)
LEFT JOIN web_assisted wa
  USING (account_id, date, date_yyyymmdd, last_touch_channel)
LEFT JOIN app_unassisted au
  USING (account_id, date, date_yyyymmdd, last_touch_channel)
LEFT JOIN app_assisted aa
  USING (account_id, date, date_yyyymmdd, last_touch_channel);