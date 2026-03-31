/* =================================================================================================
FILE: 03_create_sdi_adobePpPulsePro_bronze_ordersAppUnassisted_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE: sdi_adobePpPulsePro_bronze_ordersAppUnassisted_daily
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_adobePpPulsePro_bronze_ordersAppUnassisted_daily`
(
  account_id STRING,
  account_name STRING,
  last_touch_channel STRING,
  segments_id STRING,

  date DATE,
  date_yyyymmdd STRING,
  raw_date_int64 INT64,

  orders FLOAT64,

  insert_date INT64,
  file_load_datetime DATETIME,
  filename STRING
)
PARTITION BY date
CLUSTER BY account_id, last_touch_channel
OPTIONS(
  description = "Bronze Adobe PP Pulse Pro App Unassisted Orders Daily."
);