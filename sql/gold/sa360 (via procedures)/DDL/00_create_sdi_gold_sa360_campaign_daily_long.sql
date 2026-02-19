/*
===============================================================================
FILE: 00_create_sdi_gold_sa360_campaign_daily_long.sql
LAYER: Gold
TABLE: sdi_gold_sa360_campaign_daily_long
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

SOURCE:
  Gold Daily Wide: sdi_gold_sa360_campaign_daily

PURPOSE:
  Tableau-ready LONG fact at DAILY grain:
    - One row per (account_id, campaign_id, date, metric_name)
    - metric_value contains the numeric value
    - Keeps business dimensions for slicing

GRAIN:
  (account_id, campaign_id, date, metric_name)

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY lob, account_id, campaign_id, metric_name
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_daily_long`
(
  -- Keys
  account_id STRING,
  campaign_id STRING,
  date DATE,

  -- Dimensions
  lob STRING,
  ad_platform STRING,
  account_name STRING,
  campaign_name STRING,
  campaign_type STRING,

  advertising_channel_type STRING,
  advertising_channel_sub_type STRING,
  bidding_strategy_type STRING,
  serving_status STRING,

  -- Long-format fields
  metric_name STRING,
  metric_value FLOAT64,

  -- Optional lineage
  file_load_datetime DATETIME,
  gold_long_inserted_at TIMESTAMP
)
PARTITION BY date
CLUSTER BY lob, account_id, campaign_id, metric_name
OPTIONS(description="Gold SA360 daily LONG fact for Tableau. Wide metrics unpivoted into metric_name/metric_value.");
