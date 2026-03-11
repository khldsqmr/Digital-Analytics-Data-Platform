/* =================================================================================================
FILE: 03_create_sdi_profound_bronze_visibility_tag_topic_weekly.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE: sdi_profound_bronze_visibility_tag_topic_weekly

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_topic_weekly_tmo

PURPOSE:
  Canonical Bronze weekly table for ProFound Visibility by Asset + Tag + Topic.

BUSINESS GRAIN:
  account_id + asset_id + asset_name + tag + topic + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, asset_id
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_topic_weekly`
(
  account_id STRING,
  account_name STRING,
  asset_id STRING,
  asset_name STRING,
  tag STRING,
  topic STRING,

  date DATE,
  date_yyyymmdd STRING,
  raw_date_int64 INT64,

  executions FLOAT64,
  mentions_count FLOAT64,
  share_of_voice FLOAT64,
  visibility_score FLOAT64,

  insert_date INT64,
  file_load_datetime DATETIME,
  filename STRING
)
PARTITION BY date
CLUSTER BY account_id, asset_id
OPTIONS(
  description="Bronze ProFound Visibility Tag Topic Weekly."
);