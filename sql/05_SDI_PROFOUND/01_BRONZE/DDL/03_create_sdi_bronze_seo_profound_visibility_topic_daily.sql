/* =================================================================================================
FILE: 03_create_sdi_bronze_seo_profound_visibility_topic_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:  sdi_bronze_seo_profound_visibility_topic_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_visibility_topic_daily_tmo

PURPOSE:
  Canonical Bronze daily table for ProFound Visibility by Topic:
    - Canonical DATE parsed from date_yyyymmdd
    - Keep raw INT64 date for lineage/debug
    - Preserve lineage fields (file_load_datetime, filename, __insert_date)
    - Dedupe per grain using latest file load

GRAIN:
  account_id + asset_name + topic + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, topic
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_seo_profound_visibility_topic_daily`
(
  account_id STRING OPTIONS(description="Account ID from raw."),
  account_name STRING OPTIONS(description="Account name from raw."),
  asset_name STRING OPTIONS(description="Asset name from raw."),
  topic STRING OPTIONS(description="Topic from raw."),

  date_yyyymmdd STRING OPTIONS(description="Raw YYYYMMDD key (lineage/debug)."),
  date DATE OPTIONS(description="Canonical DATE parsed from date_yyyymmdd (partition key)."),
  raw_date_int64 INT64 OPTIONS(description="Raw INT64 date from source (lineage/debug)."),

  executions FLOAT64 OPTIONS(description="Executions metric."),
  mentions_count FLOAT64 OPTIONS(description="Mentions count metric."),
  share_of_voice FLOAT64 OPTIONS(description="Share of voice metric."),
  visibility_score FLOAT64 OPTIONS(description="Visibility score metric."),

  insert_date INT64 OPTIONS(description="Raw __insert_date (lineage)."),
  file_load_datetime DATETIME OPTIONS(description="Raw File_Load_datetime (lineage)."),
  filename STRING OPTIONS(description="Raw Filename (lineage).")
)
PARTITION BY date
CLUSTER BY account_id, topic
OPTIONS(description="Bronze ProFound Visibility Topic Daily. Canonical date + dedupe + lineage.");

