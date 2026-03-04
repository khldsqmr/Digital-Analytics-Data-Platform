/* =================================================================================================
FILE: 08_create_sdi_bronze_seo_profound_citations_topic_tag_daily.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:  sdi_bronze_seo_profound_citations_topic_tag_daily

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_citations_topic_tag_daily_tmo

PURPOSE:
  Canonical Bronze daily table for ProFound Citations by Root Domain + Topic + Tag:
    - Canonical DATE parsed from date_yyyymmdd
    - Keep raw INT64 date for lineage/debug
    - Preserve lineage fields (file_load_datetime, filename, __insert_date)
    - Dedupe per grain using latest file load

GRAIN:
  account_id + root_domain + topic + tag + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, topic, tag
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_seo_profound_citations_topic_tag_daily`
(
  account_id STRING,
  account_name STRING,
  root_domain STRING,
  topic STRING,
  tag STRING,

  date_yyyymmdd STRING,
  date DATE,
  raw_date_int64 INT64,

  count FLOAT64,
  share_of_voice FLOAT64,

  insert_date INT64,
  file_load_datetime DATETIME,
  filename STRING
)
PARTITION BY date
CLUSTER BY account_id, topic, tag
OPTIONS(description="Bronze ProFound Citations Topic+Tag Daily. Canonical date + dedupe + lineage.");


