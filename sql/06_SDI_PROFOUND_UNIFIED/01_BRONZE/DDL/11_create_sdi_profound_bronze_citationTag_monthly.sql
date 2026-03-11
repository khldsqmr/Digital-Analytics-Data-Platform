/* =================================================================================================
FILE: 11_create_sdi_profound_bronze_citation_tag_monthly.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE: sdi_profound_bronze_citation_tag_monthly

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_monthly_tmo

PURPOSE:
  Canonical Bronze monthly table for ProFound Citation by Domain + Tag.

BUSINESS GRAIN:
  account_id + root_domain + tag + date_yyyymmdd

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, root_domain
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citation_tag_monthly`
(
  account_id STRING,
  account_name STRING,
  root_domain STRING,
  tag STRING,

  date DATE,
  date_yyyymmdd STRING,
  raw_date_int64 INT64,

  count FLOAT64,
  share_of_voice FLOAT64,

  insert_date INT64,
  file_load_datetime DATETIME,
  filename STRING
)
PARTITION BY date
CLUSTER BY account_id, root_domain
OPTIONS(
  description="Bronze ProFound Citation Tag Monthly."
);