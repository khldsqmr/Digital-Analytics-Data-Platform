/* =================================================================================================
FILE: 01_create_sdi_profound_bronze_visibilityAsset_weekly.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE: sdi_profound_bronze_visibilityAsset_weekly

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_asset_weekly_tmo

PURPOSE:
  Canonical Bronze weekly table for ProFound Visibility by Asset.
  - Parse DATE from date_yyyymmdd
  - Preserve raw source lineage fields needed for audits and refresh logic
  - Serve as the durable latest Bronze snapshot for weekly visibility asset data

BUSINESS GRAIN:
  account_id + asset_id + asset_name + date_yyyymmdd

REFRESH STRATEGY:
  Date-scoped delete + insert for affected raw dates.
  This ensures Bronze reflects:
    - updates
    - inserts
    - deletions from refreshed raw snapshots
    - historical backfills

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, asset_id
================================================================================================= */

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibilityAsset_weekly`
(
  account_id STRING OPTIONS(description="Raw account identifier."),
  account_name STRING OPTIONS(description="Raw account name."),
  asset_id STRING OPTIONS(description="Raw asset identifier."),
  asset_name STRING OPTIONS(description="Raw asset name."),

  date DATE OPTIONS(description="Canonical DATE parsed from date_yyyymmdd."),
  date_yyyymmdd STRING OPTIONS(description="Raw YYYYMMDD business date key."),
  raw_date_int64 INT64 OPTIONS(description="Raw source INT64 date column kept for lineage/debug."),

  executions FLOAT64 OPTIONS(description="Visibility executions metric."),
  mentions_count FLOAT64 OPTIONS(description="Visibility mentions count metric."),
  share_of_voice FLOAT64 OPTIONS(description="Visibility share of voice metric."),
  visibility_score FLOAT64 OPTIONS(description="Visibility score metric."),

  insert_date INT64 OPTIONS(description="Raw __insert_date lineage field."),
  file_load_datetime DATETIME OPTIONS(description="Raw File_Load_datetime lineage field."),
  filename STRING OPTIONS(description="Raw Filename lineage field.")
)
PARTITION BY date
CLUSTER BY account_id, asset_id
OPTIONS(
  description="Bronze ProFound Visibility Asset Weekly."
);