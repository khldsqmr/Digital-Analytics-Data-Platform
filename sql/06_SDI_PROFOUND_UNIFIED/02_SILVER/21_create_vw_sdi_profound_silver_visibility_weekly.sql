/* =================================================================================================
FILE: 21_create_vw_sdi_profound_silver_visibility_weekly.sql
LAYER: Silver
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_profound_silver_visibility_weekly

PURPOSE:
  Canonical Silver weekly visibility view that unions asset, tag, and tag-topic Bronze tables.
  - Removes lineage columns
  - Standardizes '(none)' to NULL for analytical dimensions
  - Adds explicit metadata columns for reporting

OUTPUT COLUMNS:
  account_id
  account_name
  date
  date_yyyymmdd
  time_granularity
  report_type
  entity_type
  grain_type
  root_domain
  asset_id
  asset_name
  tag
  topic
  vis_executions
  vis_mentions_count
  vis_share_of_voice
  vis_visibility_score
  cit_count
  cit_share_of_voice
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_visibility_weekly` AS

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  'weekly' AS time_granularity,
  'visibility' AS report_type,
  'asset' AS entity_type,
  'asset' AS grain_type,
  CAST(NULL AS STRING) AS root_domain,
  NULLIF(asset_id, '(none)') AS asset_id,
  NULLIF(asset_name, '(none)') AS asset_name,
  CAST(NULL AS STRING) AS tag,
  CAST(NULL AS STRING) AS topic,
  executions AS vis_executions,
  mentions_count AS vis_mentions_count,
  share_of_voice AS vis_share_of_voice,
  visibility_score AS vis_visibility_score,
  CAST(NULL AS FLOAT64) AS cit_count,
  CAST(NULL AS FLOAT64) AS cit_share_of_voice
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_weekly`

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  'weekly' AS time_granularity,
  'visibility' AS report_type,
  'asset' AS entity_type,
  'asset_tag' AS grain_type,
  CAST(NULL AS STRING) AS root_domain,
  NULLIF(asset_id, '(none)') AS asset_id,
  NULLIF(asset_name, '(none)') AS asset_name,
  NULLIF(tag, '(none)') AS tag,
  CAST(NULL AS STRING) AS topic,
  executions AS vis_executions,
  mentions_count AS vis_mentions_count,
  share_of_voice AS vis_share_of_voice,
  visibility_score AS vis_visibility_score,
  CAST(NULL AS FLOAT64) AS cit_count,
  CAST(NULL AS FLOAT64) AS cit_share_of_voice
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_weekly`

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  'weekly' AS time_granularity,
  'visibility' AS report_type,
  'asset' AS entity_type,
  'asset_tag_topic' AS grain_type,
  CAST(NULL AS STRING) AS root_domain,
  NULLIF(asset_id, '(none)') AS asset_id,
  NULLIF(asset_name, '(none)') AS asset_name,
  NULLIF(tag, '(none)') AS tag,
  NULLIF(topic, '(none)') AS topic,
  executions AS vis_executions,
  mentions_count AS vis_mentions_count,
  share_of_voice AS vis_share_of_voice,
  visibility_score AS vis_visibility_score,
  CAST(NULL AS FLOAT64) AS cit_count,
  CAST(NULL AS FLOAT64) AS cit_share_of_voice
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_topic_weekly`;