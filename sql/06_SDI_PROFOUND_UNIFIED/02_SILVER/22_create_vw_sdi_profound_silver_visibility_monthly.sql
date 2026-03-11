/* =================================================================================================
FILE: 22_create_vw_sdi_profound_silver_visibility_monthly.sql
LAYER: Silver
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_profound_silver_visibility_monthly
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_visibility_monthly` AS

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  'monthly' AS time_granularity,
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
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_asset_monthly`

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  'monthly' AS time_granularity,
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
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_monthly`

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  'monthly' AS time_granularity,
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
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_tag_topic_monthly`;