/* =================================================================================================
FILE: 32_create_vw_sdi_profound_gold_unified_long.sql
LAYER: Gold
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_profound_gold_unified_long

PURPOSE:
  Fully melted Gold reporting view for ProFound.
  - Built from vw_sdi_profound_gold_unified_wide
  - One row per metric_name / metric_value
  - Supports flexible generic charting and parameter-driven analysis

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
  metric_name
  metric_value
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_long` AS

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  time_granularity,
  report_type,
  entity_type,
  grain_type,
  root_domain,
  asset_id,
  asset_name,
  tag,
  topic,
  'vis_executions' AS metric_name,
  vis_executions AS metric_value
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_wide`
WHERE vis_executions IS NOT NULL

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  time_granularity,
  report_type,
  entity_type,
  grain_type,
  root_domain,
  asset_id,
  asset_name,
  tag,
  topic,
  'vis_mentions_count' AS metric_name,
  vis_mentions_count AS metric_value
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_wide`
WHERE vis_mentions_count IS NOT NULL

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  time_granularity,
  report_type,
  entity_type,
  grain_type,
  root_domain,
  asset_id,
  asset_name,
  tag,
  topic,
  'vis_share_of_voice' AS metric_name,
  vis_share_of_voice AS metric_value
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_wide`
WHERE vis_share_of_voice IS NOT NULL

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  time_granularity,
  report_type,
  entity_type,
  grain_type,
  root_domain,
  asset_id,
  asset_name,
  tag,
  topic,
  'vis_visibility_score' AS metric_name,
  vis_visibility_score AS metric_value
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_wide`
WHERE vis_visibility_score IS NOT NULL

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  time_granularity,
  report_type,
  entity_type,
  grain_type,
  root_domain,
  asset_id,
  asset_name,
  tag,
  topic,
  'cit_count' AS metric_name,
  cit_count AS metric_value
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_wide`
WHERE cit_count IS NOT NULL

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  time_granularity,
  report_type,
  entity_type,
  grain_type,
  root_domain,
  asset_id,
  asset_name,
  tag,
  topic,
  'cit_share_of_voice' AS metric_name,
  cit_share_of_voice AS metric_value
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_wide`
WHERE cit_share_of_voice IS NOT NULL;