/* =================================================================================================
FILE: 23_create_vw_sdi_profound_silver_citation_weekly.sql
LAYER: Silver
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_profound_silver_citation_weekly
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_citation_weekly` AS

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  'weekly' AS time_granularity,
  'citation' AS report_type,
  'domain' AS entity_type,
  'domain' AS grain_type,
  NULLIF(root_domain, '(none)') AS root_domain,
  CAST(NULL AS STRING) AS asset_id,
  CAST(NULL AS STRING) AS asset_name,
  CAST(NULL AS STRING) AS tag,
  CAST(NULL AS STRING) AS topic,
  CAST(NULL AS FLOAT64) AS vis_executions,
  CAST(NULL AS FLOAT64) AS vis_mentions_count,
  CAST(NULL AS FLOAT64) AS vis_share_of_voice,
  CAST(NULL AS FLOAT64) AS vis_visibility_score,
  count AS cit_count,
  share_of_voice AS cit_share_of_voice
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citationDomain_weekly`

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  'weekly' AS time_granularity,
  'citation' AS report_type,
  'domain' AS entity_type,
  'domain_tag' AS grain_type,
  NULLIF(root_domain, '(none)') AS root_domain,
  CAST(NULL AS STRING) AS asset_id,
  CAST(NULL AS STRING) AS asset_name,
  NULLIF(tag, '(none)') AS tag,
  CAST(NULL AS STRING) AS topic,
  CAST(NULL AS FLOAT64) AS vis_executions,
  CAST(NULL AS FLOAT64) AS vis_mentions_count,
  CAST(NULL AS FLOAT64) AS vis_share_of_voice,
  CAST(NULL AS FLOAT64) AS vis_visibility_score,
  count AS cit_count,
  share_of_voice AS cit_share_of_voice
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citationTag_weekly`

UNION ALL

SELECT
  account_id,
  account_name,
  date,
  date_yyyymmdd,
  'weekly' AS time_granularity,
  'citation' AS report_type,
  'domain' AS entity_type,
  'domain_tag_topic' AS grain_type,
  NULLIF(root_domain, '(none)') AS root_domain,
  CAST(NULL AS STRING) AS asset_id,
  CAST(NULL AS STRING) AS asset_name,
  NULLIF(tag, '(none)') AS tag,
  NULLIF(topic, '(none)') AS topic,
  CAST(NULL AS FLOAT64) AS vis_executions,
  CAST(NULL AS FLOAT64) AS vis_mentions_count,
  CAST(NULL AS FLOAT64) AS vis_share_of_voice,
  CAST(NULL AS FLOAT64) AS vis_visibility_score,
  count AS cit_count,
  share_of_voice AS cit_share_of_voice
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citationTagTopic_weekly`;