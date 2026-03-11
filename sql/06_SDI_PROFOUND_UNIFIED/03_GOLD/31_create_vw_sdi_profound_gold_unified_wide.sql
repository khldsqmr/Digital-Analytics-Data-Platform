/* =================================================================================================
FILE: 31_create_vw_sdi_profound_gold_unified_wide.sql
LAYER: Gold
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_profound_gold_unified_wide

PURPOSE:
  Unified long-format Gold reporting view for ProFound.
  - Combines weekly/monthly citation and visibility Silver views
  - Keeps separate metric columns
  - Clean business-facing schema only

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
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_wide` AS

SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_visibility_weekly`
UNION ALL
SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_visibility_monthly`
UNION ALL
SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_citation_weekly`
UNION ALL
SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_silver_citation_monthly`;