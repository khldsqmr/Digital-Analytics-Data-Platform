/*
===============================================================================
FILE: 00_sp_profound_bronze_master_orchestration.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_profound_bronze_master_orchestration

PURPOSE:
  Daily Bronze build orchestration (incremental only) for ProFound.
  Runs Bronze merge procedures in a deterministic order.

NOTES:
  - Backfill scripts are NOT called here (those are manual/on-demand SQL scripts).
  - Each merge proc uses a lookback window (e.g., 60 days) to handle late files.
  - Order here is mostly for readability; tables are independent.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_profound_bronze_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN

  -- ---------------------------------------------------------------------------
  -- ProFound | Visibility (daily)
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_visibility_asset_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_visibility_tag_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_visibility_topic_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_visibility_topic_tag_daily`();

  -- ---------------------------------------------------------------------------
  -- ProFound | Citations (daily)
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_citations_domain_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_citations_tag_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_citations_topic_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_sdi_profound_bronze_citations_topic_tag_daily`();

END;