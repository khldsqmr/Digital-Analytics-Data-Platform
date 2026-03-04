/*
===============================================================================
FILE: 01_sp_profound_bronze_master_orchestration.sql
LAYER: Bronze
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_profound_bronze_master_orchestration

PURPOSE:
  Daily Bronze build orchestration (incremental only) for ProFound datasets.
  Runs all ProFound Bronze merge procedures in a deterministic order.

NOTES:
  - Backfill scripts are NOT called here (run manually / ad-hoc).
  - These ProFound sources are independent (no FK dependencies), so ordering is
    mostly for readability and consistent logging.
  - If you later add Silver/Gold layers for ProFound, create separate master
    procedures (e.g., sp_profound_silver_master_orchestration, sp_profound_gold_master_orchestration)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_profound_bronze_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  -- ---------------------------------------------------------------------------
  -- ProFound | Visibility (Daily)
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_visibility_asset_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_visibility_tag_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_visibility_topic_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_visibility_topic_tag_daily`();

  -- ---------------------------------------------------------------------------
  -- ProFound | Citations (Daily)
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_citations_domain_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_citations_tag_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_citations_topic_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_bronze_seo_profound_citations_topic_tag_daily`();

END;