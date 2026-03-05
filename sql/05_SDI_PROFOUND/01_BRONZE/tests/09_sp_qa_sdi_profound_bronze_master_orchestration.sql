
/*
===============================================================================
FILE: 09_sp_qa_sdi_profound_bronze_master_orchestration.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_profound_bronze_qa_master_orchestration

PURPOSE:
  Runs ProFound Bronze QA procedures for all 8 ProFound Bronze tables.

NOTES:
  - Schedule AFTER Bronze merges finish.
  - Each QA proc inserts EXACTLY 5 rows (3 critical + 2 recon).
===============================================================================
*/
CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_profound_bronze_qa_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_asset_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_tag_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_topic_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_visibility_topic_tag_daily`();

  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_citations_domain_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_citations_tag_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_citations_topic_daily`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_qa_sdi_profound_bronze_citations_topic_tag_daily`();
END;

