/*===============================================================================
FILE: 09_sp_qa_sdi_profound_bronze_master_orchestration.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROC:  sp_profound_bronze_qa_master_orchestration

PURPOSE:
  Runs daily QA procedures for all 8 ProFound Bronze tables.

DESIGN:
  - No input parameters required.
  - Each QA procedure computes its own automatic validation scope:
      latest complete week
      same aligned week last year
  - Intended to run AFTER Bronze merge/backfill orchestration completes.

EXPECTED OUTPUT:
  - 8 procedures
  - 5 result rows per procedure
  - 40 result rows per orchestration run
===============================================================================*/

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