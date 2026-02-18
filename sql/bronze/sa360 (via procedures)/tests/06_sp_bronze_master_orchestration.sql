/*
===============================================================================
FILE: 06_sp_bronze_master_orchestration.sql
LAYER: Bronze | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_master_orchestration

PURPOSE:
  One-call Bronze QA run:
    1) Bronze Daily critical
    2) Bronze Daily reconciliation vs RAW
    3) Bronze Entity critical
    4) Bronze Entity reconciliation vs RAW
    5) Bronze deep validation (weekly anomaly checks)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_daily_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_daily_reconciliation_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_campaign_entity_reconciliation_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_sa360_weekly_deep_validation_tests`();
END;
