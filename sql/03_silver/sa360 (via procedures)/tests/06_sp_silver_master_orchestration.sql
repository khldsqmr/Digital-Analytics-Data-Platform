/*
===============================================================================
FILE: 06_sp_silver_master_orchestration.sql
LAYER: Silver | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_master_orchestration
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_reconciliation_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_business_logic_tests`();
END;
