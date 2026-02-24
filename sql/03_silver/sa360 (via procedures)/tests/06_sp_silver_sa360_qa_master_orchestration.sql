/*
===============================================================================
FILE: 06_sp_silver_qa_master_orchestration.sql
LAYER: Silver | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_qa_master_orchestration

PURPOSE:
  Daily Silver QA orchestration.
  Runs essential Silver tests in a stable order.

NOTES:
  - Suggested order: critical -> reconciliation -> business logic -> deep validation
  - Keep deep validation optional if runtime is high
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_qa_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  -- ---------------------------------------------------------------------------
  -- Silver Daily QA
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_reconciliation_tests`();

  -- ---------------------------------------------------------------------------
  -- Optional business logic tests
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_campaign_daily_business_logic_tests`();

  -- ---------------------------------------------------------------------------
  -- Optional deep validation (uncomment if you want daily; otherwise run ad hoc)
  -- ---------------------------------------------------------------------------
  -- CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_sa360_weekly_deep_validation_tests`();

END;