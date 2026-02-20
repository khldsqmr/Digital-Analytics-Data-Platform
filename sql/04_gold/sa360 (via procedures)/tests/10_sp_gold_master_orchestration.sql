/*
===============================================================================
FILE: 10_sp_gold_master_orchestration.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_master_orchestration

PURPOSE:
  One-call Gold QA run:
    - Gold Daily critical + reconcile vs Silver
    - Gold Weekly critical + reconcile vs SUM(Daily)
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  -- Existing wide tests
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_reconciliation_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests`();

  -- NEW: long tests
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_reconciliation_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests`();

  -- OPTIONAL: bronze reconciliation (limited metrics)
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_bronze_reconciliation_tests`();
END;


