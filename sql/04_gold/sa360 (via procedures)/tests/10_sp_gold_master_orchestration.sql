/*
===============================================================================
FILE: 10_sp_gold_master_orchestration.sql
LAYER: Gold | QA
PROC:  sp_gold_sa360_master_orchestration

PURPOSE (focused):
  Validate Gold data flow for ONLY:
    - cart_start
    - postpaid_pspv
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  -- Wide Daily (critical + reconcile vs Silver for focus metrics)
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_reconciliation_tests`();

  -- Wide Weekly (critical + weekly==sum(daily) for focus metrics)
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests`();

  -- Long Daily (critical + long vs wide for focus metrics)
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_reconciliation_tests`();

  -- Long Weekly (critical + long vs wide for focus metrics)
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests`();

  -- OPTIONAL: bronze reconciliation (limited metrics)
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_bronze_reconciliation_tests`();
END;


