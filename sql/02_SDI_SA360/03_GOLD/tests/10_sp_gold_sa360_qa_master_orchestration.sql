/*
===============================================================================
FILE: 10_sp_gold_qa_master_orchestration.sql
LAYER: Gold | QA
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_qa_master_orchestration

PURPOSE:
  Daily Gold QA orchestration (focused on key metrics).
  Validates Gold data flow and structural correctness for:
    - cart_start
    - postpaid_pspv

NOTES:
  - Run AFTER sp_gold_sa360_master_orchestration
  - This procedure assumes all underlying Gold QA test procedures already exist.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_qa_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN
  -- ---------------------------------------------------------------------------
  -- Wide Daily QA (critical + reconcile vs Silver)
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_daily_reconciliation_tests`();

  -- ---------------------------------------------------------------------------
  -- Wide Weekly QA (critical + weekly == SUM(daily))
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_weekly_reconciliation_tests`();

  -- ---------------------------------------------------------------------------
  -- Long Daily QA (critical + long vs wide)
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_daily_reconciliation_tests`();

  -- ---------------------------------------------------------------------------
  -- Long Weekly QA (critical + long vs wide weekly)
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_critical_tests`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_weekly_reconciliation_tests`();

  -- ---------------------------------------------------------------------------
  -- Optional lineage check to Bronze (focused metrics only)
  -- ---------------------------------------------------------------------------
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_sa360_campaign_long_bronze_reconciliation_tests`();

END;