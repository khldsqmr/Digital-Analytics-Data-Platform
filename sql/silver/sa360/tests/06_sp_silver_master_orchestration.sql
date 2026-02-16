/*
===============================================================================
FILE: 06_sp_silver_master_orchestration.sql
LAYER: Silver QA

PURPOSE:
  Master Silver QA controller:
    1) Run critical tests
    2) Stop if any HIGH FAIL in critical layer
    3) Run reconciliation tests
    4) Run business logic tests

NOTE:
  We only halt on critical HIGH failures, not reconciliation/business_logic.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_master_orchestration`()
BEGIN

DECLARE v_critical_failures INT64 DEFAULT 0;

-- STEP 1: Run critical tests
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_critical`();

-- STEP 2: Halt if any HIGH critical FAIL exists today
SET v_critical_failures = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WHERE test_date = CURRENT_DATE()
    AND table_name = 'sdi_silver_sa360_campaign_daily'
    AND test_layer = 'critical'
    AND severity_level = 'HIGH'
    AND is_fail = TRUE
);

IF v_critical_failures > 0 THEN
  RAISE USING MESSAGE = 'Critical Silver QA failures detected for sdi_silver_sa360_campaign_daily. Pipeline halted.';
END IF;

-- STEP 3: Reconciliation (cross-table)
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_reconciliation`();

-- STEP 4: Business logic checks
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_business_logic`();

END;
