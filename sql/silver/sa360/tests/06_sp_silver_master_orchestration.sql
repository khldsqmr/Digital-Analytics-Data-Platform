/*
===============================================================================
FILE: 06_sp_silver_master_orchestration.sql

PURPOSE:
  Master Silver QA controller.

FLOW:
  1. Run critical tests
  2. Stop if any HIGH FAIL
  3. Run reconciliation tests
  4. Run business logic tests
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_master_orchestration`()
BEGIN

DECLARE v_critical_failures INT64;

-- STEP 1: Run Critical Tests
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_critical`();

-- STEP 2: Check for Critical Failures
SET v_critical_failures = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WHERE test_date = CURRENT_DATE()
    AND test_layer = 'critical'
    AND is_fail = TRUE
);

IF v_critical_failures > 0 THEN
  RAISE USING MESSAGE = 'Critical Silver QA failures detected. Pipeline halted.';
END IF;

-- STEP 3: Reconciliation
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_reconciliation`();

-- STEP 4: Business Logic
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_business_logic`();

END;
