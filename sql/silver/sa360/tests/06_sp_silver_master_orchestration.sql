/*
===============================================================================
FILE: 06_sp_silver_master_orchestration.sql
LAYER: Silver QA

PURPOSE:
  Master Silver QA controller:
    1) Run critical tests (blocking)
    2) Halt if any HIGH FAIL in critical layer
    3) Run reconciliation tests (inter-layer)
    4) Run business logic tests (non-blocking)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_master_orchestration`()
BEGIN

DECLARE v_critical_failures INT64;

-- STEP 1: Critical
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_critical`();

-- STEP 2: Gate (halt on HIGH failures)
SET v_critical_failures = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
  WHERE test_date = CURRENT_DATE()
    AND test_layer = 'critical'
    AND is_critical_failure = TRUE
);

IF v_critical_failures > 0 THEN
  RAISE USING MESSAGE = 'Critical Silver QA failures detected (HIGH severity). Pipeline halted.';
END IF;

-- STEP 3: Reconciliation (Bronze ↔ Silver)
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_reconciliation`();

-- STEP 4: Business logic (enrichment + domain)
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_business_logic`();

END;
