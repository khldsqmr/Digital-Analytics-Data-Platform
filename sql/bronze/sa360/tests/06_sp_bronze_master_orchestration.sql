/*
===============================================================================
FILE: 06_sp_bronze_master_orchestration.sql

PURPOSE:
  Master controller for Bronze QA framework.

FLOW:
  1. Run critical tests (blocking)
  2. If critical fails exist → STOP
  3. Run reconciliation tests
  4. Run weekly deep validation (optional)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_master_orchestration`()
BEGIN

-- Run Critical
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_critical`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_entity_critical`();

-- Check if critical failures exist
DECLARE critical_failures INT64;

SET critical_failures = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WHERE execution_date = CURRENT_DATE()
    AND test_category = 'critical'
    AND status = 'FAIL'
);

IF critical_failures > 0 THEN
  RAISE USING MESSAGE = 'Critical Bronze QA failures detected. Pipeline halted.';
END IF;

-- Run Reconciliation
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_reconciliation`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_entity_reconciliation`();

-- Weekly Deep Validation (optional schedule logic)
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_weekly_deep_validation`();

END;
