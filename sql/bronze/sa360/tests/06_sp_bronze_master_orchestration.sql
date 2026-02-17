/*
===============================================================================
FILE: 06_sp_bronze_master_orchestration.sql

PURPOSE:
  Master QA Controller for Bronze SA360

FLOW:
  1. Run critical tests
  2. Stop if any critical FAIL (HIGH severity)
  3. Run reconciliation tests
  4. Run deep validation (non-blocking, can be scheduled weekly)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_master_orchestration`()
BEGIN

DECLARE v_critical_failures INT64;

-- =====================================================
-- STEP 1: Run Critical Tests (blocking)
-- =====================================================
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_critical`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_entity_critical`();

-- =====================================================
-- STEP 2: Halt if any critical failures detected
-- =====================================================
SET v_critical_failures = (
  SELECT COUNT(*)
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
  WHERE test_date = CURRENT_DATE()
    AND test_layer = 'critical'
    AND is_critical_failure = TRUE
);

IF v_critical_failures > 0 THEN
  RAISE USING MESSAGE = 'Critical Bronze QA failures detected (HIGH severity). Pipeline halted.';
END IF;

-- =====================================================
-- STEP 3: Run Reconciliation Tests (non-blocking but important)
-- =====================================================
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_daily_reconciliation`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_campaign_entity_reconciliation`();

-- =====================================================
-- STEP 4: Run Deep Validation (non-blocking heuristics)
-- =====================================================
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_bronze_weekly_deep_validation`();

END;
