/*
===============================================================================
FILE: 06_sp_gold_master_orchestration.sql
LAYER: Gold QA

PURPOSE:
  Master Gold QA controller:
    1) Run Gold Daily critical tests (blocking)
    2) Halt if any HIGH FAIL in Gold Daily critical layer
    3) Run Gold Daily reconciliation (Gold ↔ Silver) (blocking/high)
    4) Run Gold Weekly critical tests (blocking)
    5) Halt if any HIGH FAIL in Gold Weekly critical layer
    6) Run Gold Weekly reconciliation (Weekly ↔ recomputed from Gold Daily)
    7) Run Gold Weekly business logic tests (non-blocking by default)

RESULTS TABLE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_master_orchestration`()
BEGIN

  DECLARE v_critical_failures INT64;

  -- ============================================================
  -- STEP 1: Gold Daily Critical (blocking)
  -- ============================================================
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_critical`();

  SET v_critical_failures = (
    SELECT COUNT(*)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
    WHERE test_date = CURRENT_DATE()
      AND test_layer = 'critical'
      AND table_name = 'sdi-gold-sa360-campaign-daily'
      AND is_critical_failure = TRUE
  );

  IF v_critical_failures > 0 THEN
    RAISE USING MESSAGE = 'Critical Gold DAILY QA failures detected (HIGH severity). Pipeline halted.';
  END IF;

  -- ============================================================
  -- STEP 2: Gold Daily Reconciliation vs Silver (blocking)
  -- ============================================================
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_reconciliation`();

  SET v_critical_failures = (
    SELECT COUNT(*)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
    WHERE test_date = CURRENT_DATE()
      AND test_layer = 'reconciliation'
      AND table_name = 'sdi-gold-sa360-campaign-daily'
      AND is_critical_failure = TRUE
  );

  IF v_critical_failures > 0 THEN
    RAISE USING MESSAGE = 'Gold DAILY reconciliation failures detected (HIGH severity). Pipeline halted.';
  END IF;

  -- ============================================================
  -- STEP 3: Gold Weekly Critical (blocking)
  -- ============================================================
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_critical`();

  SET v_critical_failures = (
    SELECT COUNT(*)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
    WHERE test_date = CURRENT_DATE()
      AND test_layer = 'critical'
      AND table_name = 'sdi-gold-sa360-campaign-weekly'
      AND is_critical_failure = TRUE
  );

  IF v_critical_failures > 0 THEN
    RAISE USING MESSAGE = 'Critical Gold WEEKLY QA failures detected (HIGH severity). Pipeline halted.';
  END IF;

  -- ============================================================
  -- STEP 4: Gold Weekly Reconciliation vs recomputed from Gold Daily (blocking/high)
  -- ============================================================
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_reconciliation`();

  SET v_critical_failures = (
    SELECT COUNT(*)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
    WHERE test_date = CURRENT_DATE()
      AND test_layer = 'reconciliation'
      AND table_name = 'sdi-gold-sa360-campaign-weekly'
      AND is_critical_failure = TRUE
  );

  IF v_critical_failures > 0 THEN
    RAISE USING MESSAGE = 'Gold WEEKLY reconciliation failures detected (HIGH severity). Pipeline halted.';
  END IF;

  -- ============================================================
  -- STEP 5: Gold Weekly business logic (non-blocking by default)
  -- ============================================================
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_business_logic`();

END;
