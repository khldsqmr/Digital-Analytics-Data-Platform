/*
===============================================================================
FILE: 05_sp_gold_master_orchestration.sql
LAYER: Gold QA

PURPOSE:
  Master Gold QA controller:
    1) Run Gold Daily critical tests (blocking)
    2) Run Gold Daily reconciliation vs Silver (blocking)
    3) Run Gold Weekly critical tests (blocking)
    4) Run Gold Weekly reconciliation vs recomputed-from-Gold-Daily (blocking)
    5) Halt if any HIGH FAIL marked critical

NOTE:
  The procedures themselves write into:
    prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_master_orchestration`()
OPTIONS(strict_mode=false)
BEGIN

  DECLARE v_critical_failures INT64;

  -- STEP 1: Gold Daily critical
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_critical`();

  -- STEP 2: Gold Daily reconciliation vs Silver
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_reconciliation`();

  -- STEP 3: Gold Weekly critical
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_critical`();

  -- STEP 4: Gold Weekly reconciliation vs recomputed from Gold Daily
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_reconciliation`();

  -- STEP 5: Gate (halt on HIGH failures flagged critical)
  SET v_critical_failures = (
    SELECT COUNT(*)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
    WHERE test_date = CURRENT_DATE()
      AND severity_level = 'HIGH'
      AND status = 'FAIL'
      AND is_critical_failure = TRUE
  );

  IF v_critical_failures > 0 THEN
    RAISE USING MESSAGE = 'Critical Gold QA failures detected (HIGH severity). Pipeline halted.';
  END IF;

END;
