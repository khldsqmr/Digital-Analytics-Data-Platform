/*
===============================================================================
FILE: 05_sp_gold_master_orchestration.sql
LAYER: Gold QA

PURPOSE:
  Master Gold QA controller:
    1) Run critical tests (blocking)
    2) Halt if any HIGH FAIL in critical layer
    3) Run reconciliation tests (Gold↔Silver, Weekly↔Daily recompute)

===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_master_orchestration`()
BEGIN
  DECLARE v_critical_failures INT64;

  -- Critical: Daily + Weekly
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_critical`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_critical`();

  -- Gate
  SET v_critical_failures = (
    SELECT COUNT(*)
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
    WHERE test_date = CURRENT_DATE()
      AND test_layer = 'critical'
      AND is_critical_failure = TRUE
  );

  IF v_critical_failures > 0 THEN
    RAISE USING MESSAGE = 'Critical Gold QA failures detected (HIGH severity). Pipeline halted.';
  END IF;

  -- Reconciliation
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_daily_reconciliation`();
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_gold_campaign_weekly_reconciliation`();

END;
