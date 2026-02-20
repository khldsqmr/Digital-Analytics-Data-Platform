/*
===============================================================================
FILE: 06_sp_silver_master_orchestration.sql
LAYER: Silver QA

PURPOSE:
  Master Silver QA controller:
    1) Run critical tests (blocking)
    2) Halt if the LATEST result for any critical HIGH test is FAIL
    3) Run reconciliation tests (inter-layer)
    4) Run business logic tests (non-blocking)

WHY THIS VERSION:
  Prevents false halts when you rerun tests in the same day:
  - Old FAIL rows remain in results table for audit
  - Gate checks only the latest row per test_name
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_master_orchestration`()
BEGIN

  DECLARE v_critical_failures INT64 DEFAULT 0;

  -- STEP 1: Critical
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_critical`();

  -- STEP 2: Gate (halt on LATEST HIGH critical failures only)
  SET v_critical_failures = (
    WITH latest_critical AS (
      SELECT
        test_date,
        table_name,
        test_layer,
        test_name,
        severity_level,
        is_fail,
        ROW_NUMBER() OVER (
          PARTITION BY test_date, table_name, test_layer, test_name
          ORDER BY test_run_timestamp DESC
        ) AS rn
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
      WHERE test_date = CURRENT_DATE()
        AND test_layer = 'critical'
        AND severity_level = 'HIGH'
        -- Optional but recommended to avoid cross-table gating if you add more tables later:
        AND table_name = 'sdi_silver_sa360_campaign_daily'
    )
    SELECT COUNT(*)
    FROM latest_critical
    WHERE rn = 1
      AND is_fail = TRUE
  );

  IF v_critical_failures > 0 THEN
    RAISE USING MESSAGE = 'Critical Silver QA failures detected (latest run HIGH severity). Pipeline halted.';
  END IF;

  -- STEP 3: Reconciliation (Bronze ↔ Silver)
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_reconciliation`();

  -- STEP 4: Business logic (enrichment + domain)
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_silver_campaign_daily_business_logic`();

END;
