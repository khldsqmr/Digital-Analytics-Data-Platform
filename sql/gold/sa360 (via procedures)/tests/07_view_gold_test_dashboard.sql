/*
===============================================================================
FILE: 07_view_gold_test_dashboard.sql
VIEW: vw_sdi_gold_sa360_test_dashboard
PURPOSE:
  Latest result per (test_date, table_name, test_layer, test_name) for Gold.
  Keeps output columns EXACTLY matching sdi_gold_sa360_test_results schema.
===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gold_sa360_test_dashboard` AS
WITH base AS (
  SELECT *
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
  WHERE test_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),
latest AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY test_date, table_name, test_layer, test_name
      ORDER BY test_run_timestamp DESC
    ) AS rn
  FROM base
)
SELECT
  test_run_timestamp,
  test_date,
  table_name,
  test_layer,
  test_name,
  severity_level,
  expected_value,
  actual_value,
  variance_value,
  status,
  status_emoji,
  failure_reason,
  next_step,
  is_critical_failure,
  is_pass,
  is_fail
FROM latest
WHERE rn = 1;
