/*
===============================================================================
FILE: 07_view_bronze_test_dashboard.sql

PURPOSE:
  Clean dashboard view for monitoring Bronze QA

FEATURES:
  - Only latest execution per test per day
  - Most recent days on top
  - Easy filtering by table, layer, severity, status

===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_bronze_sa360_test_dashboard`
AS

WITH latest_tests AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        test_date,
        table_name,
        test_layer,
        test_name
      ORDER BY test_run_timestamp DESC
    ) AS rn
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
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
FROM latest_tests
WHERE rn = 1
ORDER BY
  test_date DESC,
  table_name,
  CASE severity_level
    WHEN 'HIGH'   THEN 1
    WHEN 'MEDIUM' THEN 2
    WHEN 'LOW'    THEN 3
    ELSE 4
  END,
  CASE test_layer
    WHEN 'critical'       THEN 1
    WHEN 'reconciliation' THEN 2
    WHEN 'deep_validation' THEN 3
    ELSE 4
  END,
  test_name;
