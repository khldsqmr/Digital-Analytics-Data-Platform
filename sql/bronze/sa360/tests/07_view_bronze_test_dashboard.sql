/*
===============================================================================
FILE: 07_view_bronze_test_dashboard.sql

PURPOSE:
  Clean dashboard view for monitoring Bronze QA.

  • Most recent tests on top
  • Structured display
  • Easy filtering by table, layer, severity
===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_bronze_sa360_test_dashboard`
AS
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
FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
ORDER BY
    test_date DESC,
    table_name,
    CASE severity_level
        WHEN 'HIGH'   THEN 1
        WHEN 'MEDIUM' THEN 2
        WHEN 'LOW'    THEN 3
        ELSE 4
    END,
    test_run_timestamp DESC;;
