/*
===============================================================================
FILE: 06_view_gold_test_dashboard.sql
LAYER: Gold QA
PURPOSE:
  Dashboard view for Gold QA results.

===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_gold_sa360_test_dashboard`
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
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
WHERE test_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY);
