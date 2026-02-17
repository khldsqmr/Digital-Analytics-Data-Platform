/*
===============================================================================
FILE: 07_view_gold_test_dashboard.sql
LAYER: Gold QA View

PURPOSE:
  Easy dashboarding of Gold QA results.
  - Shows latest tests first
  - Supports filtering by table_name, test_layer, severity, status, date

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results

===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_gold_sa360_test_dashboard`
AS
SELECT
  test_date,
  test_run_timestamp,

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
ORDER BY
  test_date DESC,
  test_run_timestamp DESC,
  table_name,
  test_layer,
  severity_level;
