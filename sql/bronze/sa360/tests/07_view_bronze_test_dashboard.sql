/*
===============================================================================
FILE: 07_view_bronze_test_dashboard.sql

PURPOSE:
  Monitoring view for Bronze QA results.

SORT:
  Latest execution on top.

===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_bronze_sa360_test_dashboard`
AS

SELECT
  execution_timestamp,
  execution_date,
  table_name,
  test_category,
  test_name,
  severity,
  status_emoji,
  status,
  expected_value,
  actual_value,
  difference,
  test_description,
  recommended_action
FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
ORDER BY
  execution_timestamp DESC;
