/*
===============================================================================
FILE: 01_view_sa360_test_summary.sql
VIEW: vw_sdi_sa360_test_summary

PURPOSE:
  Daily rollup summary for stakeholders:
    - total tests
    - pass/fail counts and rates
    - critical fail counts
    - quick next-step guidance (lists failing test names)
===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_sa360_test_summary` AS
WITH base AS (
  SELECT
    test_date,
    table_name,
    test_layer,
    severity_level,
    status,
    is_critical_failure,
    test_name,
    next_step
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_sa360_test_dashboard`
),
agg AS (
  SELECT
    test_date,
    table_name,
    test_layer,

    COUNT(1) AS total_tests,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS passed_tests,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS failed_tests,
    SAFE_DIVIDE(SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END), COUNT(1)) AS pass_rate,
    SAFE_DIVIDE(SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END), COUNT(1)) AS fail_rate,

    SUM(CASE WHEN is_critical_failure THEN 1 ELSE 0 END) AS critical_failures,

    ARRAY_AGG(
      CASE WHEN status='FAIL' THEN CONCAT(test_name, ' | next: ', next_step) END
      IGNORE NULLS
      ORDER BY test_name
      LIMIT 25
    ) AS failing_tests_next_steps
  FROM base
  GROUP BY 1,2,3
)
SELECT * FROM agg;
