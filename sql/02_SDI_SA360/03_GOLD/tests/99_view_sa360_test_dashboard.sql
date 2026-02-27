/*
===============================================================================
FILE: 08_view_sa360_test_dashboard.sql
VIEW: vw_sdi_sa360_test_dashboard

PURPOSE:
  Combined dashboard view:
    - Detail rows: union of Bronze/Silver/Gold dashboards (latest per test per day)
    - KPI summary rows: pass/fail rate, total tests, failed tests, critical failures
      encoded as synthetic rows using SAME schema (no extra columns).

SCHEMA GUARANTEE:
  Output columns EXACTLY match your spec:
    test_run_timestamp, test_date, table_name, test_layer, test_name, severity_level,
    expected_value, actual_value, variance_value, status, status_emoji, failure_reason,
    next_step, is_critical_failure, is_pass, is_fail
===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_sa360_test_dashboard` AS
WITH detail AS (
  SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_bronze_sa360_test_dashboard`
  UNION ALL
  SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_silver_sa360_test_dashboard`
  UNION ALL
  SELECT * FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gold_sa360_test_dashboard`
),

-- Normalize a simple layer label without adding columns (we’ll use it in KPI aggregation only)
detail_with_layer AS (
  SELECT
    d.*,
    CASE
      WHEN STARTS_WITH(table_name, 'sdi_bronze_') THEN 'BRONZE'
      WHEN STARTS_WITH(table_name, 'sdi_silver_') THEN 'SILVER'
      WHEN STARTS_WITH(table_name, 'sdi_gold_') THEN 'GOLD'
      ELSE 'UNKNOWN'
    END AS pipeline_layer
  FROM detail d
),

-- KPI aggregation per (test_date, pipeline_layer)
kpi_by_layer AS (
  SELECT
    test_date,
    pipeline_layer,
    MAX(test_run_timestamp) AS max_run_ts,
    COUNT(1) AS total_tests,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS pass_tests,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS fail_tests,
    SUM(CASE WHEN is_critical_failure = TRUE THEN 1 ELSE 0 END) AS critical_failures
  FROM detail_with_layer
  WHERE pipeline_layer IN ('BRONZE','SILVER','GOLD')
  GROUP BY 1,2
),

-- KPI aggregation overall (ALL layers) per test_date
kpi_overall AS (
  SELECT
    test_date,
    'ALL' AS pipeline_layer,
    MAX(test_run_timestamp) AS max_run_ts,
    COUNT(1) AS total_tests,
    SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) AS pass_tests,
    SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) AS fail_tests,
    SUM(CASE WHEN is_critical_failure = TRUE THEN 1 ELSE 0 END) AS critical_failures
  FROM detail_with_layer
  GROUP BY 1
),

kpi_union AS (
  SELECT * FROM kpi_by_layer
  UNION ALL
  SELECT * FROM kpi_overall
),

-- Convert KPIs into synthetic rows but keep the SAME schema
kpi_rows AS (
  SELECT
    max_run_ts AS test_run_timestamp,
    test_date,

    -- table_name: use a reserved namespace so it’s obvious in Tableau
    CONCAT('__SUMMARY__|', pipeline_layer) AS table_name,

    -- test_layer: "summary" still fits your allowed taxonomy and keeps schema
    'summary' AS test_layer,

    -- One KPI per row (so dashboards can filter/visualize easily)
    kpi_name AS test_name,

    -- severity: LOW for informational, HIGH if critical failures exist
    CASE
      WHEN critical_failures > 0 THEN 'HIGH'
      ELSE 'LOW'
    END AS severity_level,

    -- expected_value / actual_value / variance_value used to store KPI values consistently
    expected_value,
    actual_value,
    variance_value,

    -- status shows overall health for that KPI row
    status,
    status_emoji,

    failure_reason,
    next_step,

    -- is_critical_failure reflects whether there are any critical failures in that layer/date
    IF(critical_failures > 0, TRUE, FALSE) AS is_critical_failure,

    -- booleans consistent with status
    IF(status = 'PASS', TRUE, FALSE) AS is_pass,
    IF(status = 'FAIL', TRUE, FALSE) AS is_fail

  FROM (
    SELECT
      test_date, pipeline_layer, max_run_ts, total_tests, pass_tests, fail_tests, critical_failures,

      'KPI | Pass Rate' AS kpi_name,
      1.0 AS expected_value,
      SAFE_DIVIDE(CAST(pass_tests AS FLOAT64), NULLIF(CAST(total_tests AS FLOAT64), 0.0)) AS actual_value,
      SAFE_DIVIDE(CAST(pass_tests AS FLOAT64), NULLIF(CAST(total_tests AS FLOAT64), 0.0)) - 1.0 AS variance_value,
      IF(critical_failures = 0 AND fail_tests = 0, 'PASS', 'FAIL') AS status,
      IF(critical_failures = 0 AND fail_tests = 0, '🟢', '🔴') AS status_emoji,
      CONCAT(
        'Total=', CAST(total_tests AS STRING),
        ' | Pass=', CAST(pass_tests AS STRING),
        ' | Fail=', CAST(fail_tests AS STRING),
        ' | CriticalFail=', CAST(critical_failures AS STRING)
      ) AS failure_reason,
      IF(critical_failures = 0 AND fail_tests = 0,
        'No action required.',
        'Investigate FAIL rows first; address critical failures before reconciliation mismatches.'
      ) AS next_step
    FROM kpi_union

    UNION ALL

    SELECT
      test_date, pipeline_layer, max_run_ts, total_tests, pass_tests, fail_tests, critical_failures,

      'KPI | Fail Rate' AS kpi_name,
      0.0 AS expected_value,
      SAFE_DIVIDE(CAST(fail_tests AS FLOAT64), NULLIF(CAST(total_tests AS FLOAT64), 0.0)) AS actual_value,
      SAFE_DIVIDE(CAST(fail_tests AS FLOAT64), NULLIF(CAST(total_tests AS FLOAT64), 0.0)) - 0.0 AS variance_value,
      IF(fail_tests = 0, 'PASS', 'FAIL') AS status,
      IF(fail_tests = 0, '🟢', '🔴') AS status_emoji,
      CONCAT('Fail rate based on latest-per-test results for the day. FailTests=', CAST(fail_tests AS STRING)) AS failure_reason,
      IF(fail_tests = 0, 'No action required.', 'Open FAIL tests; prioritize HIGH severity and reconciliation gaps.') AS next_step
    FROM kpi_union

    UNION ALL

    SELECT
      test_date, pipeline_layer, max_run_ts, total_tests, pass_tests, fail_tests, critical_failures,

      'KPI | Total Tests' AS kpi_name,
      NULL AS expected_value,
      CAST(total_tests AS FLOAT64) AS actual_value,
      NULL AS variance_value,
      'PASS' AS status,
      '📊' AS status_emoji,
      'Count of dashboard tests (latest-per-test-per-day) included in this KPI group.' AS failure_reason,
      'No action required.' AS next_step
    FROM kpi_union

    UNION ALL

    SELECT
      test_date, pipeline_layer, max_run_ts, total_tests, pass_tests, fail_tests, critical_failures,

      'KPI | Failed Tests' AS kpi_name,
      0.0 AS expected_value,
      CAST(fail_tests AS FLOAT64) AS actual_value,
      CAST(fail_tests AS FLOAT64) - 0.0 AS variance_value,
      IF(fail_tests = 0, 'PASS', 'FAIL') AS status,
      IF(fail_tests = 0, '🟢', '🔴') AS status_emoji,
      CONCAT('Number of failing tests in this KPI group: ', CAST(fail_tests AS STRING)) AS failure_reason,
      IF(fail_tests = 0, 'No action required.', 'Drill into FAIL rows; check failure_reason + next_step.') AS next_step
    FROM kpi_union

    UNION ALL

    SELECT
      test_date, pipeline_layer, max_run_ts, total_tests, pass_tests, fail_tests, critical_failures,

      'KPI | Critical Failures' AS kpi_name,
      0.0 AS expected_value,
      CAST(critical_failures AS FLOAT64) AS actual_value,
      CAST(critical_failures AS FLOAT64) - 0.0 AS variance_value,
      IF(critical_failures = 0, 'PASS', 'FAIL') AS status,
      IF(critical_failures = 0, '🟢', '🔴') AS status_emoji,
      CONCAT('Critical failures (HIGH severity FAIL) count: ', CAST(critical_failures AS STRING)) AS failure_reason,
      IF(critical_failures = 0,
        'No action required.',
        'Stop the pipeline promotion; fix critical issues first (duplicates/null keys/staleness/recon mismatches).'
      ) AS next_step
    FROM kpi_union
  )
)

-- Final output: KPI rows + detail rows (same schema)
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
FROM kpi_rows

UNION ALL

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
FROM detail;
