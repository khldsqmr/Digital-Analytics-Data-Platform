/*
===============================================================================
FILE: 00_create_sdi_bronze_sa360_test_results.sql

PURPOSE:
  Centralized Bronze QA test results table.

DESIGN:
  - Partitioned by test_date
  - Clustered for fast filtering
  - All procedures insert exactly ONE row per test (PASS or FAIL)

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
(
  -- Execution Metadata
  test_run_timestamp TIMESTAMP OPTIONS(description='Exact timestamp when test executed'),
  test_date DATE OPTIONS(description='Logical execution date (CURRENT_DATE())'),

  -- Context
  table_name STRING OPTIONS(description='Table being validated'),
  test_layer STRING OPTIONS(description='critical | reconciliation | deep_validation'),
  test_name STRING OPTIONS(description='Human-readable test name'),
  severity_level STRING OPTIONS(description='HIGH | MEDIUM | LOW'),

  -- Metrics
  expected_value FLOAT64 OPTIONS(description='Expected numeric threshold'),
  actual_value FLOAT64 OPTIONS(description='Actual computed metric'),
  variance_value FLOAT64 OPTIONS(description='Actual minus expected'),

  -- Result
  status STRING OPTIONS(description='PASS | FAIL'),
  status_emoji STRING OPTIONS(description='🟢 PASS | 🔴 FAIL'),

  -- Explanation
  failure_reason STRING OPTIONS(description='Explanation of test result'),
  next_step STRING OPTIONS(description='Recommended action'),

  -- Flags (for orchestration)
  is_critical_failure BOOL OPTIONS(description='TRUE if HIGH severity and FAIL'),
  is_pass BOOL OPTIONS(description='TRUE if PASS'),
  is_fail BOOL OPTIONS(description='TRUE if FAIL')
)
PARTITION BY test_date
CLUSTER BY table_name, test_layer, severity_level, status;
