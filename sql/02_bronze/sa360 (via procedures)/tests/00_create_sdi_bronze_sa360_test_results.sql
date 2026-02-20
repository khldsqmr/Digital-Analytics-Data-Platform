/*
===============================================================================
FILE: 00_create_sdi_bronze_sa360_test_results.sql
LAYER: Bronze | QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:   sdi_bronze_sa360_test_results

PURPOSE:
  Centralized Bronze QA test results table for SA360.
  - Every test inserts exactly ONE row per execution.
  - PASS and FAIL are both logged.

GRAIN:
  One row per (test_run_timestamp, test_name, table_name).
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
(
  test_run_timestamp TIMESTAMP OPTIONS(description="Exact timestamp when test executed."),
  test_date DATE OPTIONS(description="Logical execution date (CURRENT_DATE())."),

  table_name STRING OPTIONS(description="Table being validated."),
  test_layer STRING OPTIONS(description="critical | reconciliation | deep_validation"),
  test_name STRING OPTIONS(description="Human-readable test name."),
  severity_level STRING OPTIONS(description="HIGH | MEDIUM | LOW"),

  expected_value FLOAT64 OPTIONS(description="Expected numeric value."),
  actual_value FLOAT64 OPTIONS(description="Observed numeric value."),
  variance_value FLOAT64 OPTIONS(description="actual_value - expected_value"),

  status STRING OPTIONS(description="PASS | FAIL"),
  status_emoji STRING OPTIONS(description="🟢 for PASS, 🔴 for FAIL"),

  failure_reason STRING OPTIONS(description="Why the test failed (or PASS reason)."),
  next_step STRING OPTIONS(description="Action to take if failed."),

  is_critical_failure BOOL OPTIONS(description="TRUE when severity HIGH and FAIL."),
  is_pass BOOL OPTIONS(description="TRUE when PASS."),
  is_fail BOOL OPTIONS(description="TRUE when FAIL.")
)
PARTITION BY test_date
CLUSTER BY table_name, test_layer, severity_level
OPTIONS(description="Bronze SA360 centralized QA test results table.");
