/*
===============================================================================
FILE: 00_create_sdi_gold_sa360_test_results.sql
LAYER: Gold QA
PURPOSE:
  Centralized Gold QA results table (same pattern as Bronze/Silver).

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
(
  test_run_timestamp TIMESTAMP OPTIONS(description="Exact timestamp when test executed"),
  test_date DATE OPTIONS(description="Logical execution date (CURRENT_DATE())"),

  table_name STRING OPTIONS(description="Table being validated"),
  test_layer STRING OPTIONS(description="critical | reconciliation | business_logic"),
  test_name STRING OPTIONS(description="Human-readable test name"),
  severity_level STRING OPTIONS(description="HIGH | MEDIUM | LOW"),

  expected_value FLOAT64 OPTIONS(description="Expected numeric value"),
  actual_value FLOAT64 OPTIONS(description="Actual numeric value"),
  variance_value FLOAT64 OPTIONS(description="Actual - Expected"),

  status STRING OPTIONS(description="PASS | FAIL"),
  status_emoji STRING OPTIONS(description="🟢 | 🔴"),

  failure_reason STRING OPTIONS(description="Why the test failed (if failed)"),
  next_step STRING OPTIONS(description="Suggested next step"),

  is_critical_failure BOOL OPTIONS(description="TRUE if HIGH + FAIL in critical layer"),
  is_pass BOOL OPTIONS(description="TRUE if PASS"),
  is_fail BOOL OPTIONS(description="TRUE if FAIL")
)
PARTITION BY test_date
CLUSTER BY table_name, test_layer, severity_level, status
OPTIONS(description="Gold QA results log table.");
