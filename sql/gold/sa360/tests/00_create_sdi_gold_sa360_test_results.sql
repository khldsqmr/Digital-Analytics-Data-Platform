/*
===============================================================================
FILE: 00_create_sdi_gold_sa360_test_results.sql
LAYER: Gold QA

PURPOSE:
  Centralized Gold QA test results table.

DESIGN:
  - One row per test execution
  - PASS and FAIL both logged
  - Partitioned by test_date for fast daily filtering
  - Clustered by table_name, test_layer, severity_level

TARGET TABLE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
(
  -- Execution Metadata
  test_run_timestamp TIMESTAMP OPTIONS(description="Exact timestamp when test executed."),
  test_date DATE OPTIONS(description="Logical execution date (CURRENT_DATE())."),

  -- Context
  table_name STRING OPTIONS(description="Table being validated (logical name)."),
  test_layer STRING OPTIONS(description="critical | reconciliation | business_logic"),
  test_name STRING OPTIONS(description="Human-readable test name."),
  severity_level STRING OPTIONS(description="HIGH | MEDIUM | LOW"),

  -- Metrics
  expected_value FLOAT64 OPTIONS(description="Expected numeric value."),
  actual_value FLOAT64 OPTIONS(description="Observed numeric value."),
  variance_value FLOAT64 OPTIONS(description="Actual - Expected (numeric)."),

  -- Outcome
  status STRING OPTIONS(description="PASS | FAIL"),
  status_emoji STRING OPTIONS(description="🟢 | 🔴"),
  failure_reason STRING OPTIONS(description="Human-readable failure reason."),
  next_step STRING OPTIONS(description="Actionable next step guidance."),

  -- Gating flags
  is_critical_failure BOOL OPTIONS(description="TRUE if severity is HIGH and status=FAIL."),
  is_pass BOOL OPTIONS(description="TRUE if status=PASS."),
  is_fail BOOL OPTIONS(description="TRUE if status=FAIL.")
)
PARTITION BY test_date
CLUSTER BY table_name, test_layer, severity_level
OPTIONS(
  description = "Gold QA results for SA360 daily & weekly tables. One row per test run."
);
