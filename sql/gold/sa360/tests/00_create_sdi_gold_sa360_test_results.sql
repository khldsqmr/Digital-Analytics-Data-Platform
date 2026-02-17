/*
===============================================================================
FILE: 00_create_sdi_gold_sa360_test_results.sql
LAYER: Gold QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation

PURPOSE:
  Centralized Gold QA results table.
  - All test procedures insert exactly ONE row per test.
  - PASS and FAIL both logged.
  - Designed for dashboarding & orchestration gating.

PARTITION / CLUSTER:
  - PARTITION BY test_date
  - CLUSTER BY table_name, test_layer, severity_level

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_test_results`
(
  -- Execution Metadata
  test_run_timestamp TIMESTAMP OPTIONS(description="Exact timestamp when test executed"),
  test_date DATE OPTIONS(description="Logical execution date (CURRENT_DATE())"),

  -- Context
  table_name STRING OPTIONS(description="Table being validated"),
  test_layer STRING OPTIONS(description="critical | reconciliation | business_logic"),
  test_name STRING OPTIONS(description="Human-readable test name"),
  severity_level STRING OPTIONS(description="HIGH | MEDIUM | LOW"),

  -- Metrics
  expected_value FLOAT64 OPTIONS(description="Expected numeric value"),
  actual_value FLOAT64 OPTIONS(description="Actual numeric value"),
  variance_value FLOAT64 OPTIONS(description="Actual - Expected"),

  -- Outcome
  status STRING OPTIONS(description="PASS | FAIL"),
  status_emoji STRING OPTIONS(description="🟢 | 🔴"),
  failure_reason STRING OPTIONS(description="Short reason for failure"),
  next_step STRING OPTIONS(description="What to do next"),

  -- Flags
  is_critical_failure BOOL OPTIONS(description="True when HIGH severity FAIL in critical layer"),
  is_pass BOOL OPTIONS(description="True if PASS"),
  is_fail BOOL OPTIONS(description="True if FAIL")
)
PARTITION BY test_date
CLUSTER BY table_name, test_layer, severity_level
OPTIONS(
  description = "Centralized Gold QA test results for SA360 Gold Daily + Weekly tables."
);
