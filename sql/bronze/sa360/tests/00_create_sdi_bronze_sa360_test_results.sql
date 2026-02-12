/*
===============================================================================
FILE: 00_create_sdi_bronze_sa360_test_results.sql
LAYER: Bronze Monitoring
PURPOSE:
  Central test result table for ALL Bronze SA360 validation tests.

DESIGN PRINCIPLES:
  • One unified monitoring table
  • Daily append only
  • Supports severity & blocking
  • Dashboard ready
  • Human readable error guidance

===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`
(
  -- ======================================================
  -- RUN METADATA
  -- ======================================================

  test_run_timestamp TIMESTAMP,
  test_date DATE,

  table_name STRING,
  test_layer STRING,           -- critical / reconciliation / weekly
  test_name STRING,
  severity_level STRING,       -- HIGH / MEDIUM / LOW

  -- ======================================================
  -- NUMERIC VALIDATION
  -- ======================================================

  expected_value FLOAT64,
  actual_value FLOAT64,
  variance_value FLOAT64,

  -- ======================================================
  -- RESULT STATUS
  -- ======================================================

  status STRING,               -- PASS / FAIL / WARNING
  status_emoji STRING,         -- 🟢 🟡 🔴

  -- ======================================================
  -- EXPLANATION & NEXT STEPS
  -- ======================================================

  failure_reason STRING,
  next_step STRING
)
PARTITION BY test_date
CLUSTER BY table_name, test_layer;
