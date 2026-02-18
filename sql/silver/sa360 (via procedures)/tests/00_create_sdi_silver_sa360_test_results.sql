/*
===============================================================================
FILE: 00_create_sdi_silver_sa360_test_results.sql
LAYER: Silver QA
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:   sdi_silver_sa360_test_results

PURPOSE:
  Centralized results table for Silver SA360 QA tests.
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_test_results`
(
  test_run_timestamp TIMESTAMP,
  test_date          DATE,

  table_name         STRING,
  test_layer         STRING,
  test_name          STRING,
  severity_level     STRING,

  expected_value     FLOAT64,
  actual_value       FLOAT64,
  variance_value     FLOAT64,

  status             STRING,
  status_emoji       STRING,
  failure_reason     STRING,
  next_step          STRING,

  is_critical_failure BOOL,
  is_pass            BOOL,
  is_fail            BOOL
)
PARTITION BY test_date
CLUSTER BY table_name, test_layer, severity_level
OPTIONS(description="Silver SA360 QA test execution results (PASS + FAIL).");
