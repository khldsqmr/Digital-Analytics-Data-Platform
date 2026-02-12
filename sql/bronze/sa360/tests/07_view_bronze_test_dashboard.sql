/*
===============================================================================
FILE: 07_view_bronze_test_dashboard.sql
LAYER: Monitoring
PURPOSE:
  Centralized Bronze QA Monitoring Dashboard View.

  • Shows latest runs first
  • Adds pass/fail classification
  • Adds critical flag
  • Keeps all diagnostic fields visible
  • Production-ready for Looker/Tableau/PowerBI

DATA SOURCE:
  sdi_bronze_sa360_test_results

===============================================================================
*/

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_bronze_sa360_test_dashboard`
AS

SELECT
    -- ================================
    -- Execution Info
    -- ================================
    test_run_timestamp,
    test_date,

    -- ================================
    -- Table Metadata
    -- ================================
    table_name,
    test_layer,
    test_name,
    severity_level,

    -- ================================
    -- Metrics
    -- ================================
    expected_value,
    actual_value,
    variance_value,

    -- ================================
    -- Status
    -- ================================
    status,
    status_emoji,

    -- ================================
    -- Diagnostics
    -- ================================
    failure_reason,
    next_step,

    -- ================================
    -- Derived Flags (Enterprise Ready)
    -- ================================

    CASE 
        WHEN status = 'FAIL' AND severity_level = 'CRITICAL'
        THEN 1
        ELSE 0
    END AS is_critical_failure,

    CASE 
        WHEN status = 'PASS'
        THEN 1
        ELSE 0
    END AS is_pass,

    CASE 
        WHEN status = 'FAIL'
        THEN 1
        ELSE 0
    END AS is_fail

FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_test_results`

ORDER BY
    test_run_timestamp DESC,
    severity_level DESC;
