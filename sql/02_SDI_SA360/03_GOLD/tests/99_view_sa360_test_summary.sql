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
  SELECT 'bronze' AS pipeline_layer, * 
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_bronze_sa360_test_dashboard`
  UNION ALL
  SELECT 'silver' AS pipeline_layer, *
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_silver_sa360_test_dashboard`
  UNION ALL
  SELECT 'gold' AS pipeline_layer, *
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_gold_sa360_test_dashboard`
),
latest AS (
  SELECT MAX(test_run_timestamp) AS max_ts
  FROM base
),
batch AS (
  -- define "this run" as everything within the last N minutes of the global max timestamp
  SELECT b.*
  FROM base b
  CROSS JOIN latest l
  WHERE b.test_run_timestamp BETWEEN TIMESTAMP_SUB(l.max_ts, INTERVAL 30 MINUTE) AND l.max_ts
),
ranked AS (
  -- IMPORTANT: rank AFTER filtering to batch, so stale tests don't leak in
  SELECT
    *,
    DENSE_RANK() OVER (
      PARTITION BY pipeline_layer, test_name
      ORDER BY test_run_timestamp DESC
    ) AS rnk
  FROM batch
)
SELECT *
FROM ranked
WHERE rnk = 1
ORDER BY test_run_timestamp DESC, pipeline_layer, test_name;