SELECT
  validation_run_id,
  data_as_of_date,
  data_source,
  metric_name,
  source_value,
  bronze_value,
  bronze_comparable_value,
  silver_value,
  gold_value,
  source_bronze_variance_pct,
  bronze_silver_variance_pct,
  silver_gold_variance_pct,
  issue_layer,
  status,
  notes,
  next_step

FROM
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun

ORDER BY
  validation_run_ts DESC,
  data_source,
  metric_name;


-- Useful verification queries
-- After deployment, this is the query I would run first:

SELECT
  data_source,
  metric_name,

  source_value,
  bronze_value,
  bronze_comparable_value,
  silver_value,
  gold_value,

  source_bronze_variance_pct,
  bronze_silver_variance_pct,
  silver_gold_variance_pct,

  comparison_method,
  issue_layer,
  status,

  notes,
  next_step

FROM
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_latest_perRun

ORDER BY
  CASE status
    WHEN 'Failed'  THEN 1
    WHEN 'Warning' THEN 2
    ELSE 3
  END,
  data_source,
  metric_name;

-- For only actual problems:

SELECT
  data_source,
  metric_name,

  source_value,
  bronze_value,
  bronze_comparable_value,
  silver_value,
  gold_value,

  issue_layer,
  status,

  notes,
  next_step

FROM
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_issues_perRun

WHERE
  validation_run_id = (
    SELECT validation_run_id
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun
    WHERE is_latest_run = TRUE
  )

ORDER BY
  status_priority,
  data_source,
  metric_name;

-- For a summary of the latest run and And high-level health:
SELECT
  data_source,
  metric_count,
  healthy_count,
  warning_count,
  failed_count,
  overall_status,
  issue_metrics,
  issue_summary,
  next_step_summary

FROM
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_summary_perRun

WHERE
  validation_run_id = (
    SELECT validation_run_id
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun
    WHERE is_latest_run = TRUE
  )

ORDER BY
  CASE
    WHEN summary_level = 'OVERALL' THEN 1
    ELSE 2
  END,
  data_source;