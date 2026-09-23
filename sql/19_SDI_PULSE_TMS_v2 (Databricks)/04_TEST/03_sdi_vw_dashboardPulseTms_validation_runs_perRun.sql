/* =================================================================================================
FILE:           sdi_vw_dashboardPulseTms_validation_runs_perRun.sql
LAYER:          Validation / Monitoring
VIEW:           sdi_vw_dashboardPulseTms_validation_runs_perRun

PURPOSE:
  Returns one row per validation execution.

PRIMARY USE:
  Dashboard Historical Run dropdown.

IMPORTANT:
  The same data_as_of_date can appear multiple times because the pipeline may rerun before the
  next weekly reporting period changes.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun
AS

WITH OverallRuns AS (

  SELECT
    validation_run_id,
    validation_run_ts,

    data_as_of_date,
    week_type,

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
    summary_level = 'OVERALL'
),


Ranked AS (

  SELECT
    *,

    ROW_NUMBER() OVER (
      ORDER BY
        validation_run_ts DESC,
        validation_run_id DESC
    ) AS run_rank

  FROM OverallRuns
)


SELECT
  validation_run_id,
  validation_run_ts,

  data_as_of_date,
  week_type,

  overall_status,

  metric_count,
  healthy_count,
  warning_count,
  failed_count,

  issue_metrics,
  issue_summary,
  next_step_summary,

  CONCAT(
    DATE_FORMAT(
      validation_run_ts,
      'yyyy-MM-dd HH:mm:ss'
    ),
    ' | Data: ',
    CAST(data_as_of_date AS STRING),
    ' | ',
    overall_status
  ) AS run_label,

  CASE
    WHEN run_rank = 1 THEN TRUE
    ELSE FALSE
  END AS is_latest_run

FROM Ranked
;