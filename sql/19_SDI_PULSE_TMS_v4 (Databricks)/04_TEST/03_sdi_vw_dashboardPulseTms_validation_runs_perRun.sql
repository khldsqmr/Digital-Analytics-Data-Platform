/* =================================================================================================
FILE:           sdi_vw_dashboardPulseTms_validation_runs_perRun.sql
LAYER:          Validation / Monitoring
CATALOG.SCHEMA: prdrzranalytics.lab42
VIEW:           sdi_vw_dashboardPulseTms_validation_runs_perRun

PURPOSE:
  Returns exactly one row per validation execution.

PRIMARY USE:
  - Historical Run dashboard dropdown
  - Latest-run identification
  - Run-level status display
  - Databricks Job / manual execution lineage

IMPORTANT:
  The same data_as_of_date can appear multiple times.

  Example:

    Monday Job Run
      data_as_of_date = 2026-09-19

    Tuesday Job Run
      data_as_of_date = 2026-09-19

    Wednesday Manual Run
      data_as_of_date = 2026-09-19

  These remain separate validation executions because each has a different:
    validation_run_id
    orchestration_job_run_id

RUN TYPES:
  JOB
    orchestration_job_run_id contains the actual Databricks Job Run ID.

  MANUAL
    orchestration_job_run_id contains:
      PULSETMS_MAN_yyyyMMdd_HHmmss_SSS

================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun

AS

WITH

/* =================================================================================================
   OVERALL RUNS ONLY

   Summary already contains:
     - data-source rows
     - one overall row

   Only use OVERALL so this view produces exactly one row per validation execution.
   ================================================================================================= */

OverallRuns AS (

  SELECT

    /* ---------------------------------------------------------------------------------------------
       Validation execution
       ------------------------------------------------------------------------------------------- */

    validation_run_id,
    validation_run_ts,


    /* ---------------------------------------------------------------------------------------------
       Orchestration execution lineage
       ------------------------------------------------------------------------------------------- */

    orchestration_run_type,
    orchestration_job_id,
    orchestration_job_run_id,
    orchestration_task_run_id,
    orchestration_execution_count,


    /* ---------------------------------------------------------------------------------------------
       Reporting period
       ------------------------------------------------------------------------------------------- */

    data_as_of_date,
    week_type,


    /* ---------------------------------------------------------------------------------------------
       Validation result
       ------------------------------------------------------------------------------------------- */

    overall_status,

    metric_count,
    healthy_count,
    warning_count,
    failed_count,

    issue_metrics,
    issue_summary,
    next_step_summary


  FROM
    prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_summary_perRun


  WHERE
    summary_level = 'OVERALL'
),


/* =================================================================================================
   RANK RUNS

   Most recent validation execution receives run_rank = 1.
   ================================================================================================= */

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


/* =================================================================================================
   FINAL OUTPUT
   ================================================================================================= */

SELECT

  /* ------------------------------------------------------------------------------------------------
     Validation execution
     ---------------------------------------------------------------------------------------------- */

  validation_run_id,
  validation_run_ts,


  /* ------------------------------------------------------------------------------------------------
     Orchestration execution
     ---------------------------------------------------------------------------------------------- */

  orchestration_run_type,
  orchestration_job_id,
  orchestration_job_run_id,
  orchestration_task_run_id,
  orchestration_execution_count,


  /* ------------------------------------------------------------------------------------------------
     Reporting period
     ---------------------------------------------------------------------------------------------- */

  data_as_of_date,
  week_type,


  /* ------------------------------------------------------------------------------------------------
     Validation status
     ---------------------------------------------------------------------------------------------- */

  overall_status,

  metric_count,
  healthy_count,
  warning_count,
  failed_count,

  issue_metrics,
  issue_summary,
  next_step_summary,


  /* ------------------------------------------------------------------------------------------------
     RUN DISPLAY ID

     JOB example:
       123456789

     MANUAL example:
       PULSETMS_MAN_20260923_192530_123

     Defensive fallback:
       validation_run_id
     ---------------------------------------------------------------------------------------------- */

  COALESCE(
    orchestration_job_run_id,
    validation_run_id
  ) AS run_display_id,


  /* ------------------------------------------------------------------------------------------------
     DASHBOARD RUN LABEL

     JOB:
       Run 123456789 | 2026-09-23 18:46:02 | Data: 2026-09-19 | Healthy

     MANUAL:
       Run PULSETMS_MAN_20260923_192530_123
       | 2026-09-23 19:25:30
       | Data: 2026-09-19
       | Healthy

     Manual runs therefore remain identifiable without being visually treated as errors.
     ---------------------------------------------------------------------------------------------- */

  CONCAT(
    'Run ',
    COALESCE(
      orchestration_job_run_id,
      validation_run_id
    ),
    ' | ',
    DATE_FORMAT(
      validation_run_ts,
      'yyyy-MM-dd HH:mm:ss'
    ),
    ' | Data: ',
    CAST(
      data_as_of_date
      AS STRING
    ),
    ' | ',
    overall_status
  ) AS run_label,


  /* ------------------------------------------------------------------------------------------------
     Latest run flag
     ---------------------------------------------------------------------------------------------- */

  CASE

    WHEN run_rank = 1
      THEN TRUE

    ELSE FALSE

  END AS is_latest_run


FROM Ranked
;