/* =================================================================================================
FILE:           sdi_vw_dashboardPulseTms_validation_issues_perRun.sql
LAYER:          Validation / Monitoring
CATALOG.SCHEMA: prdrzranalytics.lab42
VIEW:           sdi_vw_dashboardPulseTms_validation_issues_perRun

PURPOSE:
  Returns only metric-level validation rows requiring attention.

INCLUDED STATUS:
  - Warning
  - Failed

EXCLUDED STATUS:
  - Healthy

PRIMARY USE:
  Dashboard drill-down:

    Overall Run
      ->
    Data Source
      ->
    Metric
      ->
    Issue Layer
      ->
    Source / Bronze / Bronze Comparable / Silver / Gold
      ->
    Variance
      ->
    Notes
      ->
    Next Step

HISTORICAL USE:
  This view contains issues from ALL validation runs.

  Dashboard filters can use:
    validation_run_id

  or:
    orchestration_job_run_id

ORCHESTRATION LINEAGE:
  JOB:
    orchestration_job_run_id is the actual Databricks Job Run ID.

  MANUAL:
    orchestration_job_run_id is the generated PULSETMS_MAN_* ID.

================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_issues_perRun

AS

SELECT

  /* ------------------------------------------------------------------------------------------------
     Validation execution
     ---------------------------------------------------------------------------------------------- */

  validation_run_id,
  validation_run_ts,


  /* ------------------------------------------------------------------------------------------------
     Orchestration execution lineage
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
  days_in_period,


  /* ------------------------------------------------------------------------------------------------
     Validation metric
     ---------------------------------------------------------------------------------------------- */

  data_source,
  metric_name,
  metric_type,

  comparison_scope,
  comparison_method,


  /* ------------------------------------------------------------------------------------------------
     Layer values
     ---------------------------------------------------------------------------------------------- */

  source_value,

  bronze_value,
  bronze_comparable_value,

  silver_value,
  gold_value,


  /* ------------------------------------------------------------------------------------------------
     Source -> Bronze reconciliation
     ---------------------------------------------------------------------------------------------- */

  source_bronze_variance,
  source_bronze_variance_pct,


  /* ------------------------------------------------------------------------------------------------
     Bronze Comparable -> Silver reconciliation
     ---------------------------------------------------------------------------------------------- */

  bronze_silver_variance,
  bronze_silver_variance_pct,


  /* ------------------------------------------------------------------------------------------------
     Silver -> Gold reconciliation
     ---------------------------------------------------------------------------------------------- */

  silver_gold_variance,
  silver_gold_variance_pct,


  /* ------------------------------------------------------------------------------------------------
     Validation result
     ---------------------------------------------------------------------------------------------- */

  issue_layer,
  status,

  notes,
  next_step,


  /* ------------------------------------------------------------------------------------------------
     Object lineage
     ---------------------------------------------------------------------------------------------- */

  source_object,
  bronze_object,
  silver_object,
  gold_object,


  /* ------------------------------------------------------------------------------------------------
     Sort priority

     Failed first
     Warning second
     ---------------------------------------------------------------------------------------------- */

  CASE status

    WHEN 'Failed'
      THEN 1

    WHEN 'Warning'
      THEN 2

    ELSE 3

  END AS status_priority


FROM
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun


WHERE
  status IN (
    'Warning',
    'Failed'
  )
;