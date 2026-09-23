/* =================================================================================================
FILE:           sdi_vw_dashboardPulseTms_validation_issues_perRun.sql
LAYER:          Validation / Monitoring
VIEW:           sdi_vw_dashboardPulseTms_validation_issues_perRun

PURPOSE:
  Returns only Warning / Failed validation rows.

PRIMARY USE:
  Dashboard drill-down:
    Overall status
      -> Data Source
      -> Metric
      -> Issue Layer
      -> Values
      -> Notes
      -> Next Step

This view contains all historical runs.
Filter validation_run_id in the Historical dashboard tab.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_issues_perRun
AS

SELECT

  validation_run_id,
  validation_run_ts,

  data_as_of_date,
  week_type,
  days_in_period,

  data_source,
  metric_name,
  metric_type,

  comparison_scope,
  comparison_method,

  source_value,

  bronze_value,
  bronze_comparable_value,

  silver_value,
  gold_value,

  source_bronze_variance,
  source_bronze_variance_pct,

  bronze_silver_variance,
  bronze_silver_variance_pct,

  silver_gold_variance,
  silver_gold_variance_pct,

  issue_layer,
  status,

  notes,
  next_step,

  source_object,
  bronze_object,
  silver_object,
  gold_object,

  CASE status
    WHEN 'Failed'  THEN 1
    WHEN 'Warning' THEN 2
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