/* =================================================================================================
FILE:           sdi_vw_dashboardPulseTms_validation_latest_perRun.sql
LAYER:          Validation / Monitoring
VIEW:           sdi_vw_dashboardPulseTms_validation_latest_perRun

PURPOSE:
  Returns all metric-level validation rows belonging to the latest validation execution.

PRIMARY USE:
  Databricks Dashboard -> Latest Run tab.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_latest_perRun
AS

WITH LatestRun AS (

  SELECT
    validation_run_id

  FROM
    prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun

  WHERE
    is_latest_run = TRUE
)


SELECT
  h.*

FROM
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun h

INNER JOIN LatestRun l
  ON h.validation_run_id = l.validation_run_id
;