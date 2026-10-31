/* =================================================================================================
FILE:           sdi_vw_dashboardPulseTms_validation_latest_perRun.sql
LAYER:          Validation / Monitoring
CATALOG.SCHEMA: prdrzranalytics.lab42
VIEW:           sdi_vw_dashboardPulseTms_validation_latest_perRun

PURPOSE:
  Returns every metric-level validation row belonging to the latest validation execution.

PRIMARY USE:
  Databricks Dashboard:
    Latest Run tab

LATEST RUN LOGIC:
  Latest validation_run_id is determined by:
    sdi_vw_dashboardPulseTms_validation_runs_perRun

  That view ranks executions using:
    validation_run_ts DESC
    validation_run_id DESC

OUTPUT:
  Includes:
    - validation execution metadata
    - orchestration execution metadata
    - reporting period
    - Source / Bronze / Silver / Gold values
    - variances
    - issue layer
    - status
    - notes
    - next step
    - source object lineage

================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_latest_perRun

AS

WITH

/* =================================================================================================
   IDENTIFY LATEST VALIDATION RUN
   ================================================================================================= */

LatestRun AS (

  SELECT
    validation_run_id

  FROM
    prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_runs_perRun

  WHERE
    is_latest_run = TRUE
)


/* =================================================================================================
   RETURN ALL METRIC-LEVEL RECORDS FOR THAT RUN
   ================================================================================================= */

SELECT
  h.*

FROM
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun h


INNER JOIN LatestRun l

  ON h.validation_run_id
   = l.validation_run_id
;