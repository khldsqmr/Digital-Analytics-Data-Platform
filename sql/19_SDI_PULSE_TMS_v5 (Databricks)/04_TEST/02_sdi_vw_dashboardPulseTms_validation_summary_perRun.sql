/* =================================================================================================
FILE:           sdi_vw_dashboardPulseTms_validation_summary_perRun.sql
LAYER:          Validation / Monitoring
CATALOG.SCHEMA: prdrzranalytics.lab42
VIEW:           sdi_vw_dashboardPulseTms_validation_summary_perRun

PURPOSE:
  Summarizes metric-level validation results by data source and validation run.

OUTPUT:
  DATA_SOURCE rows:
    - ADOBE
    - MFC_SPEND
    - PLATFORM_SPEND
    - BIDDABLE_SPEND
    - QGP_SCORECARD
    - UPV_FORECAST

  PLUS:
    - one OVERALL / PULSE_TMS row per validation run

ORCHESTRATION LINEAGE:
  Carries the execution identifiers associated with each validation snapshot:

    orchestration_run_type
      JOB / MANUAL

    orchestration_job_id
      Databricks Job ID or PULSETMS_MAN

    orchestration_job_run_id
      Databricks Job Run ID or generated PULSETMS_MAN_* ID

    orchestration_task_run_id
      Databricks Task Run ID or generated manual task ID

    orchestration_execution_count
      Databricks task execution count / retry number

STATUS PRECEDENCE:
  Failed > Warning > Healthy

DASHBOARD USE:
  - Overall status card
  - Data-source status table
  - Issue counts
  - High-level issue summary
  - Next-step summary

================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_summary_perRun

AS

WITH

/* =================================================================================================
   DATA-SOURCE LEVEL SUMMARY

   One row per:
     validation run
       x
     data source
   ================================================================================================= */

BySource AS (

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
       Summary level
       ------------------------------------------------------------------------------------------- */

    'DATA_SOURCE' AS summary_level,

    data_source,


    /* ---------------------------------------------------------------------------------------------
       Metric counts
       ------------------------------------------------------------------------------------------- */

    COUNT(*) AS metric_count,


    SUM(
      CASE
        WHEN status = 'Healthy'
          THEN 1

        ELSE 0
      END
    ) AS healthy_count,


    SUM(
      CASE
        WHEN status = 'Warning'
          THEN 1

        ELSE 0
      END
    ) AS warning_count,


    SUM(
      CASE
        WHEN status = 'Failed'
          THEN 1

        ELSE 0
      END
    ) AS failed_count,


    /* ---------------------------------------------------------------------------------------------
       Data-source overall status

       Precedence:
         Failed
           >
         Warning
           >
         Healthy
       ------------------------------------------------------------------------------------------- */

    CASE

      WHEN SUM(
             CASE
               WHEN status = 'Failed'
                 THEN 1

               ELSE 0
             END
           ) > 0
        THEN 'Failed'


      WHEN SUM(
             CASE
               WHEN status = 'Warning'
                 THEN 1

               ELSE 0
             END
           ) > 0
        THEN 'Warning'


      ELSE 'Healthy'

    END AS overall_status,


    /* ---------------------------------------------------------------------------------------------
       Metrics requiring attention

       Example:
         mfcSpendActualBroadband [DATA_AVAILABILITY],
         biddableSpendFiber [DATA_AVAILABILITY]
       ------------------------------------------------------------------------------------------- */

    COALESCE(
      NULLIF(
        ARRAY_JOIN(
          SORT_ARRAY(
            COLLECT_SET(
              CASE

                WHEN status <> 'Healthy'
                  THEN CONCAT(
                    metric_name,
                    ' [',
                    issue_layer,
                    ']'
                  )

              END
            )
          ),
          ', '
        ),
        ''
      ),
      'None'
    ) AS issue_metrics,


    /* ---------------------------------------------------------------------------------------------
       Human-readable issue summary
       ------------------------------------------------------------------------------------------- */

    COALESCE(
      NULLIF(
        ARRAY_JOIN(
          SORT_ARRAY(
            COLLECT_SET(
              CASE

                WHEN status <> 'Healthy'
                  THEN CONCAT(
                    metric_name,
                    ': ',
                    notes
                  )

              END
            )
          ),
          ' | '
        ),
        ''
      ),
      'No validation issues detected.'
    ) AS issue_summary,


    /* ---------------------------------------------------------------------------------------------
       Suggested next steps
       ------------------------------------------------------------------------------------------- */

    COALESCE(
      NULLIF(
        ARRAY_JOIN(
          SORT_ARRAY(
            COLLECT_SET(
              CASE

                WHEN status <> 'Healthy'
                  THEN next_step

              END
            )
          ),
          ' | '
        ),
        ''
      ),
      'No action required.'
    ) AS next_step_summary


  FROM
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun


  GROUP BY

    validation_run_id,
    validation_run_ts,

    orchestration_run_type,
    orchestration_job_id,
    orchestration_job_run_id,
    orchestration_task_run_id,
    orchestration_execution_count,

    data_as_of_date,
    week_type,

    data_source
),


/* =================================================================================================
   OVERALL PULSETMS SUMMARY

   One row per validation execution.

   Rolls all data-source summaries into one overall pipeline status.
   ================================================================================================= */

Overall AS (

  SELECT

    /* ---------------------------------------------------------------------------------------------
       Validation execution
       ------------------------------------------------------------------------------------------- */

    bs.validation_run_id,
    bs.validation_run_ts,


    /* ---------------------------------------------------------------------------------------------
       Orchestration execution lineage
       ------------------------------------------------------------------------------------------- */

    bs.orchestration_run_type,
    bs.orchestration_job_id,
    bs.orchestration_job_run_id,
    bs.orchestration_task_run_id,
    bs.orchestration_execution_count,


    /* ---------------------------------------------------------------------------------------------
       Reporting period
       ------------------------------------------------------------------------------------------- */

    bs.data_as_of_date,
    bs.week_type,


    /* ---------------------------------------------------------------------------------------------
       Summary level
       ------------------------------------------------------------------------------------------- */

    'OVERALL' AS summary_level,

    'PULSE_TMS' AS data_source,


    /* ---------------------------------------------------------------------------------------------
       Metric counts
       ------------------------------------------------------------------------------------------- */

    SUM(bs.metric_count)
      AS metric_count,

    SUM(bs.healthy_count)
      AS healthy_count,

    SUM(bs.warning_count)
      AS warning_count,

    SUM(bs.failed_count)
      AS failed_count,


    /* ---------------------------------------------------------------------------------------------
       Overall pipeline status

       Precedence:
         Failed
           >
         Warning
           >
         Healthy
       ------------------------------------------------------------------------------------------- */

    CASE

      WHEN SUM(bs.failed_count) > 0
        THEN 'Failed'

      WHEN SUM(bs.warning_count) > 0
        THEN 'Warning'

      ELSE 'Healthy'

    END AS overall_status,


    /* ---------------------------------------------------------------------------------------------
       Aggregate issue metric list across affected data sources
       ------------------------------------------------------------------------------------------- */

    COALESCE(
      NULLIF(
        ARRAY_JOIN(
          SORT_ARRAY(
            COLLECT_SET(
              CASE

                WHEN bs.overall_status <> 'Healthy'
                  THEN CONCAT(
                    bs.data_source,
                    ': ',
                    bs.issue_metrics
                  )

              END
            )
          ),
          ' | '
        ),
        ''
      ),
      'None'
    ) AS issue_metrics,


    /* ---------------------------------------------------------------------------------------------
       Aggregate issue descriptions
       ------------------------------------------------------------------------------------------- */

    COALESCE(
      NULLIF(
        ARRAY_JOIN(
          SORT_ARRAY(
            COLLECT_SET(
              CASE

                WHEN bs.overall_status <> 'Healthy'
                  THEN CONCAT(
                    bs.data_source,
                    ': ',
                    bs.issue_summary
                  )

              END
            )
          ),
          ' || '
        ),
        ''
      ),
      'No validation issues detected.'
    ) AS issue_summary,


    /* ---------------------------------------------------------------------------------------------
       Aggregate next steps
       ------------------------------------------------------------------------------------------- */

    COALESCE(
      NULLIF(
        ARRAY_JOIN(
          SORT_ARRAY(
            COLLECT_SET(
              CASE

                WHEN bs.overall_status <> 'Healthy'
                  THEN bs.next_step_summary

              END
            )
          ),
          ' || '
        ),
        ''
      ),
      'No action required.'
    ) AS next_step_summary


  FROM BySource bs


  GROUP BY

    bs.validation_run_id,
    bs.validation_run_ts,

    bs.orchestration_run_type,
    bs.orchestration_job_id,
    bs.orchestration_job_run_id,
    bs.orchestration_task_run_id,
    bs.orchestration_execution_count,

    bs.data_as_of_date,
    bs.week_type
)


/* =================================================================================================
   FINAL OUTPUT
   ================================================================================================= */

SELECT
  *

FROM BySource


UNION ALL


SELECT
  *

FROM Overall
;