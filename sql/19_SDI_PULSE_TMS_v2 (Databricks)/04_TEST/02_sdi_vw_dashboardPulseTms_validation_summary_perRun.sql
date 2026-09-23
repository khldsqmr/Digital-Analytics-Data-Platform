/* =================================================================================================
FILE:           sdi_vw_dashboardPulseTms_validation_summary_perRun.sql
LAYER:          Validation / Monitoring
VIEW:           sdi_vw_dashboardPulseTms_validation_summary_perRun

PURPOSE:
  Summarizes metric-level validation results by data source and validation run.

OUTPUT:
  DATA_SOURCE rows:
    ADOBE
    MFC_SPEND
    PLATFORM_SPEND
    BIDDABLE_SPEND
    QGP_SCORECARD
    UPV_FORECAST

  PLUS:
    one OVERALL / PULSE_TMS row per validation run.

STATUS PRECEDENCE:
  Failed > Warning > Healthy

DASHBOARD USE:
  Status cards / source summary table.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_validation_summary_perRun
AS

WITH

BySource AS (

  SELECT
    validation_run_id,
    validation_run_ts,

    data_as_of_date,
    week_type,

    'DATA_SOURCE' AS summary_level,

    data_source,

    COUNT(*) AS metric_count,

    SUM(
      CASE
        WHEN status = 'Healthy' THEN 1
        ELSE 0
      END
    ) AS healthy_count,

    SUM(
      CASE
        WHEN status = 'Warning' THEN 1
        ELSE 0
      END
    ) AS warning_count,

    SUM(
      CASE
        WHEN status = 'Failed' THEN 1
        ELSE 0
      END
    ) AS failed_count,


    CASE

      WHEN SUM(
             CASE
               WHEN status = 'Failed' THEN 1
               ELSE 0
             END
           ) > 0
        THEN 'Failed'

      WHEN SUM(
             CASE
               WHEN status = 'Warning' THEN 1
               ELSE 0
             END
           ) > 0
        THEN 'Warning'

      ELSE 'Healthy'

    END AS overall_status,


    /* ---------------------------------------------------------------------------------------------
       Which metrics require attention?
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
       Human-readable issue summary.
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
    data_as_of_date,
    week_type,
    data_source
),


Overall AS (

  SELECT
    validation_run_id,
    validation_run_ts,

    data_as_of_date,
    week_type,

    'OVERALL' AS summary_level,

    'PULSE_TMS' AS data_source,

    SUM(metric_count) AS metric_count,
    SUM(healthy_count) AS healthy_count,
    SUM(warning_count) AS warning_count,
    SUM(failed_count) AS failed_count,


    CASE

      WHEN SUM(failed_count) > 0
        THEN 'Failed'

      WHEN SUM(warning_count) > 0
        THEN 'Warning'

      ELSE 'Healthy'

    END AS overall_status,


    COALESCE(
      NULLIF(
        ARRAY_JOIN(
          SORT_ARRAY(
            COLLECT_SET(
              CASE
                WHEN overall_status <> 'Healthy'
                  THEN CONCAT(
                    data_source,
                    ': ',
                    issue_metrics
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


    COALESCE(
      NULLIF(
        ARRAY_JOIN(
          SORT_ARRAY(
            COLLECT_SET(
              CASE
                WHEN overall_status <> 'Healthy'
                  THEN CONCAT(
                    data_source,
                    ': ',
                    issue_summary
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


    COALESCE(
      NULLIF(
        ARRAY_JOIN(
          SORT_ARRAY(
            COLLECT_SET(
              CASE
                WHEN overall_status <> 'Healthy'
                  THEN next_step_summary
              END
            )
          ),
          ' || '
        ),
        ''
      ),
      'No action required.'
    ) AS next_step_summary

  FROM BySource

  GROUP BY
    validation_run_id,
    validation_run_ts,
    data_as_of_date,
    week_type
)


SELECT * FROM BySource

UNION ALL

SELECT * FROM Overall
;