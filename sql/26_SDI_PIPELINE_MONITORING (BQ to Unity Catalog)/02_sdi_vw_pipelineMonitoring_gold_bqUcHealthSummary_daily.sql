CREATE OR REPLACE VIEW
    prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcHealthSummary_daily
AS

WITH ranked_runs AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY
                run_date,
                bq_table

            ORDER BY
                job_run_ts DESC
        ) AS run_rank

    FROM
        prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcRunDetails_daily

),


latest_table_runs AS (

    SELECT
        *

    FROM ranked_runs

    WHERE run_rank = 1

),


daily_attempts AS (

    SELECT
        run_date,
        COUNT(*) AS total_execution_attempts

    FROM
        prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcRunDetails_daily

    GROUP BY
        run_date

)


SELECT

    l.run_date,


    /* ==============================================================
       EXECUTION COUNTS
       ============================================================== */

    d.total_execution_attempts,

    COUNT(*) AS total_tables,

    SUM(
        CASE
            WHEN l.status = 'Success'
            THEN 1
            ELSE 0
        END
    ) AS successful_tables,

    SUM(
        CASE
            WHEN l.status = 'Failed'
            THEN 1
            ELSE 0
        END
    ) AS failed_tables,

    SUM(
        CASE
            WHEN l.status = 'Skipped'
            THEN 1
            ELSE 0
        END
    ) AS skipped_tables,

    SUM(
        CASE
            WHEN l.status = 'Unknown'
            THEN 1
            ELSE 0
        END
    ) AS unknownStatus_tables,


    /* ==============================================================
       SUCCESS RATE
       ============================================================== */

    ROUND(
        100.0
        *
        SUM(
            CASE
                WHEN l.status = 'Success'
                THEN 1
                ELSE 0
            END
        )
        /
        NULLIF(COUNT(*), 0),

        2
    ) AS success_rate_pct,


    /* ==============================================================
       DATA QUALITY
       ============================================================== */

    SUM(
        CASE
            WHEN l.has_rowCountMismatch = TRUE
            THEN 1
            ELSE 0
        END
    ) AS rowCountMismatch_tables,

    SUM(
        CASE
            WHEN l.has_schemaChange = TRUE
            THEN 1
            ELSE 0
        END
    ) AS schemaChange_tables,


    /* ==============================================================
       ROW VOLUMES
       ============================================================== */

    SUM(l.bq_rows) AS total_bq_rows,

    SUM(l.loaded_rows) AS total_loaded_rows,

    SUM(l.loaded_rows) - SUM(l.bq_rows)
        AS total_rowCountDifference,


    /* ==============================================================
       EXECUTION WINDOW
       ============================================================== */

    MIN(l.job_run_ts) AS first_jobRun_ts,

    MAX(l.job_end_ts) AS last_jobEnd_ts,

    ROUND(
        SUM(COALESCE(l.elapsed_sec, 0)) / 60.0,
        2
    ) AS total_elapsed_minutes


FROM latest_table_runs l

LEFT JOIN daily_attempts d
    ON l.run_date = d.run_date

GROUP BY
    l.run_date,
    d.total_execution_attempts;