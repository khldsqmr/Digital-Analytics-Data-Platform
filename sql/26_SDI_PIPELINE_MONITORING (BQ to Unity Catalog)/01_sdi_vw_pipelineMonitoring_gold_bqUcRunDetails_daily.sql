CREATE OR REPLACE VIEW
    prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcRunDetails_daily
AS

SELECT

    /* ==============================================================
       RUN INFORMATION
       ============================================================== */

    TRY_CAST(job_run_ts AS TIMESTAMP) AS job_run_ts,
    TRY_CAST(job_end_ts AS TIMESTAMP) AS job_end_ts,

    CAST(
        TRY_CAST(job_run_ts AS TIMESTAMP)
        AS DATE
    ) AS run_date,

    total_time,
    load_type,

    TRY_CAST(date_from AS DATE) AS date_from,
    TRY_CAST(date_to AS DATE) AS date_to,


    /* ==============================================================
       SOURCE / TARGET
       ============================================================== */

    bq_table,
    uc_table,
    run_type,


    /* ==============================================================
       STATUS

       Source currently contains values such as:
         ✅ Success
         ❌ Failed

       Normalize these for downstream monitoring.
       ============================================================== */

    status AS status_raw,

    CASE
        WHEN LOWER(status) LIKE '%failed%'
            THEN 'Failed'

        WHEN LOWER(status) LIKE '%success%'
            THEN 'Success'

        WHEN LOWER(status) LIKE '%skipped%'
            THEN 'Skipped'

        ELSE 'Unknown'
    END AS status,


    CASE
        WHEN LOWER(status) LIKE '%failed%'
            THEN TRUE
        ELSE FALSE
    END AS is_failed,


    /* ==============================================================
       ROW COUNTS
       ============================================================== */

    bq_rows,
    loaded_rows,

    CASE
        WHEN bq_rows IS NOT NULL
         AND loaded_rows IS NOT NULL
        THEN loaded_rows - bq_rows

        ELSE NULL
    END AS row_count_difference,


    CASE
        WHEN bq_rows IS NOT NULL
         AND loaded_rows IS NOT NULL
         AND bq_rows <> loaded_rows
        THEN TRUE

        ELSE FALSE
    END AS has_rowCountMismatch,


    /* ==============================================================
       EXECUTION INFORMATION
       ============================================================== */

    elapsed_sec,
    error_message,


    /* ==============================================================
       SCHEMA CHANGES
       ============================================================== */

    cols_added,
    cols_removed,
    cols_changed,

    CASE
        WHEN cols_added IS NOT NULL
          OR cols_removed IS NOT NULL
          OR cols_changed IS NOT NULL
        THEN TRUE

        ELSE FALSE
    END AS has_schemaChange,


    /* ==============================================================
       PIPELINE CONFIGURATION
       ============================================================== */

    lookback_days,
    batch_number,
    date_storage_type


FROM prd_dbi_analytics.improvado.bq_uc_pipeline_run_log;