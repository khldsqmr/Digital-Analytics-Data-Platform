-- =====================================================================
-- PROJECT  : Pipeline Monitoring
-- OBJECT   : Gold View
-- PURPOSE  : Lists BQ -> UC tables whose latest execution for a day
--            remains in Failed status
-- CADENCE  : Daily
--
-- EXAMPLE:
--
--   Table A 09:00 Failed
--   Table A 10:00 Success
--
--   Result:
--     Table A will NOT appear because its latest execution succeeded.
--
--   Table B 09:00 Failed
--
--   Result:
--     Table B WILL appear because its latest execution remains Failed.
-- =====================================================================


CREATE OR REPLACE VIEW
    prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcFailedTables_daily
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

)


SELECT

    run_date,

    job_run_ts,

    job_end_ts,

    batch_number,

    bq_table,

    uc_table,

    load_type,

    run_type,

    status,

    status_raw,

    bq_rows,

    loaded_rows,

    row_count_difference,

    has_rowCountMismatch,

    elapsed_sec,

    error_message,

    cols_added,

    cols_removed,

    cols_changed,

    has_schemaChange

FROM ranked_runs


WHERE

    run_rank = 1

    AND status = 'Failed';