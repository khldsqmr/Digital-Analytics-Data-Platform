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

    run_type,
    status,

    bq_rows,
    loaded_rows,
    row_count_difference,

    elapsed_sec,
    error_message

FROM ranked_runs

WHERE run_rank = 1
  AND status = 'Failed';