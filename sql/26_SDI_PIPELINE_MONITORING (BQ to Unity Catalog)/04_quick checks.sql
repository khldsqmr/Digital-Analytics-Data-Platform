SELECT *
FROM prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcHealthSummary_daily
ORDER BY run_date DESC
LIMIT 30;


WITH ranked_runs AS (

    SELECT
        *,

        ROW_NUMBER() OVER (
            PARTITION BY bq_table
            ORDER BY job_run_ts DESC
        ) AS run_rank

    FROM
        prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcRunDetails_daily

    WHERE run_date = CURRENT_DATE()

)

SELECT

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
  AND status = 'Failed'

ORDER BY
    batch_number,
    bq_table;


    