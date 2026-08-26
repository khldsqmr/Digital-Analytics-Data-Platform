SELECT *
FROM prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcHealthSummary_daily
ORDER BY run_date DESC
LIMIT 30;


SELECT *
FROM prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcFailedTables_daily
WHERE run_date = CURRENT_DATE()
ORDER BY batch_number, bq_table;

SELECT
    bq_table,
    uc_table
FROM prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcFailedTables_daily
WHERE run_date = CURRENT_DATE()
ORDER BY bq_table;


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


    


    prd_dbi_analytics.improvado.bq_uc_pipeline_run_log
                    ↓
       Gold Run Details View
                    ↓
          ┌─────────┴─────────┐
          ↓                   ↓
Gold Health Summary    Gold Failed Tables
          ↓                   ↓
          └─────────┬─────────┘
                    ↓
            Alert Notebook
                    ↓
             Run existed?
              /       \
            No         Yes
            ↓           ↓
          FAIL      failures?
                     /     \
                   Yes      No
                    ↓        ↓
                  FAIL     SUCCESS