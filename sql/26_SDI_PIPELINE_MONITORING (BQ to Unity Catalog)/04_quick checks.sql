SELECT *
FROM prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcRunDetails_daily
ORDER BY job_run_ts DESC
LIMIT 100;

SELECT *
FROM prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcHealthSummary_daily
ORDER BY run_date DESC
LIMIT 30;

SELECT *
FROM prdrzranalytics.lab42.sdi_vw_pipelineMonitoring_gold_bqUcFailedTables_daily
ORDER BY run_date DESC, batch_number, bq_table;


    
/*

prd_dbi_analytics.improvado.bq_uc_pipeline_run_log
                         │
                         ▼
┌─────────────────────────────────────────────────────────────┐
│ Gold 1                                                      │
│ sdi_vw_pipelineMonitoring_gold_bqUcRunDetails_daily         │
│                                                             │
│ Clean / normalized detailed run history                     │
└────────────────────────────┬────────────────────────────────┘
                             │
                 ┌───────────┴───────────┐
                 │                       │
                 ▼                       ▼
┌────────────────────────┐   ┌──────────────────────────────┐
│ Gold 2                 │   │ Gold 3                       │
│ Health Summary         │   │ Failed Tables                │
│                        │   │                              │
│ Daily health metrics   │   │ Latest status per table/day  │
└────────────┬───────────┘   └──────────────┬───────────────┘
             │                              │
             └──────────────┬───────────────┘
                            ▼
                 Monitoring Notebook
                            │
                    Determine latest
                    available run date
                            │
                  ┌─────────┴─────────┐
                  │                   │
             0 failures          failures > 0
                  │                   │
                  ▼                   ▼
               SUCCESS            Exception
                                      │
                                      ▼
                              Databricks task fails
                                      │
                                      ▼
                                 Notification

*/


/*
sdi_pipelineMonitoring_orchestration_bqUcFailureAlert_daily.ipynb


CELL 1
│
├── Set view names
├── Find MAX(run_date)
├── Print monitoring date
│
▼

CELL 2
│
├── Read health summary
├── Print metrics
├── display()
│
▼

CELL 3
│
├── Read failed-table Gold view
├── Get failed table names
├── Print each failure
├── Print error_message
├── display()
│
▼

CELL 4
│
├── failures = 0
│      ↓
│    PASS
│
└── failures > 0
       ↓
   Print names
       ↓
   raise Exception()
       ↓
   TASK FAILED
       ↓
   Notification


CELL 5
Optional success completion message

*/
