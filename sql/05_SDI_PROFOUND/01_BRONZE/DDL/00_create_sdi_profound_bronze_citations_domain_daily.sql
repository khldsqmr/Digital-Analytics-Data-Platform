CREATE OR REPLACE TABLE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_<entity>_daily`
(
    -- Primary grain columns
    account_name STRING,
    <entity_key_1> STRING,
    <entity_key_2> STRING,
    event_date DATE,

    -- Metrics
    share_of_voice FLOAT64,
    visibility_score FLOAT64,
    count INT64,
    mentions_count INT64,
    executions INT64,

    -- Lineage / metadata
    filename STRING,
    file_load_datetime TIMESTAMP,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY event_date
CLUSTER BY account_name;