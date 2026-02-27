CREATE OR REPLACE TABLE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_silver_<entity>_daily`
PARTITION BY event_date
CLUSTER BY account_name AS

SELECT
    account_name,
    LOWER(<entity_key_1>) AS <entity_key_1>,
    LOWER(<entity_key_2>) AS <entity_key_2>,
    event_date,
    share_of_voice,
    visibility_score,
    count,
    mentions_count,
    executions
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_<entity>_daily`;