CREATE OR REPLACE TABLE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_silver_<entity>_daily`
PARTITION BY event_date
CLUSTER BY account_name AS

SELECT *
FROM (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY account_name, <entity_key_1>, <entity_key_2>, event_date
            ORDER BY file_load_datetime DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_<entity>_daily`
)
WHERE rn = 1;