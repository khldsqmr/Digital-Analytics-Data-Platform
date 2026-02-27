CREATE OR REPLACE TABLE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_gold_<entity>_daily`
PARTITION BY event_date
CLUSTER BY account_name AS

SELECT
    *
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_silver_<entity>_daily`;