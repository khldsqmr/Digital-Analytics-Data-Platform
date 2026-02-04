/*
===============================================================================
TEST | BRONZE | AD MEDIA | NOT NULL KEYS
===============================================================================
*/

SELECT *
FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_admedia_daily`
WHERE
  account_name IS NULL
  OR campaign_id IS NULL
  OR campaign IS NULL
  OR date IS NULL;
