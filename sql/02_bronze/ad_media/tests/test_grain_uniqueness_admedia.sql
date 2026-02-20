/*
===============================================================================
TEST | BRONZE | AD MEDIA | GRAIN UNIQUENESS
===============================================================================

EXPECTATION
- Exactly one row per:
  account_name + campaign_id + date

===============================================================================
*/

SELECT
  account_name,
  campaign_id,
  date,
  COUNT(*) AS row_count
FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_admedia_daily`
GROUP BY
  account_name,
  campaign_id,
  date
HAVING
  COUNT(*) > 1;
