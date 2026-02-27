/*
===============================================================================
TEST | BRONZE | AD MEDIA | NEGATIVE METRICS
===============================================================================
*/

SELECT *
FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_admedia_daily`
WHERE
  clicks < 0
  OR impressions < 0
  OR conversions < 0
  OR spend < 0;
