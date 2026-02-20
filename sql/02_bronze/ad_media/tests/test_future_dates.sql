/*
===============================================================================
TEST | BRONZE | AD MEDIA | NO FUTURE DATES
===============================================================================
*/

SELECT *
FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_admedia_daily`
WHERE
  date > CURRENT_DATE();
