/*
===============================================================================
TEST | BRONZE | SEARCH CONSOLE | NO FUTURE DATES
===============================================================================
*/

SELECT *
FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
WHERE
  date > CURRENT_DATE();
