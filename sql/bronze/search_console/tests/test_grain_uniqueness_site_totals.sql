/*
===============================================================================
TEST | BRONZE | SEARCH CONSOLE | SITE TOTALS | GRAIN UNIQUENESS
===============================================================================

EXPECTATION
- Exactly one row per:
  account_name + site_url + date

===============================================================================
*/

SELECT
  account_name,
  site_url,
  date,
  COUNT(*) AS row_count
FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily`
GROUP BY
  account_name,
  site_url,
  date
HAVING
  COUNT(*) > 1;
