/*
===============================================================================
TEST | BRONZE | SEARCH CONSOLE | QUERY DAILY | GRAIN UNIQUENESS
===============================================================================

EXPECTATION
- Exactly one row per:
  account_name + site_url + page + query + search_type + date

FAIL CONDITION
- Any grain appears more than once

RESULT
- 0 rows = PASS
- 1+ rows = FAIL

===============================================================================
*/

SELECT
  account_name,
  site_url,
  page,
  query,
  search_type,
  date,
  COUNT(*) AS row_count
FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
GROUP BY
  account_name,
  site_url,
  page,
  query,
  search_type,
  date
HAVING
  COUNT(*) > 1;
