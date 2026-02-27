/*
===============================================================================
TEST | BRONZE | SEARCH CONSOLE | NOT NULL KEYS
===============================================================================

EXPECTATION
- Core business keys must never be NULL

===============================================================================
*/

SELECT *
FROM
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
WHERE
  account_name IS NULL
  OR site_url IS NULL
  OR page IS NULL
  OR query IS NULL
  OR search_type IS NULL
  OR date IS NULL;
