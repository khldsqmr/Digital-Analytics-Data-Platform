/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS | BACKFILL (LAST 3 YEARS)
===============================================================================

PURPOSE
- Backfill historical site-level Search Console data
- Covers last 3 years explicitly
- Safe to re-run (MERGE-based)

===============================================================================
*/

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily` T
USING (
  SELECT
    account_id,
    account_name,
    site_url,

    CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS date,

    clicks,
    impressions,
    sum_position,

    __insert_date,
    TIMESTAMP(File_Load_datetime) AS file_load_datetime,
    Filename AS filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_site_totals_tmo`
  WHERE
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))
      BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
          AND CURRENT_DATE()
) S
ON
  T.account_name = S.account_name
  AND T.site_url = S.site_url
  AND T.date = S.date

WHEN MATCHED THEN
UPDATE SET
  clicks = S.clicks,
  impressions = S.impressions,
  sum_position = S.sum_position,
  __insert_date = S.__insert_date,
  file_load_datetime = S.file_load_datetime,
  filename = S.filename

WHEN NOT MATCHED THEN
INSERT (
  account_id,
  account_name,
  site_url,
  date_yyyymmdd,
  date,
  clicks,
  impressions,
  sum_position,
  __insert_date,
  file_load_datetime,
  filename
)
VALUES (
  S.account_id,
  S.account_name,
  S.site_url,
  S.date_yyyymmdd,
  S.date,
  S.clicks,
  S.impressions,
  S.sum_position,
  S.__insert_date,
  S.file_load_datetime,
  S.filename
);
