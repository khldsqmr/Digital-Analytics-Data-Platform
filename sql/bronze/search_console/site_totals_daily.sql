/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS | INCREMENTAL MERGE
===============================================================================

INCREMENTAL STRATEGY
- MERGE-based upsert
- Rolling INT64 date_yyyymmdd lookback
- Safe for late-arriving files

IMPORTANT
- date_yyyymmdd is INT64 everywhere
- No STRING ↔ INT64 comparisons
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation
 .sdi_bronze_search_console_site_totals_daily` T
USING (
  SELECT
    account_id,
    account_name,
    site_url,

    -- Source-faithful date
    date_yyyymmdd,

    -- Analytics date
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS date,

    clicks,
    impressions,
    sum_position,

    __insert_date,
    File_Load_datetime,
    Filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master
     .google_search_console_site_totals_tmo`
  WHERE
    date_yyyymmdd >=
      CAST(
        FORMAT_DATE(
          '%Y%m%d',
          DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
        ) AS INT64
      )
) S
ON
  T.account_name = S.account_name
  AND T.site_url = S.site_url
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN
  UPDATE SET
    clicks = S.clicks,
    impressions = S.impressions,
    sum_position = S.sum_position,
    __insert_date = S.__insert_date,
    File_Load_datetime = S.File_Load_datetime,
    Filename = S.Filename

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
    File_Load_datetime,
    Filename
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
    S.File_Load_datetime,
    S.Filename
  );
