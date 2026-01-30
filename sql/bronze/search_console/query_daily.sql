/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | INCREMENTAL MERGE
===============================================================================

INCREMENTAL STRATEGY
- MERGE-based upsert
- Full natural grain
- INT64-safe filtering

POSITION
- Exists ONLY in query-level data
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation
 .sdi_bronze_search_console_query_daily` T
USING (
  SELECT
    account_id,
    account_name,
    site_url,
    page,
    query,
    search_type,

    -- Source-faithful date
    date_yyyymmdd,

    -- Analytics date
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS date,

    clicks,
    impressions,
    position,
    sum_position,

    __insert_date,
    File_Load_datetime,
    Filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master
     .google_search_console_query_search_type_tmo`
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
  AND T.page = S.page
  AND T.query = S.query
  AND T.search_type = S.search_type
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN
  UPDATE SET
    clicks = S.clicks,
    impressions = S.impressions,
    position = S.position,
    sum_position = S.sum_position,
    __insert_date = S.__insert_date,
    File_Load_datetime = S.File_Load_datetime,
    Filename = S.Filename

WHEN NOT MATCHED THEN
  INSERT (
    account_id,
    account_name,
    site_url,
    page,
    query,
    search_type,
    date_yyyymmdd,
    date,
    clicks,
    impressions,
    position,
    sum_position,
    __insert_date,
    File_Load_datetime,
    Filename
  )
  VALUES (
    S.account_id,
    S.account_name,
    S.site_url,
    S.page,
    S.query,
    S.search_type,
    S.date_yyyymmdd,
    S.date,
    S.clicks,
    S.impressions,
    S.position,
    S.sum_position,
    S.__insert_date,
    S.File_Load_datetime,
    S.Filename
  );
