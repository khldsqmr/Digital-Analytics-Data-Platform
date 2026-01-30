/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | INCREMENTAL
===============================================================================

PURPOSE
- Incrementally loads query-level Search Console data
- Handles late-arriving files safely
- Ensures ONE row per natural grain per day

INCREMENTAL STRATEGY
- MERGE-based upsert
- Rolling lookback window
- Idempotent by design

WHY MERGE?
- Prevents duplicates
- Allows safe reprocessing
- Supports upstream file reloads
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation
 .sdi_bronze_search_console_query_daily` T
USING (
  SELECT
    -- Identifiers
    account_id,
    account_name,
    site_url,

    -- Dimensions
    page,
    query,
    search_type,

    -- Convert YYYYMMDD → DATE
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS date,

    -- Metrics
    clicks,
    impressions,
    sum_position,
    position,

    -- Audit fields
    __insert_date,
    file_load_datetime,
    filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master
     .google_search_console_query_search_type_tmo`
  WHERE
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
) S
ON
  T.account_name = S.account_name
  AND T.site_url = S.site_url
  AND T.page = S.page
  AND T.query = S.query
  AND T.search_type = S.search_type
  AND T.date = S.date

WHEN MATCHED THEN
  UPDATE SET
    clicks = S.clicks,
    impressions = S.impressions,
    sum_position = S.sum_position,
    position = S.position,
    __insert_date = S.__insert_date,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename

WHEN NOT MATCHED THEN
  INSERT (
    account_id,
    account_name,
    site_url,
    page,
    query,
    search_type,
    date,
    clicks,
    impressions,
    sum_position,
    position,
    __insert_date,
    file_load_datetime,
    filename
  )
  VALUES (
    S.account_id,
    S.account_name,
    S.site_url,
    S.page,
    S.query,
    S.search_type,
    S.date,
    S.clicks,
    S.impressions,
    S.sum_position,
    S.position,
    S.__insert_date,
    S.file_load_datetime,
    S.filename
  );
