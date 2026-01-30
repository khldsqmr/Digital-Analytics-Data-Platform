/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | INCREMENTAL MERGE
===============================================================================

GRAIN
account + site + page + query + search_type + date

INCREMENTAL STRATEGY
- Rolling lookback window
- MERGE ensures idempotency
- Late-arriving files handled safely

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily` T
USING (
  SELECT
    account_id,
    account_name,
    site_url,
    page,
    query,
    search_type,

    -- Dates
    date_yyyymmdd,
    DATE(PARSE_DATE('%Y%m%d', date_yyyymmdd)) AS date,

    -- Metrics
    clicks,
    impressions,
    position,
    sum_position,

    -- Audit
    __insert_date,
    file_load_datetime,
    filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_query_search_type_tmo`
  WHERE
    DATE(PARSE_DATE('%Y%m%d', date_yyyymmdd))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
) S
ON
  T.account_name   = S.account_name
  AND T.site_url   = S.site_url
  AND T.page       = S.page
  AND T.query      = S.query
  AND T.search_type= S.search_type
  AND T.date       = S.date

WHEN MATCHED THEN
  UPDATE SET
    clicks             = S.clicks,
    impressions        = S.impressions,
    position           = S.position,
    sum_position       = S.sum_position,
    __insert_date      = S.__insert_date,
    file_load_datetime = S.file_load_datetime,
    filename           = S.filename

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
    S.date_yyyymmdd,
    S.date,
    S.clicks,
    S.impressions,
    S.position,
    S.sum_position,
    S.__insert_date,
    S.file_load_datetime,
    S.filename
  );
