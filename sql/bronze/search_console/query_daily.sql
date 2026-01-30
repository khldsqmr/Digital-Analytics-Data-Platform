/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | INCREMENTAL
===============================================================================

TABLE PURPOSE
- Bronze ingestion table for query-level Search Console data
- Includes page and search_type granularity
- Grain:
  account + site + page + query + search_type + date

WHY THIS TABLE EXISTS
- Enables deep SEO analysis (queries, pages, intent)
- Preserves full source granularity
- Feeds Silver enrichment and Gold KPIs

INCREMENTAL STRATEGY
- MERGE-based upsert
- Rolling lookback window
- Safe handling of late-arriving files

PARTITIONING
- Partitioned by `date`
- Limits scan cost
- Improves MERGE performance

SOURCE TABLE
- prj-dbi-prd-1.ds_dbi_improvado_master
    .google_search_console_query_search_type_tmo

TARGET TABLE
- prj-dbi-prd-1.ds_dbi_digitalmedia_automation
    .sdi_bronze_search_console_query_daily
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

    -- Convert YYYYMMDD to DATE
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS date,

    clicks,
    impressions,
    sum_position,
    position,

    -- Audit metadata
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
  -- Full natural grain
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
