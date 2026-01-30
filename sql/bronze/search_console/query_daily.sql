/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | INCREMENTAL MERGE
===============================================================================

PURPOSE
- Ingest raw Google Search Console query-level data
- Preserve maximum source granularity (query + page + search_type)
- Serve as the lowest-level SEO fact table in the platform

GRAIN (Natural Key)
- account_name
- site_url
- page
- query
- search_type
- date

WHY MERGE
- Improvado can re-deliver historical files
- Late-arriving data is common
- MERGE ensures idempotency (no duplicates)

PARTITIONING
- date (DATE)
- Enables partition pruning and low-cost reprocessing

SOURCE
- ds_dbi_improvado_master.google_search_console_query_search_type_tmo

TARGET
- ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily
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
    page,
    query,
    search_type,

    -- Convert YYYYMMDD → DATE (authoritative partition key)
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS date,

    -- Metrics
    clicks,
    impressions,
    position,
    sum_position,

    -- Audit / lineage
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
    position = S.position,
    sum_position = S.sum_position,
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
    S.date,
    S.clicks,
    S.impressions,
    S.position,
    S.sum_position,
    S.__insert_date,
    S.file_load_datetime,
    S.filename
  );
