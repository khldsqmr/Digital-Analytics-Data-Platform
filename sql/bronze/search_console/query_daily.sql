/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | INCREMENTAL MERGE
===============================================================================

PURPOSE
- Ingest raw Google Search Console query-level data
- Preserve maximum source granularity (query + page + search_type)
- Serve as the lowest-level SEO fact table in the platform

STRATEGY
- Merge on full natural grain
- Preserves raw diagnostic metrics

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

-- ============================================================
-- INCREMENTAL MERGE: Search Console Query + Search Type (Bronze)
--

-- ============================================================

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily` T
USING
(
  SELECT
    account_id,
    account_name,
    site_url,
    page,
    query,
    search_type,
    date,
    date_yyyymmdd,
    clicks,
    impressions,
    position,
    sum_position,
    __insert_date,
    File_Load_datetime,
    Filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_query_search_type_tmo`
) S
ON
  T.site_url = S.site_url
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
    date,
    date_yyyymmdd,
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
    S.date,
    S.date_yyyymmdd,
    S.clicks,
    S.impressions,
    S.position,
    S.sum_position,
    S.__insert_date,
    S.File_Load_datetime,
    S.Filename
  );
