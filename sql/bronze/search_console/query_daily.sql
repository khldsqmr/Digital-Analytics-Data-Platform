/* =============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | INCREMENTAL MERGE
===============================================================================
Query error: Value of type STRING cannot be assigned to date_yyyymmdd, which has type INT64 at [89:21]
PURPOSE
- Incrementally ingest query-level Search Console data
- Preserve full source granularity (site × page × query × search type × date)
- Handle late-arriving data safely and idempotently

GRAIN
- account_name
- site_url
- page
- query
- search_type
- event_date

PARTITIONING
- Partitioned by event_date (DATE)
- Enables partition pruning and low-cost reprocessing

INCREMENTAL STRATEGY
- MERGE-based upsert
- Rolling lookback window
- Updates existing rows if source files are reloaded

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

    -- Dimensions
    page,
    query,
    search_type,

    -- Canonical event date (materialized DATE)
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS event_date,

    -- Metrics (as delivered by source)
    clicks,
    impressions,
    position,
    sum_position,

    -- Preserve raw date representation
    date_yyyymmdd,

    -- Audit metadata
    __insert_date,
    file_load_datetime,
    filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master
     .google_search_console_query_search_type_tmo`
  WHERE
    -- Partition pruning + late-arriving data handling
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
) S
ON
  -- Full natural business key
  T.account_name = S.account_name
  AND T.site_url = S.site_url
  AND T.page = S.page
  AND T.query = S.query
  AND T.search_type = S.search_type
  AND T.event_date = S.event_date

WHEN MATCHED THEN
  UPDATE SET
    clicks = S.clicks,
    impressions = S.impressions,
    position = S.position,
    sum_position = S.sum_position,
    date_yyyymmdd = S.date_yyyymmdd,
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
    event_date,
    clicks,
    impressions,
    position,
    sum_position,
    date_yyyymmdd,
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
    S.event_date,
    S.clicks,
    S.impressions,
    S.position,
    S.sum_position,
    S.date_yyyymmdd,
    S.__insert_date,
    S.file_load_datetime,
    S.filename
  );
