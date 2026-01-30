/* =============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | QUERY LEVEL | INCREMENTAL MERGE
===============================================================================
Query error: No matching signature for operator >= for argument types: STRING, INT64 Signature: T1 >= T1 Unable to find common supertype for templated argument <T1> Input types for <T1>: {INT64, STRING} at [30:5] at [37:1]

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
    account_id,
    account_name,
    site_url,
    page,
    query,
    search_type,

    -- Source-faithful integer date
    date_yyyymmdd,

    -- Analytics date
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS date,

    clicks,
    impressions,
    position,
    sum_position,

    -- Audit metadata
    __insert_date,
    File_Load_datetime,
    Filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master
     .google_search_console_query_search_type_tmo`
  WHERE
    date_yyyymmdd >= CAST(
      FORMAT_DATE('%Y%m%d',
        DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
      ) AS INT64
    )
) S
ON
  T.account_name     = S.account_name
  AND T.site_url    = S.site_url
  AND T.page        = S.page
  AND T.query       = S.query
  AND T.search_type = S.search_type
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN
  UPDATE SET
    clicks            = S.clicks,
    impressions       = S.impressions,
    position          = S.position,
    sum_position      = S.sum_position,
    __insert_date     = S.__insert_date,
    File_Load_datetime = S.File_Load_datetime,
    Filename          = S.Filename

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
