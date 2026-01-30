/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS | INCREMENTAL
===============================================================================

TABLE PURPOSE
- Bronze (raw) ingestion table for Google Search Console site-level metrics
- Data source: Improvado
- Grain: one row per site per day

WHY BRONZE EXISTS
- Preserve source data exactly as delivered
- Enable reproducible downstream logic
- Act as an auditable, immutable foundation

INCREMENTAL STRATEGY
- MERGE-based upsert
- Rolling lookback window to capture late-arriving files
- Idempotent: same data can be safely reprocessed

WHY MERGE (NOT INSERT)
- INSERT-only would duplicate rows if Improvado reloads files
- MERGE guarantees one record per grain per day

PARTITIONING STRATEGY
- Partitioned by `date`
- BigQuery scans only relevant partitions
- Reduces cost and improves performance
- Allows efficient reprocessing of recent days only

SOURCE TABLE
- prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_site_totals_tmo

TARGET TABLE
- prj-dbi-prd-1.ds_dbi_digitalmedia_automation
    .sdi_bronze_search_console_site_totals_daily
===============================================================================
*/

-- Number of recent days to reprocess
DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation
 .sdi_bronze_search_console_site_totals_daily` T
USING (
  SELECT
    account_id,
    account_name,
    site_url,

    -- Convert YYYYMMDD integer to DATE
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS date,

    clicks,
    impressions,
    sum_position,
    position,

    -- Metadata for auditability
    __insert_date,
    file_load_datetime,
    filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master
     .google_search_console_site_totals_tmo`
  WHERE
    -- Partition pruning: only scan recent days
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
) S
ON
  -- Natural business key
  T.account_name = S.account_name
  AND T.site_url = S.site_url
  AND T.date = S.date

WHEN MATCHED THEN
  -- Update metrics if data for the same day is reloaded
  UPDATE SET
    clicks = S.clicks,
    impressions = S.impressions,
    sum_position = S.sum_position,
    position = S.position,
    __insert_date = S.__insert_date,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename

WHEN NOT MATCHED THEN
  -- Insert new daily records
  INSERT (
    account_id,
    account_name,
    site_url,
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
    S.date,
    S.clicks,
    S.impressions,
    S.sum_position,
    S.position,
    S.__insert_date,
    S.file_load_datetime,
    S.filename
  );
