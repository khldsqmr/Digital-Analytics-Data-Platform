/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS | INCREMENTAL
===============================================================================

PURPOSE
- Raw site-level daily Search Console metrics
- Acts as authoritative site KPI source

GRAIN
- property + site_url + event_date

INCREMENTAL STRATEGY
- MERGE with rolling lookback
- Idempotent and late-data safe

PARTITIONING
- event_date (DATE)

SOURCE
- ds_dbi_improvado_master.google_search_console_site_totals_tmo

TARGET
- ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily` T
USING (
  SELECT
    -- business keys
    account_id    AS property,
    account_name,
    site_url,

    -- dates
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS event_date,
    date_yyyymmdd                                              AS event_date_yyyymmdd,

    -- metrics
    clicks,
    impressions,
    sum_position,
    position,

    -- ingestion metadata
    Filename            AS file_name,
    File_Load_datetime  AS file_load_datetime,
    TIMESTAMP_SECONDS(__insert_date) AS insert_ts,

    -- lineage
    'google_search_console' AS source_system,

    -- deterministic merge key
    TO_HEX(
      MD5(CONCAT(
        account_id,
        site_url,
        CAST(date_yyyymmdd AS STRING)
      ))
    ) AS record_hash

  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_site_totals_tmo`
  WHERE
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
) S
ON T.record_hash = S.record_hash

WHEN MATCHED THEN
  UPDATE SET
    clicks             = S.clicks,
    impressions        = S.impressions,
    sum_position       = S.sum_position,
    position           = S.position,
    file_name          = S.file_name,
    file_load_datetime = S.file_load_datetime,
    insert_ts          = S.insert_ts

WHEN NOT MATCHED THEN
  INSERT (
    property,
    account_name,
    site_url,
    event_date,
    event_date_yyyymmdd,
    clicks,
    impressions,
    sum_position,
    position,
    file_name,
    file_load_datetime,
    insert_ts,
    source_system,
    record_hash
  )
  VALUES (
    S.property,
    S.account_name,
    S.site_url,
    S.event_date,
    S.event_date_yyyymmdd,
    S.clicks,
    S.impressions,
    S.sum_position,
    S.position,
    S.file_name,
    S.file_load_datetime,
    S.insert_ts,
    S.source_system,
    S.record_hash
  );
