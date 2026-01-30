/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS | INCREMENTAL MERGE
===============================================================================

PURPOSE
- Ingest site-level daily Search Console metrics
- Acts as the parent aggregate for query-level data
- Used for executive SEO KPIs and trend analysis

GRAIN (Natural Key)
- account_name
- site_url
- date

WHY MERGE
- Handles file re-delivery and corrections
- Guarantees one record per site per day

PARTITIONING
- date (DATE)
- Efficient reprocessing of recent days only

SOURCE
- ds_dbi_improvado_master.google_search_console_site_totals_tmo

TARGET
- ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation
 .sdi_bronze_search_console_site_totals_daily` T
USING (
  SELECT
    -- Identifiers
    account_id,
    account_name,
    site_url,

    -- Convert YYYYMMDD → DATE
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
     .google_search_console_site_totals_tmo`
  WHERE
    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
) S
ON
  T.account_name = S.account_name
  AND T.site_url = S.site_url
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
    S.date,
    S.clicks,
    S.impressions,
    S.position,
    S.sum_position,
    S.__insert_date,
    S.file_load_datetime,
    S.filename
  );
