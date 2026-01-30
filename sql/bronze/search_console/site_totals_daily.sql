/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS | INCREMENTAL MERGE
===============================================================================

PURPOSE
- Ingest site-level daily Search Console metrics
- Acts as the parent aggregate for query-level data
- Used for executive SEO KPIs and trend analysis

STRATEGY
- Merge on (date_yyyymmdd + site_url)
- Update metrics if source reloads
- Insert new dates

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
    account_id,
    account_name,
    site_url,

    -- Materialized partition column
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS event_date,

    clicks,
    impressions,
    sum_position,

    date_yyyymmdd,
    __insert_date,
    file_load_datetime,
    filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master
     .google_search_console_site_totals_tmo`
  WHERE
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
) S
ON
  T.account_name = S.account_name
  AND T.site_url = S.site_url
  AND T.event_date = S.event_date

WHEN MATCHED THEN
  UPDATE SET
    clicks = S.clicks,
    impressions = S.impressions,
    sum_position = S.sum_position,
    date_yyyymmdd = S.date_yyyymmdd,
    __insert_date = S.__insert_date,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename

WHEN NOT MATCHED THEN
  INSERT VALUES (
    S.account_id,
    S.account_name,
    S.site_url,
    S.event_date,
    S.clicks,
    S.impressions,
    S.sum_position,
    S.date_yyyymmdd,
    S.__insert_date,
    S.file_load_datetime,
    S.filename
  );
