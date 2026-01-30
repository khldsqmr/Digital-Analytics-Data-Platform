/*
===============================================================================
BRONZE | GOOGLE SEARCH CONSOLE | SITE TOTALS | INCREMENTAL
===============================================================================

PURPOSE
- Incrementally loads site-level Search Console metrics
- Maintains exactly one record per site per day

INCREMENTAL STRATEGY
- MERGE with rolling lookback window
- Safe handling of late-arriving and reloaded data

WHY THIS TABLE MATTERS
- Parent aggregate for query-level detail
- Used for executive KPIs and trend analysis
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

    DATE(PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))) AS date,

    clicks,
    impressions,
    sum_position,
    position,

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
