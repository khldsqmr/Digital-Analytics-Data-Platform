/*
===============================================================================
BRONZE | SA 360 | CAMPAIGN DAILY | BACKFILL (LAST 3 YEARS)
===============================================================================

PURPOSE
- Backfill historical campaign-level Google Ads data
- Covers last 3 years explicitly
- Safe to re-run (MERGE-based)

USAGE
- Manual execution only
- Never scheduled

===============================================================================
*/

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_google_ads_360_campaign_daily` T
USING (
  SELECT
    account_id,
    customer_id,
    campaign_id,
    resource_name,

    CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS date,

    cost_micros,
    impressions,
    clicks,
    all_conversions,

    __insert_date,
    TIMESTAMP(File_Load_datetime) AS file_load_datetime,
    Filename AS filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`
  WHERE
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))
      BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 YEAR)
          AND CURRENT_DATE()
) S
ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date = S.date

WHEN MATCHED THEN
UPDATE SET
  cost_micros = S.cost_micros,
  impressions = S.impressions,
  clicks = S.clicks,
  all_conversions = S.all_conversions,
  __insert_date = S.__insert_date,
  file_load_datetime = S.file_load_datetime,
  filename = S.filename

WHEN NOT MATCHED THEN
INSERT (
  account_id,
  customer_id,
  campaign_id,
  resource_name,
  date_yyyymmdd,
  date,
  cost_micros,
  impressions,
  clicks,
  all_conversions,
  __insert_date,
  file_load_datetime,
  filename
)
VALUES (
  S.account_id,
  S.customer_id,
  S.campaign_id,
  S.resource_name,
  S.date_yyyymmdd,
  S.date,
  S.cost_micros,
  S.impressions,
  S.clicks,
  S.all_conversions,
  S.__insert_date,
  S.file_load_datetime,
  S.filename
);
