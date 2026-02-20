/*
===============================================================================
BRONZE | AD MEDIA | DAILY CAMPAIGN METRICS | INCREMENTAL MERGE
===============================================================================

GRAIN
- account + campaign_id + date

STRATEGY
- Rolling lookback window
- Idempotent MERGE
- Handles late-arriving files safely

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_admedia_daily` T
USING (
  SELECT
    account_id,
    account_name,
    campaign,
    CAST(campaign_id AS STRING) AS campaign_id,

    CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS date,

    clicks,
    impressions,
    conversions,
    spend_ AS spend,

    __insert_date,
    TIMESTAMP(File_Load_datetime) AS file_load_datetime,
    Filename AS filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master.ps_admedia_daily_tmo`
  WHERE
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))
      >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
) S
ON
  T.account_name = S.account_name
  AND T.campaign_id = S.campaign_id
  AND T.date = S.date

WHEN MATCHED THEN
UPDATE SET
  campaign = S.campaign,
  clicks = S.clicks,
  impressions = S.impressions,
  conversions = S.conversions,
  spend = S.spend,
  __insert_date = S.__insert_date,
  file_load_datetime = S.file_load_datetime,
  filename = S.filename

WHEN NOT MATCHED THEN
INSERT (
  account_id,
  account_name,
  campaign,
  campaign_id,
  date_yyyymmdd,
  date,
  clicks,
  impressions,
  conversions,
  spend,
  __insert_date,
  file_load_datetime,
  filename
)
VALUES (
  S.account_id,
  S.account_name,
  S.campaign,
  S.campaign_id,
  S.date_yyyymmdd,
  S.date,
  S.clicks,
  S.impressions,
  S.conversions,
  S.spend,
  S.__insert_date,
  S.file_load_datetime,
  S.filename
);
