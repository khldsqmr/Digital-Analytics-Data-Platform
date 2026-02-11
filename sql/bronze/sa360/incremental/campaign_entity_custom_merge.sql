/*
===============================================================================
BRONZE | SA 360 | CAMPAIGN ENTITY | INCREMENTAL MERGE
===============================================================================

GRAIN
account_id + campaign_id + file_load_datetime

NOTES
- 7-day lookback window for late-arriving files
- Safe for daily scheduling
- Snapshot-style ingestion

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_google_ads_360_campaign_entity` T
USING (
  SELECT
    account_id,
    customer_id,
    campaign_id,
    resource_name,
    name AS campaign_name,
    advertising_channel_type,
    advertising_channel_sub_type,
    bidding_strategy_type,
    status,
    serving_status,
    SAFE_CAST(start_date AS DATE) AS start_date,
    SAFE_CAST(end_date AS DATE) AS end_date,
    SAFE_CAST(creation_time AS TIMESTAMP) AS creation_time,
    target_cpa_target_cpa_micros,
    target_spend_micros,
    percent_cpc_cpc_bid_ceiling_micros,
    SAFE_CAST(enable_local AS BOOL) AS enable_local,
    SAFE_CAST(opt_in AS BOOL) AS opt_in,
    labels,
    url_custom_parameters,
    CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
    __insert_date,
    TIMESTAMP(File_Load_datetime) AS file_load_datetime,
    Filename AS filename
  FROM
    `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
  WHERE
    TIMESTAMP(File_Load_datetime)
      >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)
) S
ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.file_load_datetime = S.file_load_datetime

WHEN MATCHED THEN
UPDATE SET
  campaign_name = S.campaign_name,
  advertising_channel_type = S.advertising_channel_type,
  advertising_channel_sub_type = S.advertising_channel_sub_type,
  bidding_strategy_type = S.bidding_strategy_type,
  status = S.status,
  serving_status = S.serving_status,
  start_date = S.start_date,
  end_date = S.end_date,
  creation_time = S.creation_time,
  target_cpa_target_cpa_micros = S.target_cpa_target_cpa_micros,
  target_spend_micros = S.target_spend_micros,
  percent_cpc_cpc_bid_ceiling_micros = S.percent_cpc_cpc_bid_ceiling_micros,
  enable_local = S.enable_local,
  opt_in = S.opt_in,
  labels = S.labels,
  url_custom_parameters = S.url_custom_parameters,
  date_yyyymmdd = S.date_yyyymmdd,
  __insert_date = S.__insert_date,
  filename = S.filename

WHEN NOT MATCHED THEN
INSERT (
  account_id,
  customer_id,
  campaign_id,
  resource_name,
  campaign_name,
  advertising_channel_type,
  advertising_channel_sub_type,
  bidding_strategy_type,
  status,
  serving_status,
  start_date,
  end_date,
  creation_time,
  target_cpa_target_cpa_micros,
  target_spend_micros,
  percent_cpc_cpc_bid_ceiling_micros,
  enable_local,
  opt_in,
  labels,
  url_custom_parameters,
  date_yyyymmdd,
  __insert_date,
  file_load_datetime,
  filename
)
VALUES (
  S.account_id,
  S.customer_id,
  S.campaign_id,
  S.resource_name,
  S.campaign_name,
  S.advertising_channel_type,
  S.advertising_channel_sub_type,
  S.bidding_strategy_type,
  S.status,
  S.serving_status,
  S.start_date,
  S.end_date,
  S.creation_time,
  S.target_cpa_target_cpa_micros,
  S.target_spend_micros,
  S.percent_cpc_cpc_bid_ceiling_micros,
  S.enable_local,
  S.opt_in,
  S.labels,
  S.url_custom_parameters,
  S.date_yyyymmdd,
  S.__insert_date,
  S.file_load_datetime,
  S.filename
);
