/*
===============================================================================
BOOTSTRAP | BRONZE | SA 360 | CAMPAIGN ENTITY (ONE-TIME)
===============================================================================

PURPOSE
- Creates Bronze campaign configuration snapshot table
- One row per account + campaign + file load
- Executed ONCE per environment

SOURCE
- Improvado Google Ads 360 Campaign Entity

GRAIN
account_id + campaign_id + file_load_datetime

NOTES
- All micros fields preserved as INT64 (no currency conversion in Bronze)
- date_yyyymmdd preserved for lineage
- JSON-like fields stored as STRING (no parsing in Bronze)
===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_google_ads_360_campaign_entity`
(
  account_id STRING OPTIONS (
    description = 'Google Ads account identifier.'
  ),

  customer_id STRING OPTIONS (
    description = 'Google Ads customer identifier.'
  ),

  campaign_id STRING OPTIONS (
    description = 'Unique campaign identifier.'
  ),

  resource_name STRING OPTIONS (
    description = 'Google Ads API resource name.'
  ),

  campaign_name STRING OPTIONS (
    description = 'Campaign name from source system.'
  ),

  advertising_channel_type STRING OPTIONS (
    description = 'Primary advertising channel type.'
  ),

  advertising_channel_sub_type STRING OPTIONS (
    description = 'Advertising channel sub-type.'
  ),

  bidding_strategy_type STRING OPTIONS (
    description = 'Bidding strategy type applied to campaign.'
  ),

  status STRING OPTIONS (
    description = 'Campaign status.'
  ),

  serving_status STRING OPTIONS (
    description = 'Serving status of campaign.'
  ),

  start_date DATE OPTIONS (
    description = 'Campaign start date.'
  ),

  end_date DATE OPTIONS (
    description = 'Campaign end date.'
  ),

  creation_time TIMESTAMP OPTIONS (
    description = 'Timestamp when campaign was created.'
  ),

  target_cpa_target_cpa_micros INT64 OPTIONS (
    description = 'Target CPA in micros.'
  ),

  target_spend_micros INT64 OPTIONS (
    description = 'Target spend in micros.'
  ),

  percent_cpc_cpc_bid_ceiling_micros INT64 OPTIONS (
    description = 'CPC bid ceiling in micros.'
  ),

  enable_local BOOL OPTIONS (
    description = 'Boolean flag indicating local campaign.'
  ),

  opt_in BOOL OPTIONS (
    description = 'Boolean opt-in flag.'
  ),

  labels STRING OPTIONS (
    description = 'Campaign labels preserved as raw string.'
  ),

  url_custom_parameters STRING OPTIONS (
    description = 'URL custom parameters preserved as raw string.'
  ),

  date_yyyymmdd STRING OPTIONS (
    description = 'Source snapshot date in YYYYMMDD format.'
  ),

  __insert_date INT64 OPTIONS (
    description = 'Epoch timestamp when record was ingested.'
  ),

  file_load_datetime TIMESTAMP OPTIONS (
    description = 'Timestamp when source file was loaded.'
  ),

  filename STRING OPTIONS (
    description = 'Source filename delivered by ingestion pipeline.'
  )
)
PARTITION BY DATE(file_load_datetime)
CLUSTER BY account_id, campaign_id
OPTIONS (
  description = 'Bronze Google Ads 360 campaign configuration snapshot table.'
);
