/*
===============================================================================
BOOTSTRAP | BRONZE | SA 360 | CAMPAIGN DAILY (ONE-TIME)
===============================================================================

PURPOSE
- Creates Bronze campaign-level daily performance table
- One row per account + campaign + date
- Executed ONCE per environment

SOURCE
- Improvado Google Ads 360 Campaign Daily

GRAIN
account_id + campaign_id + date

NOTES
- cost remains in micros (no currency conversion in Bronze)
- date_yyyymmdd preserved for lineage
===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_google_ads_360_campaign_daily`
(
  account_id STRING OPTIONS (description = 'Google Ads account identifier.'),
  customer_id STRING OPTIONS (description = 'Google Ads customer identifier.'),
  campaign_id STRING OPTIONS (description = 'Unique campaign identifier.'),
  resource_name STRING OPTIONS (description = 'Google Ads API resource name.'),

  date_yyyymmdd STRING OPTIONS (
    description = 'Source date in YYYYMMDD format preserved for lineage.'
  ),

  date DATE OPTIONS (
    description = 'Derived analytics date used for partitioning and joins.'
  ),

  cost_micros INT64 OPTIONS (
    description = 'Raw cost in micros (1e6 = 1 currency unit).'
  ),

  impressions INT64 OPTIONS (
    description = 'Total impressions for the campaign on the given day.'
  ),

  clicks INT64 OPTIONS (
    description = 'Total clicks for the campaign on the given day.'
  ),

  all_conversions FLOAT64 OPTIONS (
    description = 'Total conversions attributed to the campaign.'
  ),

  __insert_date INT64 OPTIONS (
    description = 'Epoch timestamp when record was ingested.'
  ),

  file_load_datetime TIMESTAMP OPTIONS (
    description = 'Timestamp when the source file was loaded.'
  ),

  filename STRING OPTIONS (
    description = 'Source filename delivered by ingestion pipeline.'
  )
)
PARTITION BY date
CLUSTER BY account_id, campaign_id
OPTIONS (
  description = 'Bronze Google Ads 360 campaign-level daily metrics.'
);
