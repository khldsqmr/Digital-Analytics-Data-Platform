/*
===============================================================================
BOOTSTRAP | BRONZE | AD MEDIA | DAILY CAMPAIGN METRICS (ONE-TIME)
===============================================================================

PURPOSE
- Creates the physical Bronze table for paid media daily performance
- Executed ONCE per environment
- Must NOT be re-run after creation

SOURCE
- Improvado ps_admedia_daily_tmo

GRAIN
- account + campaign_id + date

===============================================================================
*/

CREATE TABLE IF NOT EXISTS
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_admedia_daily`
(
  account_id STRING OPTIONS (
    description = 'Source system identifier for the ad account.'
  ),

  account_name STRING OPTIONS (
    description = 'Human-readable ad account name.'
  ),

  campaign STRING OPTIONS (
    description = 'Campaign name as defined in the ad platform.'
  ),

  campaign_id STRING OPTIONS (
    description = 'Unique campaign identifier from the ad platform.'
  ),

  -- Source date preserved for lineage
  date_yyyymmdd STRING OPTIONS (
    description = 'Source date in YYYYMMDD format.'
  ),

  -- Analytics date
  date DATE OPTIONS (
    description = 'Derived date used for partitioning and joins.'
  ),

  clicks FLOAT64 OPTIONS (
    description = 'Number of ad clicks.'
  ),

  impressions FLOAT64 OPTIONS (
    description = 'Number of ad impressions.'
  ),

  conversions FLOAT64 OPTIONS (
    description = 'Number of conversions attributed to the campaign.'
  ),

  spend FLOAT64 OPTIONS (
    description = 'Total media spend for the campaign on the given day.'
  ),

  __insert_date INT64 OPTIONS (
    description = 'Epoch timestamp when the record was ingested.'
  ),

  file_load_datetime TIMESTAMP OPTIONS (
    description = 'Timestamp when the source file was loaded.'
  ),

  filename STRING OPTIONS (
    description = 'Source filename delivered by ingestion pipeline.'
  )
)
PARTITION BY date
CLUSTER BY account_name, campaign_id
OPTIONS (
  description = 'Bronze paid media daily campaign performance. One row per campaign per day.'
);
