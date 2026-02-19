/*
===============================================================================
FILE: 00_create_sdi_gold_sa360_campaign_qgp_week_long.sql
LAYER: Gold
TABLE: sdi_gold_sa360_campaign_qgp_week_long

SOURCE:
  Gold QGP Week Wide: sdi_gold_sa360_campaign_weekly (UPDATED to qgp_week design)

PURPOSE:
  Tableau-ready LONG fact at QGP_WEEK grain:
    - one row per (account_id, campaign_id, qgp_week, metric_name)
    - qgp_week can be:
        * Saturday week ending
        * Quarter-end date for quarter-end partial

GRAIN:
  (account_id, campaign_id, qgp_week, metric_name)

PARTITION / CLUSTER:
  PARTITION BY qgp_week
  CLUSTER BY lob, account_id, campaign_id, metric_name
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_gold_sa360_campaign_qgp_week_long`
(
  -- Keys
  account_id STRING,
  campaign_id STRING,
  qgp_week DATE,
  qgp_week_yyyymmdd STRING,
  period_type STRING,  -- WEEKLY | QUARTER_END_PARTIAL

  -- Dimensions
  lob STRING,
  ad_platform STRING,
  account_name STRING,
  campaign_name STRING,
  campaign_type STRING,

  advertising_channel_type STRING,
  advertising_channel_sub_type STRING,
  bidding_strategy_type STRING,
  serving_status STRING,

  -- Long format fields
  metric_name STRING,
  metric_value FLOAT64,

  -- Lineage
  gold_qgp_long_inserted_at TIMESTAMP
)
PARTITION BY qgp_week
CLUSTER BY lob, account_id, campaign_id, metric_name
OPTIONS(description="Gold SA360 QGP_WEEK LONG fact for Tableau. Wide metrics unpivoted into metric_name/metric_value.");
