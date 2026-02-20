/*
===============================================================================
FILE: 00_create_sdi_bronze_sa360_campaign_entity.sql
LAYER: Bronze
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
TABLE:   sdi_bronze_sa360_campaign_entity

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo

PURPOSE:
  Create a Bronze campaign entity/settings snapshot:
    - Canonical DATE parsed from date_yyyymmdd
    - Rename name -> campaign_name
    - Partition + cluster for efficient AS-OF joins to daily fact

PARTITION / CLUSTER:
  PARTITION BY date
  CLUSTER BY account_id, campaign_id
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
(
  -- =====================================================================
  -- GRAIN KEYS (SETTINGS SNAPSHOT)
  -- =====================================================================
  account_id STRING OPTIONS(description="Entity account ID (raw: account_id)."),
  campaign_id STRING OPTIONS(description="Entity campaign ID (raw: campaign_id)."),
  date_yyyymmdd STRING OPTIONS(description="Snapshot date in YYYYMMDD (raw: date_yyyymmdd)."),
  date DATE OPTIONS(description="Canonical DATE parsed from date_yyyymmdd. Partition key."),

  -- =====================================================================
  -- CORE DIMENSIONS / SETTINGS
  -- =====================================================================
  account_name STRING OPTIONS(description="Account name (raw: account_name)."),
  customer_id STRING OPTIONS(description="Customer ID (raw: customer_id)."),

  ad_serving_optimization_status STRING OPTIONS(description="Ad serving optimization status (raw: ad_serving_optimization_status)."),
  advertising_channel_sub_type STRING OPTIONS(description="Advertising channel sub type (raw: advertising_channel_sub_type)."),
  advertising_channel_type STRING OPTIONS(description="Advertising channel type (raw: advertising_channel_type)."),

  bidding_strategy STRING OPTIONS(description="Bidding strategy (raw: bidding_strategy)."),
  bidding_strategy_id STRING OPTIONS(description="Bidding strategy id (raw: bidding_strategy_id)."),
  bidding_strategy_system_status STRING OPTIONS(description="Bidding strategy system status (raw: bidding_strategy_system_status)."),
  bidding_strategy_type STRING OPTIONS(description="Bidding strategy type (raw: bidding_strategy_type)."),

  campaign_budget STRING OPTIONS(description="Campaign budget (raw: campaign_budget)."),
  campaign_priority STRING OPTIONS(description="Campaign priority (raw: campaign_priority)."),
  conversion_actions STRING OPTIONS(description="Conversion actions (raw: conversion_actions)."),
  creation_time STRING OPTIONS(description="Creation time (raw: creation_time)."),

  domain_name STRING OPTIONS(description="Domain name (raw: domain_name)."),
  enable_local STRING OPTIONS(description="Enable local flag (raw: enable_local)."),
  end_date STRING OPTIONS(description="End date (raw: end_date)."),
  engine_id STRING OPTIONS(description="Engine ID (raw: engine_id)."),
  excluded_parent_asset_field_types STRING OPTIONS(description="Excluded parent asset field types (raw: excluded_parent_asset_field_types)."),
  feed_label STRING OPTIONS(description="Feed label (raw: feed_label)."),
  final_url_suffix STRING OPTIONS(description="Final URL suffix (raw: final_url_suffix)."),
  frequency_caps STRING OPTIONS(description="Frequency caps (raw: frequency_caps)."),
  labels STRING OPTIONS(description="Labels (raw: labels)."),
  language_code STRING OPTIONS(description="Language code (raw: language_code)."),

  manual_cpa STRING OPTIONS(description="Manual CPA (raw: manual_cpa)."),
  manual_cpc_enhanced_cpc_enabled STRING OPTIONS(description="Manual CPC enhanced flag (raw: manual_cpc_enhanced_cpc_enabled)."),
  manual_cpm STRING OPTIONS(description="Manual CPM (raw: manual_cpm)."),

  max_convs_target_cpa_micros STRING OPTIONS(description="Max convs target CPA micros (raw: max_convs_target_cpa_micros)."),
  max_conv_value_target_roas STRING OPTIONS(description="Max conv value target ROAS (raw: max_conv_value_target_roas)."),

  merchant_id STRING OPTIONS(description="Merchant ID (raw: merchant_id)."),
  campaign_name STRING OPTIONS(description="Campaign name (raw: name)."),

  negative_geo_target_type STRING OPTIONS(description="Negative geo target type (raw: negative_geo_target_type)."),
  optimization_goal_types STRING OPTIONS(description="Optimization goal types (raw: optimization_goal_types)."),
  opt_in STRING OPTIONS(description="Opt-in (raw: opt_in)."),

  percent_cpc_cpc_bid_ceiling_micros STRING OPTIONS(description="Percent CPC bid ceiling micros (raw: percent_cpc_cpc_bid_ceiling_micros)."),
  percent_cpc_enhanced_cpc_enabled STRING OPTIONS(description="Percent CPC enhanced flag (raw: percent_cpc_enhanced_cpc_enabled)."),

  positive_geo_target_type STRING OPTIONS(description="Positive geo target type (raw: positive_geo_target_type)."),
  resource_name STRING OPTIONS(description="Resource name (raw: resource_name)."),
  sales_country STRING OPTIONS(description="Sales country (raw: sales_country)."),
  serving_status STRING OPTIONS(description="Serving status (raw: serving_status)."),
  start_date STRING OPTIONS(description="Start date (raw: start_date)."),
  status STRING OPTIONS(description="Status (raw: status)."),

  target_content_network STRING OPTIONS(description="Target content network (raw: target_content_network)."),
  target_cpa_cpc_bid_ceiling_micros STRING OPTIONS(description="Target CPA CPC bid ceiling micros (raw: target_cpa_cpc_bid_ceiling_micros)."),
  target_cpa_cpc_bid_floor_micros STRING OPTIONS(description="Target CPA CPC bid floor micros (raw: target_cpa_cpc_bid_floor_micros)."),
  target_cpa_target_cpa_micros STRING OPTIONS(description="Target CPA micros (raw: target_cpa_target_cpa_micros)."),
  target_cpm STRING OPTIONS(description="Target CPM (raw: target_cpm)."),
  target_google_search STRING OPTIONS(description="Target Google search (raw: target_google_search)."),

  target_imp_share_cpc_bid_ceiling_micros STRING OPTIONS(description="Target imp share CPC bid ceiling micros (raw: target_imp_share_cpc_bid_ceiling_micros)."),
  target_imp_share_location STRING OPTIONS(description="Target imp share location (raw: target_imp_share_location)."),
  target_imp_share_location_fraction_micros STRING OPTIONS(description="Target imp share location fraction micros (raw: target_imp_share_location_fraction_micros)."),

  target_partner_search_network STRING OPTIONS(description="Target partner search network (raw: target_partner_search_network)."),

  target_roas_cpc_bid_ceiling_micros STRING OPTIONS(description="Target ROAS CPC bid ceiling micros (raw: target_roas_cpc_bid_ceiling_micros)."),
  target_roas_cpc_bid_floor_micros STRING OPTIONS(description="Target ROAS CPC bid floor micros (raw: target_roas_cpc_bid_floor_micros)."),
  target_roas_target_roas STRING OPTIONS(description="Target ROAS (raw: target_roas_target_roas)."),

  target_search_network STRING OPTIONS(description="Target search network (raw: target_search_network)."),

  target_spend_cpc_bid_ceiling_micros STRING OPTIONS(description="Target spend CPC bid ceiling micros (raw: target_spend_cpc_bid_ceiling_micros)."),
  target_spend_micros STRING OPTIONS(description="Target spend micros (raw: target_spend_micros)."),

  tracking_url STRING OPTIONS(description="Tracking URL (raw: tracking_url)."),
  tracking_url_template STRING OPTIONS(description="Tracking URL template (raw: tracking_url_template)."),
  url_custom_parameters STRING OPTIONS(description="URL custom parameters (raw: url_custom_parameters)."),
  url_expansion_opt_out STRING OPTIONS(description="URL expansion opt out (raw: url_expansion_opt_out)."),
  use_supplied_urls_only STRING OPTIONS(description="Use supplied URLs only (raw: use_supplied_urls_only)."),
  use_vehicle_inventory STRING OPTIONS(description="Use vehicle inventory (raw: use_vehicle_inventory)."),

  -- =====================================================================
  -- INGESTION / LINEAGE
  -- =====================================================================
  file_load_datetime DATETIME OPTIONS(description="Ingestion load timestamp (raw: File_Load_datetime)."),
  filename STRING OPTIONS(description="Source file name/path (raw: Filename).")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id
OPTIONS(
  description = "Bronze SA360 campaign entity/settings snapshot. Cleaned naming, partitioned by canonical date."
);
