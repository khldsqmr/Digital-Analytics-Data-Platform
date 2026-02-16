/*
===============================================================================
BOOTSTRAP | BRONZE | SA360 | CAMPAIGN ENTITY (ONE-TIME)
===============================================================================

SOURCE
------
`prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`

GRAIN
-----
(account_id, campaign_id, date_yyyymmdd)

DATE REQUIREMENT
----------------
Derived DATE from date_yyyymmdd is named `date` (per your instruction).

RAW COLUMN RULE
---------------
Only columns present in your INFORMATION_SCHEMA list are referenced.
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
(
  file_load_datetime DATETIME OPTIONS(description="Raw: File_Load_datetime. ETL ingestion timestamp."),
  filename STRING OPTIONS(description="Raw: Filename. Source file path/name."),

  account_id STRING OPTIONS(description="Raw: account_id."),
  account_name STRING OPTIONS(description="Raw: account_name."),
  customer_id STRING OPTIONS(description="Raw: customer_id."),
  campaign_id STRING OPTIONS(description="Raw: campaign_id."),
  resource_name STRING OPTIONS(description="Raw: resource_name."),
  name STRING OPTIONS(description="Raw: name (campaign name)."),

  date_yyyymmdd STRING OPTIONS(description="Raw: date_yyyymmdd."),
  date DATE OPTIONS(description="DATE derived from date_yyyymmdd (per requirement)."),

  ad_serving_optimization_status STRING OPTIONS(description="Raw: ad_serving_optimization_status."),
  advertising_channel_sub_type STRING OPTIONS(description="Raw: advertising_channel_sub_type."),
  advertising_channel_type STRING OPTIONS(description="Raw: advertising_channel_type."),
  bidding_strategy STRING OPTIONS(description="Raw: bidding_strategy."),
  bidding_strategy_id STRING OPTIONS(description="Raw: bidding_strategy_id."),
  bidding_strategy_system_status STRING OPTIONS(description="Raw: bidding_strategy_system_status."),
  bidding_strategy_type STRING OPTIONS(description="Raw: bidding_strategy_type."),
  campaign_budget STRING OPTIONS(description="Raw: campaign_budget."),
  campaign_priority STRING OPTIONS(description="Raw: campaign_priority."),
  conversion_actions STRING OPTIONS(description="Raw: conversion_actions."),
  creation_time STRING OPTIONS(description="Raw: creation_time."),
  domain_name STRING OPTIONS(description="Raw: domain_name."),
  enable_local STRING OPTIONS(description="Raw: enable_local."),
  end_date STRING OPTIONS(description="Raw: end_date."),
  engine_id STRING OPTIONS(description="Raw: engine_id."),
  excluded_parent_asset_field_types STRING OPTIONS(description="Raw: excluded_parent_asset_field_types."),
  feed_label STRING OPTIONS(description="Raw: feed_label."),
  final_url_suffix STRING OPTIONS(description="Raw: final_url_suffix."),
  frequency_caps STRING OPTIONS(description="Raw: frequency_caps."),
  labels STRING OPTIONS(description="Raw: labels."),
  language_code STRING OPTIONS(description="Raw: language_code."),
  manual_cpa STRING OPTIONS(description="Raw: manual_cpa."),
  manual_cpc_enhanced_cpc_enabled STRING OPTIONS(description="Raw: manual_cpc_enhanced_cpc_enabled."),
  manual_cpm STRING OPTIONS(description="Raw: manual_cpm."),
  max_conv_value_target_roas STRING OPTIONS(description="Raw: max_conv_value_target_roas."),
  max_convs_target_cpa_micros STRING OPTIONS(description="Raw: max_convs_target_cpa_micros."),
  merchant_id STRING OPTIONS(description="Raw: merchant_id."),
  negative_geo_target_type STRING OPTIONS(description="Raw: negative_geo_target_type."),
  opt_in STRING OPTIONS(description="Raw: opt_in."),
  optimization_goal_types STRING OPTIONS(description="Raw: optimization_goal_types."),
  percent_cpc_cpc_bid_ceiling_micros STRING OPTIONS(description="Raw: percent_cpc_cpc_bid_ceiling_micros."),
  percent_cpc_enhanced_cpc_enabled STRING OPTIONS(description="Raw: percent_cpc_enhanced_cpc_enabled."),
  positive_geo_target_type STRING OPTIONS(description="Raw: positive_geo_target_type."),
  sales_country STRING OPTIONS(description="Raw: sales_country."),
  serving_status STRING OPTIONS(description="Raw: serving_status."),
  start_date STRING OPTIONS(description="Raw: start_date."),
  status STRING OPTIONS(description="Raw: status."),
  target_content_network STRING OPTIONS(description="Raw: target_content_network."),
  target_cpa_cpc_bid_ceiling_micros STRING OPTIONS(description="Raw: target_cpa_cpc_bid_ceiling_micros."),
  target_cpa_cpc_bid_floor_micros STRING OPTIONS(description="Raw: target_cpa_cpc_bid_floor_micros."),
  target_cpa_target_cpa_micros STRING OPTIONS(description="Raw: target_cpa_target_cpa_micros."),
  target_cpm STRING OPTIONS(description="Raw: target_cpm."),
  target_google_search STRING OPTIONS(description="Raw: target_google_search."),
  target_imp_share_cpc_bid_ceiling_micros STRING OPTIONS(description="Raw: target_imp_share_cpc_bid_ceiling_micros."),
  target_imp_share_location STRING OPTIONS(description="Raw: target_imp_share_location."),
  target_imp_share_location_fraction_micros STRING OPTIONS(description="Raw: target_imp_share_location_fraction_micros."),
  target_partner_search_network STRING OPTIONS(description="Raw: target_partner_search_network."),
  target_roas_cpc_bid_ceiling_micros STRING OPTIONS(description="Raw: target_roas_cpc_bid_ceiling_micros."),
  target_roas_cpc_bid_floor_micros STRING OPTIONS(description="Raw: target_roas_cpc_bid_floor_micros."),
  target_roas_target_roas STRING OPTIONS(description="Raw: target_roas_target_roas."),
  target_search_network STRING OPTIONS(description="Raw: target_search_network."),
  target_spend_cpc_bid_ceiling_micros STRING OPTIONS(description="Raw: target_spend_cpc_bid_ceiling_micros."),
  target_spend_micros STRING OPTIONS(description="Raw: target_spend_micros."),
  tracking_url STRING OPTIONS(description="Raw: tracking_url."),
  tracking_url_template STRING OPTIONS(description="Raw: tracking_url_template."),
  url_custom_parameters STRING OPTIONS(description="Raw: url_custom_parameters."),
  url_expansion_opt_out STRING OPTIONS(description="Raw: url_expansion_opt_out."),
  use_supplied_urls_only STRING OPTIONS(description="Raw: use_supplied_urls_only."),
  use_vehicle_inventory STRING OPTIONS(description="Raw: use_vehicle_inventory."),

  bronze_updated_at TIMESTAMP OPTIONS(description="System timestamp when row was inserted/updated in Bronze.")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id;
