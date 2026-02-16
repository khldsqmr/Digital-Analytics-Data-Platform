/*
===============================================================================
BOOTSTRAP | BRONZE | SA360 | CAMPAIGN ENTITY (NORMALIZED SNAPSHOT)
===============================================================================

PURPOSE
-------
Create normalized Bronze Campaign Entity snapshot table.

SOURCE
------
google_search_ads_360_beta_campaign_entity_custom_tmo

GRAIN
-----
campaign_id + date_yyyymmdd (daily snapshot)

PARTITION
---------
snapshot_date (parsed from date_yyyymmdd)

CLUSTER
-------
account_id, campaign_id

NOTES
-----
• All fields preserved
• No business logic
• Column names normalized where necessary
• Raw column name documented in description
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
(

/* ============================================================================
   IDENTIFIERS
============================================================================ */

account_id STRING OPTIONS(description='Raw: account_id. SA360 account ID.'),

account_name STRING OPTIONS(description='Raw: account_name. Account name.'),

campaign_id STRING OPTIONS(description='Raw: campaign_id. Campaign unique identifier.'),

resource_name STRING OPTIONS(description='Raw: resource_name. Google Ads API resource path.'),

customer_id STRING OPTIONS(description='Raw: customer_id. Engine customer ID.'),

/* ============================================================================
   SNAPSHOT DATE
============================================================================ */

date_yyyymmdd STRING OPTIONS(description='Raw: date_yyyymmdd. Snapshot date in YYYYMMDD.'),

snapshot_date DATE OPTIONS(description='Derived: Parsed snapshot date from date_yyyymmdd.'),

/* ============================================================================
   LOAD METADATA
============================================================================ */

file_load_datetime DATETIME OPTIONS(description='Raw: File_Load_datetime. ETL ingestion timestamp.'),

filename STRING OPTIONS(description='Raw: Filename. Source file name.'),

bronze_inserted_at TIMESTAMP OPTIONS(description='Timestamp when inserted into Bronze.'),

/* ============================================================================
   CAMPAIGN CONFIGURATION
============================================================================ */

name STRING OPTIONS(description='Raw: name. Campaign name.'),

advertising_channel_type STRING OPTIONS(description='Raw: advertising_channel_type.'),

advertising_channel_sub_type STRING OPTIONS(description='Raw: advertising_channel_sub_type.'),

status STRING OPTIONS(description='Raw: status. Engine status (ENABLED, PAUSED, etc).'),

serving_status STRING OPTIONS(description='Raw: serving_status. Serving state.'),

campaign_budget STRING OPTIONS(description='Raw: campaign_budget.'),

campaign_priority STRING OPTIONS(description='Raw: campaign_priority.'),

creation_time STRING OPTIONS(description='Raw: creation_time. Creation timestamp.'),

start_date STRING OPTIONS(description='Raw: start_date.'),

end_date STRING OPTIONS(description='Raw: end_date.'),

sales_country STRING OPTIONS(description='Raw: sales_country.'),

domain_name STRING OPTIONS(description='Raw: domain_name.'),

engine_id STRING OPTIONS(description='Raw: engine_id.'),

merchant_id STRING OPTIONS(description='Raw: merchant_id.'),

feed_label STRING OPTIONS(description='Raw: feed_label.'),

labels STRING OPTIONS(description='Raw: labels.'),

language_code STRING OPTIONS(description='Raw: language_code.'),

opt_in STRING OPTIONS(description='Raw: opt_in.'),

/* ============================================================================
   BIDDING
============================================================================ */

bidding_strategy STRING OPTIONS(description='Raw: bidding_strategy.'),

bidding_strategy_id STRING OPTIONS(description='Raw: bidding_strategy_id.'),

bidding_strategy_type STRING OPTIONS(description='Raw: bidding_strategy_type.'),

bidding_strategy_system_status STRING OPTIONS(description='Raw: bidding_strategy_system_status.'),

manual_cpa STRING OPTIONS(description='Raw: manual_cpa.'),

manual_cpm STRING OPTIONS(description='Raw: manual_cpm.'),

manual_cpc_enhanced_cpc_enabled STRING OPTIONS(description='Raw: manual_cpc_enhanced_cpc_enabled.'),

max_conv_value_target_roas STRING OPTIONS(description='Raw: max_conv_value_target_roas.'),

max_convs_target_cpa_micros STRING OPTIONS(description='Raw: max_convs_target_cpa_micros.'),

target_cpa_target_cpa_micros STRING OPTIONS(description='Raw: target_cpa_target_cpa_micros.'),

target_cpa_cpc_bid_ceiling_micros STRING OPTIONS(description='Raw: target_cpa_cpc_bid_ceiling_micros.'),

target_cpa_cpc_bid_floor_micros STRING OPTIONS(description='Raw: target_cpa_cpc_bid_floor_micros.'),

target_roas_target_roas STRING OPTIONS(description='Raw: target_roas_target_roas.'),

target_roas_cpc_bid_ceiling_micros STRING OPTIONS(description='Raw: target_roas_cpc_bid_ceiling_micros.'),

target_roas_cpc_bid_floor_micros STRING OPTIONS(description='Raw: target_roas_cpc_bid_floor_micros.'),

target_spend_micros STRING OPTIONS(description='Raw: target_spend_micros.'),

target_spend_cpc_bid_ceiling_micros STRING OPTIONS(description='Raw: target_spend_cpc_bid_ceiling_micros.'),

percent_cpc_bid_ceiling_micros STRING OPTIONS(description='Raw: percent_cpc_cpc_bid_ceiling_micros.'),

percent_cpc_enhanced_cpc_enabled STRING OPTIONS(description='Raw: percent_cpc_enhanced_cpc_enabled.'),

target_imp_share_location STRING OPTIONS(description='Raw: target_imp_share_location.'),

target_imp_share_location_fraction_micros STRING OPTIONS(description='Raw: target_imp_share_location_fraction_micros.'),

target_imp_share_cpc_bid_ceiling_micros STRING OPTIONS(description='Raw: target_imp_share_cpc_bid_ceiling_micros.'),

target_cpm STRING OPTIONS(description='Raw: target_cpm.'),

optimization_goal_types STRING OPTIONS(description='Raw: optimization_goal_types.'),

conversion_actions STRING OPTIONS(description='Raw: conversion_actions.'),

/* ============================================================================
   TARGETING
============================================================================ */

positive_geo_target_type STRING OPTIONS(description='Raw: positive_geo_target_type.'),

negative_geo_target_type STRING OPTIONS(description='Raw: negative_geo_target_type.'),

target_search_network STRING OPTIONS(description='Raw: target_search_network.'),

target_partner_search_network STRING OPTIONS(description='Raw: target_partner_search_network.'),

target_google_search STRING OPTIONS(description='Raw: target_google_search.'),

target_content_network STRING OPTIONS(description='Raw: target_content_network.'),

enable_local STRING OPTIONS(description='Raw: enable_local.'),

frequency_caps STRING OPTIONS(description='Raw: frequency_caps.'),

excluded_parent_asset_field_types STRING OPTIONS(description='Raw: excluded_parent_asset_field_types.'),

use_supplied_urls_only STRING OPTIONS(description='Raw: use_supplied_urls_only.'),

url_expansion_opt_out STRING OPTIONS(description='Raw: url_expansion_opt_out.'),

use_vehicle_inventory STRING OPTIONS(description='Raw: use_vehicle_inventory.'),

/* ============================================================================
   TRACKING
============================================================================ */

tracking_url STRING OPTIONS(description='Raw: tracking_url.'),

tracking_url_template STRING OPTIONS(description='Raw: tracking_url_template.'),

url_custom_parameters STRING OPTIONS(description='Raw: url_custom_parameters.')

)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id;
