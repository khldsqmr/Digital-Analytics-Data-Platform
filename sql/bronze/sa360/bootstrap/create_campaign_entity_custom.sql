/*
===============================================================================
BOOTSTRAP | BRONZE | SA360 | CAMPAIGN ENTITY (SETTINGS SNAPSHOT)
===============================================================================

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo

TARGET (BRONZE):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

PURPOSE:
  Create the Bronze Campaign Entity table:
    - Campaign settings / configuration snapshot by date_yyyymmdd
    - Standardized column names for reporting
    - Strong column-level descriptions
    - Partitioning/clustering for efficient joins with Campaign Daily

KEY DESIGN CHOICES:
  1) "date" (DATE) is parsed from date_yyyymmdd (per your requirement).
  2) Raw column "name" is stored as campaign_name (avoid confusion with generic "name").

PARTITION / CLUSTER:
  - Partition by date (DATE)
  - Cluster by account_id, campaign_id

===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
(
  -- -----------------------------
  -- Keys / Snapshot
  -- -----------------------------
  account_id STRING OPTIONS(description="Unique numerical identifier assigned to each entity within SA360 hierarchy (raw: account_id)."),
  campaign_id STRING OPTIONS(description="Unique numeric identifier for a paid search campaign (raw: campaign_id)."),
  date_yyyymmdd STRING OPTIONS(description="Partition/snapshot date in YYYYMMDD (raw: date_yyyymmdd)."),
  date DATE OPTIONS(description="Parsed DATE from date_yyyymmdd (required canonical date for reporting)."),

  -- -----------------------------
  -- Core Dimensions / Settings
  -- -----------------------------
  account_name STRING OPTIONS(description="User-defined identifier for an engine/sub-manager/manager account (raw: account_name)."),
  customer_id STRING OPTIONS(description="Engine customer ID (same as account_id for Google Ads) (raw: customer_id)."),

  ad_serving_optimization_status STRING OPTIONS(description="Real-time operational state of automated bidding/budgeting/ad delivery (raw: ad_serving_optimization_status)."),
  advertising_channel_sub_type STRING OPTIONS(description="Paid search channel sub-type (e.g., SMART, LOCAL). Often empty for standard Search (raw: advertising_channel_sub_type)."),
  advertising_channel_type STRING OPTIONS(description="Paid search primary channel type (e.g., SEARCH, PERFORMANCE_MAX) (raw: advertising_channel_type)."),

  bidding_strategy STRING OPTIONS(description="Automated system managing keyword bids across engines to achieve performance goals (raw: bidding_strategy)."),
  bidding_strategy_id STRING OPTIONS(description="Unique identifier for automated bidding portfolio (raw: bidding_strategy_id)."),
  bidding_strategy_system_status STRING OPTIONS(description="Status of automated bidding portfolio (e.g., ENABLED, PAUSED) (raw: bidding_strategy_system_status)."),
  bidding_strategy_type STRING OPTIONS(description="Automated bidding algorithm type (e.g., TARGET_ROAS, MAXIMIZE_CONVERSIONS) (raw: bidding_strategy_type)."),

  campaign_budget STRING OPTIONS(description="Budget resource / budget setting used to control spend (raw: campaign_budget)."),
  campaign_priority STRING OPTIONS(description="Shopping campaign priority used when multiple campaigns advertise same product (raw: campaign_priority)."),
  conversion_actions STRING OPTIONS(description="Tracked conversion actions (Floodlights) used to measure success (raw: conversion_actions)."),
  creation_time STRING OPTIONS(description="Timestamp when campaign/item was created or first synced (raw: creation_time)."),

  domain_name STRING OPTIONS(description="Domain associated with the campaign (raw: domain_name)."),
  enable_local STRING OPTIONS(description="Whether local inventory/targeting is enabled (raw: enable_local)."),
  end_date STRING OPTIONS(description="Scheduled end date for the campaign (raw: end_date)."),
  engine_id STRING OPTIONS(description="Engine-specific account ID (Microsoft Ads often populated) (raw: engine_id)."),
  excluded_parent_asset_field_types STRING OPTIONS(description="Excluded asset field types inherited from parent (raw: excluded_parent_asset_field_types)."),
  feed_label STRING OPTIONS(description="Merchant Center/feed label associated with campaign (raw: feed_label)."),
  final_url_suffix STRING OPTIONS(description="Tracking parameters appended to landing page URLs (raw: final_url_suffix)."),
  frequency_caps STRING OPTIONS(description="Limits number of impressions per user over time (raw: frequency_caps)."),
  labels STRING OPTIONS(description="Custom internal tags used to organize/filter/report (raw: labels)."),
  language_code STRING OPTIONS(description="Target language code (e.g., en, es) (raw: language_code)."),

  manual_cpa STRING OPTIONS(description="Manual cost-per-action bidding configuration (raw: manual_cpa)."),
  manual_cpc_enhanced_cpc_enabled STRING OPTIONS(description="Enhanced CPC flag for manual CPC strategy (raw: manual_cpc_enhanced_cpc_enabled)."),
  manual_cpm STRING OPTIONS(description="Manual CPM strategy configuration (raw: manual_cpm)."),

  max_conv_value_target_roas STRING OPTIONS(description="Maximize conversion value target ROAS ratio (raw: max_conv_value_target_roas)."),
  max_convs_target_cpa_micros STRING OPTIONS(description="Maximize conversions target CPA cap in micros (raw: max_convs_target_cpa_micros)."),

  merchant_id STRING OPTIONS(description="Merchant/account identifier associated with commerce feeds (raw: merchant_id)."),
  campaign_name STRING OPTIONS(description="Campaign name (raw column: name)."),

  negative_geo_target_type STRING OPTIONS(description="Exclusion geo targeting type (e.g., PRESENCE) (raw: negative_geo_target_type)."),
  optimization_goal_types STRING OPTIONS(description="Business objective types for optimization (raw: optimization_goal_types)."),
  opt_in STRING OPTIONS(description="Whether feature/setting is opted in (raw: opt_in)."),

  percent_cpc_cpc_bid_ceiling_micros STRING OPTIONS(description="Percent CPC bid ceiling in micros (raw: percent_cpc_cpc_bid_ceiling_micros)."),
  percent_cpc_enhanced_cpc_enabled STRING OPTIONS(description="Percent CPC enhanced CPC flag (raw: percent_cpc_enhanced_cpc_enabled)."),

  positive_geo_target_type STRING OPTIONS(description="Inclusion geo targeting type (raw: positive_geo_target_type)."),
  resource_name STRING OPTIONS(description="Google Ads API resource name (raw: resource_name)."),
  sales_country STRING OPTIONS(description="Sales country/market (raw: sales_country)."),
  serving_status STRING OPTIONS(description="Serving/eligibility status (raw: serving_status)."),
  start_date STRING OPTIONS(description="Campaign start date (raw: start_date)."),
  status STRING OPTIONS(description="User-set status (ENABLED/PAUSED/REMOVED) (raw: status)."),

  target_content_network STRING OPTIONS(description="Whether content/display network is targeted (raw: target_content_network)."),
  target_cpa_cpc_bid_ceiling_micros STRING OPTIONS(description="Target CPA CPC bid ceiling in micros (raw: target_cpa_cpc_bid_ceiling_micros)."),
  target_cpa_cpc_bid_floor_micros STRING OPTIONS(description="Target CPA CPC bid floor in micros (raw: target_cpa_cpc_bid_floor_micros)."),
  target_cpa_target_cpa_micros STRING OPTIONS(description="Target CPA value in micros (raw: target_cpa_target_cpa_micros)."),
  target_cpm STRING OPTIONS(description="Target CPM value (raw: target_cpm)."),
  target_google_search STRING OPTIONS(description="Whether Google Search is targeted (raw: target_google_search)."),

  target_imp_share_cpc_bid_ceiling_micros STRING OPTIONS(description="Target impression share CPC bid ceiling (raw: target_imp_share_cpc_bid_ceiling_micros)."),
  target_imp_share_location STRING OPTIONS(description="Target impression share location (raw: target_imp_share_location)."),
  target_imp_share_location_fraction_micros STRING OPTIONS(description="Desired impression share fraction (micros) (raw: target_imp_share_location_fraction_micros)."),

  target_partner_search_network STRING OPTIONS(description="Whether partner search network is targeted (raw: target_partner_search_network)."),

  target_roas_cpc_bid_ceiling_micros STRING OPTIONS(description="Target ROAS CPC bid ceiling (raw: target_roas_cpc_bid_ceiling_micros)."),
  target_roas_cpc_bid_floor_micros STRING OPTIONS(description="Target ROAS CPC bid floor (raw: target_roas_cpc_bid_floor_micros)."),
  target_roas_target_roas STRING OPTIONS(description="Target ROAS ratio (raw: target_roas_target_roas)."),

  target_search_network STRING OPTIONS(description="Whether search network is targeted (raw: target_search_network)."),

  target_spend_cpc_bid_ceiling_micros STRING OPTIONS(description="Target spend CPC bid ceiling (raw: target_spend_cpc_bid_ceiling_micros)."),
  target_spend_micros STRING OPTIONS(description="Target spend budget amount in micros (raw: target_spend_micros)."),

  tracking_url STRING OPTIONS(description="Expanded tracking URL used for click redirect (raw: tracking_url)."),
  tracking_url_template STRING OPTIONS(description="Tracking URL template (pre-expansion) (raw: tracking_url_template)."),
  url_custom_parameters STRING OPTIONS(description="Key-value URL parameters at campaign level (raw: url_custom_parameters)."),
  url_expansion_opt_out STRING OPTIONS(description="If true, URL expansion is disabled (raw: url_expansion_opt_out)."),
  use_supplied_urls_only STRING OPTIONS(description="If true, only supplied URLs may be used (raw: use_supplied_urls_only)."),
  use_vehicle_inventory STRING OPTIONS(description="Flag indicating vehicle inventory feed usage (raw: use_vehicle_inventory)."),

  -- -----------------------------
  -- Ingestion metadata
  -- -----------------------------
  file_load_datetime DATETIME OPTIONS(description="Ingestion load timestamp (raw: File_Load_datetime)."),
  filename STRING OPTIONS(description="Source file path/name used to ingest this snapshot (raw: Filename).")
)
PARTITION BY date
CLUSTER BY account_id, campaign_id
OPTIONS(
  description = "Bronze SA360 campaign entity/settings snapshot. Standardized naming + column descriptions. Partitioned by parsed date."
);
