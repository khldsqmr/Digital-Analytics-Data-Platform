/*
===============================================================================
BOOTSTRAP | BRONZE | SA 360 | CAMPAIGN ENTITY (FULL SNAPSHOT BUILD)
===============================================================================

PURPOSE
-------
Create the Bronze Campaign Entity metadata table from raw
Improvado Search Ads 360 campaign entity export.

This table captures:
  • Campaign configuration
  • Bidding strategies
  • Targeting settings
  • Status & serving states
  • URL parameters
  • Budget configuration
  • Optimization flags

This is a SNAPSHOT table.

SOURCE TABLE
------------
prj-dbi-prd-1.ds_dbi_improvado_master
  .google_search_ads_360_beta_campaign_entity_custom_tmo

TARGET TABLE
------------
prj-dbi-prd-1.ds_dbi_digitalmedia_automation
  .sdi_bronze_sa360_campaign_entity

GRAIN
-----
account_id + campaign_id + date_yyyymmdd

PARTITION
---------
date (parsed from date_yyyymmdd)

CLUSTER
-------
account_id, campaign_id

DESIGN PRINCIPLES
-----------------
1. No column dropped
2. No business logic applied
3. Raw configuration preserved
4. Snapshot-safe for incremental loads
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
(

  /* ============================================================
     CORE IDENTIFIERS
     ============================================================ */

  account_id STRING OPTIONS(description='Search Ads 360 advertiser account ID'),
  account_name STRING OPTIONS(description='Advertiser account name'),
  campaign_id STRING OPTIONS(description='Campaign ID'),
  resource_name STRING OPTIONS(description='Google Ads API resource path'),
  customer_id STRING OPTIONS(description='Engine customer ID'),

  /* ============================================================
     DATE FIELDS
     ============================================================ */

  date_yyyymmdd STRING OPTIONS(description='Snapshot date in YYYYMMDD format'),
  date DATE OPTIONS(description='Parsed DATE from date_yyyymmdd'),
  creation_time STRING OPTIONS(description='Campaign creation timestamp'),
  start_date STRING OPTIONS(description='Campaign start date'),
  end_date STRING OPTIONS(description='Campaign scheduled end date'),

  /* ============================================================
     CHANNEL & STATUS
     ============================================================ */

  advertising_channel_type STRING OPTIONS(description='Primary channel type'),
  advertising_channel_sub_type STRING OPTIONS(description='Channel subtype'),
  status STRING OPTIONS(description='Campaign status'),
  serving_status STRING OPTIONS(description='Serving status'),
  ad_serving_optimization_status STRING OPTIONS(description='Ad serving optimization setting'),

  /* ============================================================
     BIDDING CONFIGURATION
     ============================================================ */

  bidding_strategy STRING OPTIONS(description='Bidding strategy resource path'),
  bidding_strategy_id STRING OPTIONS(description='Bidding strategy ID'),
  bidding_strategy_system_status STRING OPTIONS(description='System bidding status'),
  bidding_strategy_type STRING OPTIONS(description='Bidding strategy type'),

  campaign_budget STRING OPTIONS(description='Campaign budget resource'),
  campaign_priority STRING OPTIONS(description='Campaign priority'),

  manual_cpa STRING OPTIONS(description='Manual CPA'),
  manual_cpc_enhanced_cpc_enabled STRING OPTIONS(description='Enhanced CPC flag'),
  manual_cpm STRING OPTIONS(description='Manual CPM'),

  max_conv_value_target_roas STRING OPTIONS(description='Max conversion ROAS'),
  max_convs_target_cpa_micros STRING OPTIONS(description='Max conversions target CPA'),

  target_cpa_target_cpa_micros STRING OPTIONS(description='Target CPA micros'),
  target_cpa_cpc_bid_ceiling_micros STRING OPTIONS(description='Target CPA bid ceiling'),
  target_cpa_cpc_bid_floor_micros STRING OPTIONS(description='Target CPA bid floor'),

  target_roas_target_roas STRING OPTIONS(description='Target ROAS'),
  target_roas_cpc_bid_ceiling_micros STRING OPTIONS(description='Target ROAS bid ceiling'),
  target_roas_cpc_bid_floor_micros STRING OPTIONS(description='Target ROAS bid floor'),

  target_search_network STRING OPTIONS(description='Search network targeting'),
  target_google_search STRING OPTIONS(description='Google search targeting'),
  target_partner_search_network STRING OPTIONS(description='Partner search targeting'),
  target_content_network STRING OPTIONS(description='Content network targeting'),

  target_imp_share_cpc_bid_ceiling_micros STRING OPTIONS(description='Impression share bid ceiling'),
  target_imp_share_location STRING OPTIONS(description='Impression share location'),
  target_imp_share_location_fraction_micros STRING OPTIONS(description='Impression share fraction'),

  target_spend_cpc_bid_ceiling_micros STRING OPTIONS(description='Target spend bid ceiling'),
  target_spend_micros STRING OPTIONS(description='Target spend micros'),

  percent_cpc_cpc_bid_ceiling_micros STRING OPTIONS(description='Percent CPC bid ceiling'),
  percent_cpc_enhanced_cpc_enabled STRING OPTIONS(description='Percent CPC enhanced flag'),

  /* ============================================================
     TARGETING
     ============================================================ */

  positive_geo_target_type STRING OPTIONS(description='Positive geo targeting type'),
  negative_geo_target_type STRING OPTIONS(description='Negative geo targeting type'),
  language_code STRING OPTIONS(description='Language targeting'),
  merchant_id STRING OPTIONS(description='Merchant Center ID'),
  sales_country STRING OPTIONS(description='Sales country'),

  domain_name STRING OPTIONS(description='Associated domain'),
  enable_local STRING OPTIONS(description='Local inventory flag'),
  use_vehicle_inventory STRING OPTIONS(description='Vehicle inventory flag'),
  opt_in STRING OPTIONS(description='Opt-in flag'),

  /* ============================================================
     URL & TRACKING
     ============================================================ */

  final_url_suffix STRING OPTIONS(description='Final URL suffix'),
  tracking_url STRING OPTIONS(description='Expanded tracking URL'),
  tracking_url_template STRING OPTIONS(description='Tracking URL template'),
  url_custom_parameters STRING OPTIONS(description='Custom URL parameters'),
  url_expansion_opt_out STRING OPTIONS(description='URL expansion opt-out'),
  use_supplied_urls_only STRING OPTIONS(description='Use supplied URLs only'),

  /* ============================================================
     ADDITIONAL METADATA
     ============================================================ */

  name STRING OPTIONS(description='Campaign name'),
  labels STRING OPTIONS(description='Campaign labels'),
  optimization_goal_types STRING OPTIONS(description='Optimization goals'),
  excluded_parent_asset_field_types STRING OPTIONS(description='Excluded asset fields'),
  feed_label STRING OPTIONS(description='Merchant feed label'),
  frequency_caps STRING OPTIONS(description='Frequency caps'),
  conversion_actions STRING OPTIONS(description='Conversion actions'),
  engine_id STRING OPTIONS(description='Engine-specific ID'),

  /* ============================================================
     LOAD METADATA
     ============================================================ */

  file_load_datetime DATETIME OPTIONS(description='File load timestamp'),
  filename STRING OPTIONS(description='Source file name'),
  bronze_inserted_at TIMESTAMP OPTIONS(description='Bronze ingestion timestamp')

)
PARTITION BY date
CLUSTER BY account_id, campaign_id;
