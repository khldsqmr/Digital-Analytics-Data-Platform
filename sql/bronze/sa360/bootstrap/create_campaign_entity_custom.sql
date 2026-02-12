/*
===============================================================================
BOOTSTRAP | BRONZE | SA 360 | CAMPAIGN ENTITY (ONE-TIME)
===============================================================================

-- PURPOSE:
--   Bootstrap Bronze SA360 Campaign Entity (metadata) table.
--   - Cleans column names
--   - Preserves tmo naming
--   - Keeps date_yyyymmdd
--   - Adds derived date
--   - Adds schema descriptions
--
-- SOURCE TABLE:
--   prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo
--
-- TARGET TABLE:
--   prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity
--
-- GRAIN
-- account_id + campaign_id + file_load_datetime

===============================================================================
*/
CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
(

  -- ============================================================
  -- IDENTIFIERS
  -- ============================================================

  account_id STRING OPTIONS(description='Search Ads 360 advertiser account ID.'),
  account_name STRING OPTIONS(description='Advertiser/account name.'),
  campaign_id STRING OPTIONS(description='Unique campaign ID within engine account.'),
  resource_name STRING OPTIONS(description='Google Ads API resource path.'),

  customer_id STRING OPTIONS(description='Engine customer ID.'),

  -- ============================================================
  -- DATE FIELDS
  -- ============================================================

  date_yyyymmdd STRING OPTIONS(description='Partition snapshot date in YYYYMMDD format.'),
  date DATE OPTIONS(description='Parsed DATE derived from date_yyyymmdd.'),

  creation_time STRING OPTIONS(description='Campaign creation timestamp from engine.'),
  start_date STRING OPTIONS(description='Campaign start date.'),
  end_date STRING OPTIONS(description='Campaign scheduled end date.'),

  -- ============================================================
  -- CHANNEL & STATUS
  -- ============================================================

  advertising_channel_type STRING OPTIONS(description='Primary channel type (SEARCH, PERFORMANCE_MAX).'),
  advertising_channel_sub_type STRING OPTIONS(description='Campaign sub-type.'),
  status STRING OPTIONS(description='Engine status set by user.'),
  serving_status STRING OPTIONS(description='Current serving status.'),

  ad_serving_optimization_status STRING OPTIONS(description='Ad serving optimization setting.'),

  -- ============================================================
  -- BIDDING
  -- ============================================================

  bidding_strategy STRING OPTIONS(description='Bidding strategy resource path.'),
  bidding_strategy_id STRING OPTIONS(description='Bidding strategy numeric ID.'),
  bidding_strategy_system_status STRING OPTIONS(description='System bidding strategy status.'),
  bidding_strategy_type STRING OPTIONS(description='Type of bidding strategy.'),

  campaign_budget STRING OPTIONS(description='Campaign budget resource path.'),

  manual_cpa STRING OPTIONS(description='Manual CPA value.'),
  manual_cpc_enhanced_cpc_enabled STRING OPTIONS(description='Enhanced CPC enabled flag.'),
  manual_cpm STRING OPTIONS(description='Manual CPM value.'),

  max_conv_value_target_roas STRING OPTIONS(description='Max conversion value target ROAS ratio.'),
  max_convs_target_cpa_micros STRING OPTIONS(description='Max conversions target CPA cap in micros.'),

  target_cpa_target_cpa_micros STRING OPTIONS(description='Target CPA value in micros.'),
  target_roas_target_roas STRING OPTIONS(description='Target ROAS ratio.'),

  target_search_network STRING OPTIONS(description='Search network targeting flag.'),
  target_google_search STRING OPTIONS(description='Google Search targeting flag.'),
  target_partner_search_network STRING OPTIONS(description='Partner search network targeting flag.'),

  -- ============================================================
  -- TARGETING
  -- ============================================================

  positive_geo_target_type STRING OPTIONS(description='Positive geo target type (PRESENCE or PRESENCE_OR_INTEREST).'),
  negative_geo_target_type STRING OPTIONS(description='Negative geo target type.'),

  language_code STRING OPTIONS(description='Language targeting code.'),

  merchant_id STRING OPTIONS(description='Merchant Center ID for Shopping/PMax.'),
  sales_country STRING OPTIONS(description='Sales country for Shopping/PMax.'),

  domain_name STRING OPTIONS(description='Domain associated with campaign.'),

  -- ============================================================
  -- URL & TRACKING
  -- ============================================================

  final_url_suffix STRING OPTIONS(description='Final URL suffix parameters.'),
  tracking_url STRING OPTIONS(description='Expanded tracking URL.'),
  tracking_url_template STRING OPTIONS(description='Tracking URL template.'),
  url_custom_parameters STRING OPTIONS(description='Campaign-level custom URL parameters.'),
  url_expansion_opt_out STRING OPTIONS(description='URL expansion opt-out flag.'),
  use_supplied_urls_only STRING OPTIONS(description='Use supplied URLs only flag.'),

  -- ============================================================
  -- OTHER METADATA
  -- ============================================================

  name STRING OPTIONS(description='Campaign name.'),
  labels STRING OPTIONS(description='List of labels applied to campaign.'),
  optimization_goal_types STRING OPTIONS(description='Optimization goals configured.'),

  enable_local STRING OPTIONS(description='Local inventory enabled flag.'),
  use_vehicle_inventory STRING OPTIONS(description='Vehicle inventory usage flag.'),

  campaign_priority STRING OPTIONS(description='Campaign priority value.'),

  engine_id STRING OPTIONS(description='Engine-specific account ID.'),

  excluded_parent_asset_field_types STRING OPTIONS(description='Excluded parent asset field types.'),
  feed_label STRING OPTIONS(description='Merchant Center feed label.'),

  -- ============================================================
  -- LOAD METADATA
  -- ============================================================

  file_load_datetime DATETIME OPTIONS(description='ETL file load timestamp.'),
  filename STRING OPTIONS(description='Source file name.'),
  bronze_inserted_at TIMESTAMP OPTIONS(description='Bronze ingestion timestamp.')

)
PARTITION BY date
CLUSTER BY account_id, campaign_id;
