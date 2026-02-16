/*
===============================================================================
ONE-TIME | BRONZE | SA360 | CAMPAIGN ENTITY (SETTINGS SNAPSHOT)
===============================================================================

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo

TARGET (BRONZE):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

GRAIN:
  One row per (account_id, campaign_id, date_yyyymmdd) snapshot.

NOTES:
  - We keep date_yyyymmdd (STRING) for snapshot tracking.
  - We also create a parsed DATE column named "date" (per your requirement).
  - We rename raw column "name" -> "campaign_name" because "name" is too generic.
  - We keep raw ingestion metadata (file_load_datetime, filename).

PARTITIONING / CLUSTERING:
  - Partition by date (parsed from date_yyyymmdd).
  - Cluster by account_id, campaign_id for performance.

===============================================================================
*/

CREATE OR REPLACE TABLE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
(
  -- -----------------------------
  -- Keys / Snapshot
  -- -----------------------------
  account_id STRING OPTIONS(description='Unique numerical identifier assigned to each entity within the SA360 hierarchy.'),
  campaign_id STRING OPTIONS(description='Unique, persistent numeric identifier assigned to a paid search campaign within SA360.'),
  date_yyyymmdd STRING OPTIONS(description='Partition/snapshot date in YYYYMMDD used for snapshotting.'),
  date DATE OPTIONS(description='Parsed snapshot date derived from date_yyyymmdd (YYYYMMDD). This field is named "date" by design.'),

  -- -----------------------------
  -- Dimensions / Settings
  -- -----------------------------
  account_name STRING OPTIONS(description='User-defined identifier for an engine account, sub-manager, or manager account within SA360.'),
  customer_id STRING OPTIONS(description='Engine customer ID (same as account_id for Google Ads).'),

  ad_serving_optimization_status STRING OPTIONS(description='Real-time state of automated bidding/budgeting/ad delivery (e.g., learning/restricted).'),
  advertising_channel_sub_type STRING OPTIONS(description='Paid search advertising channel sub-type (e.g., SMART, LOCAL). Often empty for standard Search.'),
  advertising_channel_type STRING OPTIONS(description='Paid search primary advertising channel type (e.g., SEARCH, PERFORMANCE_MAX).'),

  bidding_strategy STRING OPTIONS(description='Automated system that manages bids to achieve goals like conversions/CPA/ROAS.'),
  bidding_strategy_id STRING OPTIONS(description='Unique identifier for an automated bidding portfolio in SA360.'),
  bidding_strategy_system_status STRING OPTIONS(description='Status of the bidding portfolio (e.g., ENABLED, PAUSED, UNAVAILABLE).'),
  bidding_strategy_type STRING OPTIONS(description='Bid algorithm type (e.g., TARGET_ROAS, MAXIMIZE_CONVERSIONS).'),

  campaign_budget STRING OPTIONS(description='Budget resource / amount reference used to control spend for the campaign.'),
  campaign_priority STRING OPTIONS(description='Shopping campaign priority setting used when product is in multiple campaigns.'),
  conversion_actions STRING OPTIONS(description='Tracked conversion actions (e.g., Floodlights).'),

  creation_time STRING OPTIONS(description='Timestamp when campaign was created/first synced (yyyy-MM-dd HH:mm:ss).'),
  domain_name STRING OPTIONS(description='Domain associated to the campaign (if used in templates/tracking).'),
  enable_local STRING OPTIONS(description='Indicates if local inventory/targeting is enabled for the campaign.'),
  end_date STRING OPTIONS(description='Scheduled end date for the paid search campaign.'),
  engine_id STRING OPTIONS(description='Engine-specific account ID (often populated for Microsoft Ads).'),
  excluded_parent_asset_field_types STRING OPTIONS(description='Excluded asset field types inherited from parent.'),
  feed_label STRING OPTIONS(description='Merchant Center/feed label associated with the campaign.'),
  final_url_suffix STRING OPTIONS(description='Tracking suffix appended to landing URLs (e.g., UTM/custom params).'),
  frequency_caps STRING OPTIONS(description='Limits impressions per user within a time period (Display/Video settings).'),
  labels STRING OPTIONS(description='Custom internal tags used to organize/filter/report on campaigns.'),
  language_code STRING OPTIONS(description='Target language code (e.g., en, es).'),

  manual_cpa STRING OPTIONS(description='Manual CPA bidding configuration (if applicable).'),
  manual_cpc_enhanced_cpc_enabled STRING OPTIONS(description='Enhanced CPC flag for manual CPC bidding.'),
  manual_cpm STRING OPTIONS(description='Manual CPM bidding configuration (Display/Video contexts).'),

  max_convs_target_cpa_micros STRING OPTIONS(description='Target CPA cap (micros) for Maximize Conversions strategies.'),
  max_conv_value_target_roas STRING OPTIONS(description='Target ROAS for Maximize Conversion Value strategies (ratio).'),

  merchant_id STRING OPTIONS(description='Merchant identifier (where applicable).'),

  campaign_name STRING OPTIONS(description='Campaign name (raw field "name" renamed to "campaign_name").'),

  negative_geo_target_type STRING OPTIONS(description='Geo exclusion behavior (e.g., PRESENCE).'),
  optimization_goal_types STRING OPTIONS(description='Business objective(s) for automated bidding strategies.'),
  opt_in STRING OPTIONS(description='Whether the feature the field refers to is opted in (contextual).'),

  percent_cpc_cpc_bid_ceiling_micros STRING OPTIONS(description='Percent CPC: max CPC bid ceiling (micros).'),
  percent_cpc_enhanced_cpc_enabled STRING OPTIONS(description='Percent CPC with Enhanced CPC flag.'),

  positive_geo_target_type STRING OPTIONS(description='Geo inclusion behavior (e.g., PRESENCE, PRESENCE_OR_INTEREST).'),
  resource_name STRING OPTIONS(description='Full Google Ads API resource name (customers/{id}/campaigns/{id}).'),
  sales_country STRING OPTIONS(description='Sales country/market (e.g., US).'),
  serving_status STRING OPTIONS(description='Serving state (SERVING, NOT_ELIGIBLE, etc.).'),
  start_date STRING OPTIONS(description='Campaign start date.'),
  status STRING OPTIONS(description='User/engine status (ENABLED, PAUSED, REMOVED).'),

  target_content_network STRING OPTIONS(description='Whether content/display network is targeted.'),
  target_cpa_cpc_bid_ceiling_micros STRING OPTIONS(description='Target CPA: CPC bid ceiling (micros).'),
  target_cpa_cpc_bid_floor_micros STRING OPTIONS(description='Target CPA: CPC bid floor (micros).'),
  target_cpa_target_cpa_micros STRING OPTIONS(description='Target CPA value (micros).'),
  target_cpm STRING OPTIONS(description='Target CPM value (where applicable).'),
  target_google_search STRING OPTIONS(description='Whether Google Search is targeted.'),
  target_imp_share_cpc_bid_ceiling_micros STRING OPTIONS(description='Target Impression Share: CPC bid ceiling (micros).'),
  target_imp_share_location STRING OPTIONS(description='Target Impression Share location (e.g., TOP_OF_PAGE).'),
  target_imp_share_location_fraction_micros STRING OPTIONS(description='Target impression share fraction (micros, e.g., 500000=50%).'),
  target_partner_search_network STRING OPTIONS(description='Whether partner search network is targeted.'),
  target_roas_cpc_bid_ceiling_micros STRING OPTIONS(description='Target ROAS: CPC bid ceiling (micros).'),
  target_roas_cpc_bid_floor_micros STRING OPTIONS(description='Target ROAS: CPC bid floor (micros).'),
  target_roas_target_roas STRING OPTIONS(description='Target ROAS ratio (e.g., 3.0=300%).'),
  target_search_network STRING OPTIONS(description='Whether search network is targeted.'),
  target_spend_cpc_bid_ceiling_micros STRING OPTIONS(description='Target Spend: CPC bid ceiling (micros).'),
  target_spend_micros STRING OPTIONS(description='Target Spend budget amount (micros).'),

  tracking_url STRING OPTIONS(description='Expanded tracking URL used for click redirect (often SA360 ds_* params).'),
  tracking_url_template STRING OPTIONS(description='Tracking URL template (pre-expansion).'),

  url_custom_parameters STRING OPTIONS(description='Key-value URL parameters defined at campaign level.'),
  url_expansion_opt_out STRING OPTIONS(description='If true, URL expansion is disabled.'),
  use_supplied_urls_only STRING OPTIONS(description='If true, only supplied URLs may be used (no auto-generated).'),
  use_vehicle_inventory STRING OPTIONS(description='Flag indicating vehicle inventory feed usage.'),

  -- -----------------------------
  -- Ingestion metadata
  -- -----------------------------
  file_load_datetime DATETIME OPTIONS(description='Ingestion load timestamp (ETL file load time).'),
  filename STRING OPTIONS(description='Source file path/name used to ingest this snapshot.')
)
PARTITION BY date
CLUSTER BY account_id, campaign_id;
