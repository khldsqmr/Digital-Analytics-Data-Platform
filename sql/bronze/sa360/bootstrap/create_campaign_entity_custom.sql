/*
===============================================================================
BOOTSTRAP | BRONZE | SA360 | CAMPAIGN ENTITY (ONE-TIME)
===============================================================================

PURPOSE
-------
Create Bronze Campaign Entity snapshot table with:
- Normalized schema
- Column descriptions (governance)
- Partition by snapshot_date (from date_yyyymmdd)
- Cluster by account_id + campaign_id

SOURCE
------
`prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`

GRAIN
-----
account_id + campaign_id + date_yyyymmdd
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
(
  /* ---------- LOAD METADATA ---------- */
  file_load_datetime DATETIME OPTIONS(description="Ingestion load timestamp (ETL file load time)."),
  filename STRING OPTIONS(description="Source file path/name used to ingest this snapshot."),

  /* ---------- KEYS / DIMS ---------- */
  account_id STRING OPTIONS(description="Unique numerical identifier assigned to each entity within the SA360 hierarchy."),
  account_name STRING OPTIONS(description="User-defined identifier for an engine account/sub-manager/manager account in SA360."),
  customer_id STRING OPTIONS(description="Engine customer ID (same as account_id for Google Ads)."),
  campaign_id STRING OPTIONS(description="Unique, persistent numeric identifier assigned to a paid search campaign within SA360."),
  resource_name STRING OPTIONS(description="Full Google Ads API resource name (e.g., customers/{id}/campaigns/{id})."),
  name STRING OPTIONS(description="Campaign name."),

  /* ---------- SNAPSHOT DATE ---------- */
  date_yyyymmdd STRING OPTIONS(description="Partition date in YYYYMMDD used for snapshotting."),
  snapshot_date DATE OPTIONS(description="Snapshot date as DATE derived from date_yyyymmdd."),

  /* ---------- CORE SETTINGS ---------- */
  ad_serving_optimization_status STRING OPTIONS(description="Real-time operational state of automated bidding/budgeting/ad delivery."),
  advertising_channel_type STRING OPTIONS(description="Paid search primary channel type (e.g., SEARCH)."),
  advertising_channel_sub_type STRING OPTIONS(description="Paid search channel sub-type (e.g., SMART, LOCAL)."),
  status STRING OPTIONS(description="Engine status set by user (e.g., ENABLED, PAUSED, REMOVED)."),
  serving_status STRING OPTIONS(description="Indicates whether campaign is actively running or not eligible."),
  campaign_budget STRING OPTIONS(description="Budget resource controlling campaign spend."),
  campaign_priority STRING OPTIONS(description="Shopping campaign priority when multiple campaigns advertise same product."),

  conversion_actions STRING OPTIONS(description="Tracked conversion actions (Floodlights)."),
  optimization_goal_types STRING OPTIONS(description="Business objective automated bidding targets to maximize profit."),
  creation_time STRING OPTIONS(description="Timestamp when item was created or first synced into SA360."),
  start_date STRING OPTIONS(description="Paid search campaign start date."),
  end_date STRING OPTIONS(description="Scheduled end date for the paid search campaign."),
  sales_country STRING OPTIONS(description="Sales country/market (e.g., US)."),
  domain_name STRING OPTIONS(description="Domain associated to the campaign (if used in templates/tracking)."),
  engine_id STRING OPTIONS(description="Engine-specific account ID (Microsoft Ads often populated)."),
  merchant_id STRING OPTIONS(description="Merchant / feed merchant identifier."),
  feed_label STRING OPTIONS(description="Merchant Center or feed label associated with the campaign."),
  labels STRING OPTIONS(description="Internal tags used to organize/filter/report paid search entities."),
  language_code STRING OPTIONS(description="Language code for targeting (e.g., en, es)."),
  opt_in STRING OPTIONS(description="Whether the feature the field refers to is opted in (contextual)."),

  /* ---------- BIDDING STRATEGY ---------- */
  bidding_strategy STRING OPTIONS(description="Automated bidding strategy resource."),
  bidding_strategy_id STRING OPTIONS(description="Unique identifier for automated portfolio bidding model."),
  bidding_strategy_type STRING OPTIONS(description="Bidding algorithm type (e.g., TARGET_ROAS)."),
  bidding_strategy_system_status STRING OPTIONS(description="System status of bidding strategy (ENABLED/PAUSED/UNAVAILABLE)."),

  manual_cpa STRING OPTIONS(description="Manual cost per action (CPA) bidding setting."),
  manual_cpm STRING OPTIONS(description="Manual cost per thousand impressions (CPM) bidding setting."),
  manual_cpc_enhanced_cpc_enabled STRING OPTIONS(description="Enhanced CPC enablement flag (manual CPC + AI adjustments)."),

  max_conv_value_target_roas STRING OPTIONS(description="Maximize conversion value target ROAS ratio."),
  max_convs_target_cpa_micros STRING OPTIONS(description="Maximize conversions target CPA cap in micros."),

  percent_cpc_bid_ceiling_micros STRING OPTIONS(description="Percent CPC bid ceiling in micros."),
  percent_cpc_enhanced_cpc_enabled STRING OPTIONS(description="Percent CPC enhanced flag."),

  target_cpa_target_cpa_micros STRING OPTIONS(description="Target CPA value in micros."),
  target_cpa_cpc_bid_ceiling_micros STRING OPTIONS(description="Target CPA CPC bid ceiling in micros."),
  target_cpa_cpc_bid_floor_micros STRING OPTIONS(description="Target CPA CPC bid floor in micros."),

  target_roas_target_roas STRING OPTIONS(description="Target ROAS ratio."),
  target_roas_cpc_bid_ceiling_micros STRING OPTIONS(description="Target ROAS CPC bid ceiling in micros."),
  target_roas_cpc_bid_floor_micros STRING OPTIONS(description="Target ROAS CPC bid floor in micros."),

  target_spend_micros STRING OPTIONS(description="Target spend budget in micros."),
  target_spend_cpc_bid_ceiling_micros STRING OPTIONS(description="Target spend CPC bid ceiling in micros."),

  target_imp_share_cpc_bid_ceiling_micros STRING OPTIONS(description="Target impression share CPC bid ceiling in micros."),
  target_imp_share_location STRING OPTIONS(description="Target impression share location (e.g., TOP_OF_PAGE)."),
  target_imp_share_location_fraction_micros STRING OPTIONS(description="Desired impression share fraction in micros."),
  target_cpm STRING OPTIONS(description="Target CPM value for CPM strategies."),

  /* ---------- TARGETING ---------- */
  positive_geo_target_type STRING OPTIONS(description="Included geo targeting type (PRESENCE or PRESENCE_OR_INTEREST)."),
  negative_geo_target_type STRING OPTIONS(description="Excluded geo targeting type (PRESENCE)."),
  target_search_network STRING OPTIONS(description="Whether search network is targeted."),
  target_partner_search_network STRING OPTIONS(description="Whether partner search network is targeted."),
  target_google_search STRING OPTIONS(description="Whether Google Search is targeted."),
  target_content_network STRING OPTIONS(description="Whether content/display network is targeted."),
  enable_local STRING OPTIONS(description="Whether local targeting/inventory is enabled."),
  frequency_caps STRING OPTIONS(description="Caps limiting frequency of impressions per user in a time window."),
  excluded_parent_asset_field_types STRING OPTIONS(description="Excluded asset field types inherited from parent."),
  use_supplied_urls_only STRING OPTIONS(description="If true, only supplied URLs may be used."),
  url_expansion_opt_out STRING OPTIONS(description="If true, URL expansion is disabled."),
  use_vehicle_inventory STRING OPTIONS(description="Flag indicating vehicle inventory feed usage."),

  /* ---------- TRACKING ---------- */
  final_url_suffix STRING OPTIONS(description="Suffix for tracking parameters appended to landing page."),
  tracking_url STRING OPTIONS(description="Expanded tracking URL used for click redirect."),
  tracking_url_template STRING OPTIONS(description="Tracking URL template (pre-expansion)."),
  url_custom_parameters STRING OPTIONS(description="Key-value URL parameters defined at campaign level."),

  bronze_inserted_at TIMESTAMP OPTIONS(description="System timestamp when row was inserted/updated in Bronze.")
)
PARTITION BY snapshot_date
CLUSTER BY account_id, campaign_id;
