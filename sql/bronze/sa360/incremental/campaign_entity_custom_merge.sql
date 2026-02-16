/*
===============================================================================
BRONZE | SA 360 | CAMPAIGN ENTITY | INCREMENTAL MERGE (FULL SNAPSHOT VERSION)
===============================================================================

PURPOSE
-------
Incrementally load and refresh the Bronze Campaign Entity
metadata table from raw Improvado SA360 export.

This merge:
  • Preserves ALL raw configuration columns
  • Applies 7-day lookback window for late-arriving files
  • Fully refreshes all entity attributes on match
  • Is idempotent and safe for daily orchestration

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

LOOKBACK STRATEGY
-----------------
Uses file_load_datetime timestamp
to capture newly delivered or backfilled files.

DESIGN PRINCIPLES
-----------------
1. Snapshot-style ingestion
2. No column dropped
3. No transformation applied
4. Full configuration refresh on match
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` T

USING (

  SELECT

    /* ============================================================
       IDENTIFIERS
       ============================================================ */

    account_id,
    account_name,
    campaign_id,
    resource_name,
    customer_id,

    /* ============================================================
       DATE FIELDS
       ============================================================ */

    date_yyyymmdd,
    PARSE_DATE('%Y%m%d', date_yyyymmdd) AS date,
    creation_time,
    start_date,
    end_date,

    /* ============================================================
       CHANNEL & STATUS
       ============================================================ */

    advertising_channel_type,
    advertising_channel_sub_type,
    status,
    serving_status,
    ad_serving_optimization_status,

    /* ============================================================
       BIDDING CONFIGURATION
       ============================================================ */

    bidding_strategy,
    bidding_strategy_id,
    bidding_strategy_system_status,
    bidding_strategy_type,
    campaign_budget,
    campaign_priority,

    manual_cpa,
    manual_cpc_enhanced_cpc_enabled,
    manual_cpm,

    max_conv_value_target_roas,
    max_convs_target_cpa_micros,

    target_cpa_target_cpa_micros,
    target_cpa_cpc_bid_ceiling_micros,
    target_cpa_cpc_bid_floor_micros,

    target_roas_target_roas,
    target_roas_cpc_bid_ceiling_micros,
    target_roas_cpc_bid_floor_micros,

    target_search_network,
    target_google_search,
    target_partner_search_network,
    target_content_network,

    target_imp_share_cpc_bid_ceiling_micros,
    target_imp_share_location,
    target_imp_share_location_fraction_micros,

    target_spend_cpc_bid_ceiling_micros,
    target_spend_micros,

    percent_cpc_cpc_bid_ceiling_micros,
    percent_cpc_enhanced_cpc_enabled,

    /* ============================================================
       TARGETING
       ============================================================ */

    positive_geo_target_type,
    negative_geo_target_type,
    language_code,
    merchant_id,
    sales_country,

    domain_name,
    enable_local,
    use_vehicle_inventory,
    opt_in,

    /* ============================================================
       URL & TRACKING
       ============================================================ */

    final_url_suffix,
    tracking_url,
    tracking_url_template,
    url_custom_parameters,
    url_expansion_opt_out,
    use_supplied_urls_only,

    /* ============================================================
       ADDITIONAL METADATA
       ============================================================ */

    name,
    labels,
    optimization_goal_types,
    excluded_parent_asset_field_types,
    feed_label,
    frequency_caps,
    conversion_actions,
    engine_id,

    /* ============================================================
       LOAD METADATA
       ============================================================ */

    File_Load_datetime AS file_load_datetime,
    Filename AS filename,
    CURRENT_TIMESTAMP() AS bronze_inserted_at

  FROM
  `prj-dbi-prd-1.ds_dbi_improvado_master
    .google_search_ads_360_beta_campaign_entity_custom_tmo`

  WHERE
  TIMESTAMP(File_Load_datetime)
  >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)

) S

ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

/* ============================================================================
   WHEN MATCHED → FULL CONFIGURATION REFRESH
   ============================================================================ */

WHEN MATCHED THEN
UPDATE SET

  advertising_channel_type = S.advertising_channel_type,
  advertising_channel_sub_type = S.advertising_channel_sub_type,
  status = S.status,
  serving_status = S.serving_status,
  ad_serving_optimization_status = S.ad_serving_optimization_status,

  bidding_strategy = S.bidding_strategy,
  bidding_strategy_id = S.bidding_strategy_id,
  bidding_strategy_system_status = S.bidding_strategy_system_status,
  bidding_strategy_type = S.bidding_strategy_type,
  campaign_budget = S.campaign_budget,
  campaign_priority = S.campaign_priority,

  manual_cpa = S.manual_cpa,
  manual_cpc_enhanced_cpc_enabled = S.manual_cpc_enhanced_cpc_enabled,
  manual_cpm = S.manual_cpm,

  max_conv_value_target_roas = S.max_conv_value_target_roas,
  max_convs_target_cpa_micros = S.max_convs_target_cpa_micros,

  target_cpa_target_cpa_micros = S.target_cpa_target_cpa_micros,
  target_roas_target_roas = S.target_roas_target_roas,

  positive_geo_target_type = S.positive_geo_target_type,
  negative_geo_target_type = S.negative_geo_target_type,
  language_code = S.language_code,

  final_url_suffix = S.final_url_suffix,
  tracking_url = S.tracking_url,
  tracking_url_template = S.tracking_url_template,
  url_custom_parameters = S.url_custom_parameters,

  name = S.name,
  labels = S.labels,
  optimization_goal_types = S.optimization_goal_types,

  file_load_datetime = S.file_load_datetime,
  filename = S.filename,
  bronze_inserted_at = CURRENT_TIMESTAMP()

/* ============================================================================
   WHEN NOT MATCHED → INSERT FULL SNAPSHOT ROW
   ============================================================================ */

WHEN NOT MATCHED THEN
INSERT ROW;
