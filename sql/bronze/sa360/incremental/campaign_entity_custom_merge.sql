/*
===============================================================================
INCREMENTAL | BRONZE | SA360 | CAMPAIGN ENTITY (SCHEDULED DAILY)
===============================================================================

GOAL
----
Upsert snapshot campaign entity rows without duplicates.

KEY
---
account_id + campaign_id + date_yyyymmdd

LOOKBACK
--------
7 days by File_Load_datetime

NOTES
-----
Entity table does NOT contain `bi` (bi belongs only to Campaign Daily),
so this MERGE never references bi.
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` T
USING (
  SELECT
    File_Load_datetime AS file_load_datetime,
    Filename AS filename,

    CAST(account_id AS STRING) AS account_id,
    account_name,
    CAST(customer_id AS STRING) AS customer_id,
    CAST(campaign_id AS STRING) AS campaign_id,
    resource_name,
    name,

    CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS snapshot_date,

    ad_serving_optimization_status,
    advertising_channel_type,
    advertising_channel_sub_type,
    status,
    serving_status,
    campaign_budget,
    campaign_priority,

    conversion_actions,
    optimization_goal_types,
    creation_time,
    start_date,
    end_date,
    sales_country,
    domain_name,
    engine_id,
    merchant_id,
    feed_label,
    labels,
    language_code,
    opt_in,

    bidding_strategy,
    bidding_strategy_id,
    bidding_strategy_type,
    bidding_strategy_system_status,

    manual_cpa,
    manual_cpm,
    manual_cpc_enhanced_cpc_enabled,

    max_conv_value_target_roas,
    max_convs_target_cpa_micros,

    percent_cpc_cpc_bid_ceiling_micros AS percent_cpc_bid_ceiling_micros,
    percent_cpc_enhanced_cpc_enabled,

    target_cpa_target_cpa_micros,
    target_cpa_cpc_bid_ceiling_micros,
    target_cpa_cpc_bid_floor_micros,

    target_roas_target_roas,
    target_roas_cpc_bid_ceiling_micros,
    target_roas_cpc_bid_floor_micros,

    target_spend_micros,
    target_spend_cpc_bid_ceiling_micros,

    target_imp_share_cpc_bid_ceiling_micros,
    target_imp_share_location,
    target_imp_share_location_fraction_micros,
    target_cpm,

    positive_geo_target_type,
    negative_geo_target_type,
    target_search_network,
    target_partner_search_network,
    target_google_search,
    target_content_network,
    enable_local,
    frequency_caps,
    excluded_parent_asset_field_types,
    use_supplied_urls_only,
    url_expansion_opt_out,
    use_vehicle_inventory,

    final_url_suffix,
    tracking_url,
    tracking_url_template,
    url_custom_parameters

  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
  WHERE TIMESTAMP(File_Load_datetime) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)
) S
ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN
UPDATE SET
  T.file_load_datetime = S.file_load_datetime,
  T.filename = S.filename,

  T.account_name = S.account_name,
  T.customer_id = S.customer_id,
  T.resource_name = S.resource_name,
  T.name = S.name,

  T.snapshot_date = S.snapshot_date,

  T.ad_serving_optimization_status = S.ad_serving_optimization_status,
  T.advertising_channel_type = S.advertising_channel_type,
  T.advertising_channel_sub_type = S.advertising_channel_sub_type,
  T.status = S.status,
  T.serving_status = S.serving_status,
  T.campaign_budget = S.campaign_budget,
  T.campaign_priority = S.campaign_priority,

  T.conversion_actions = S.conversion_actions,
  T.optimization_goal_types = S.optimization_goal_types,
  T.creation_time = S.creation_time,
  T.start_date = S.start_date,
  T.end_date = S.end_date,
  T.sales_country = S.sales_country,
  T.domain_name = S.domain_name,
  T.engine_id = S.engine_id,
  T.merchant_id = S.merchant_id,
  T.feed_label = S.feed_label,
  T.labels = S.labels,
  T.language_code = S.language_code,
  T.opt_in = S.opt_in,

  T.bidding_strategy = S.bidding_strategy,
  T.bidding_strategy_id = S.bidding_strategy_id,
  T.bidding_strategy_type = S.bidding_strategy_type,
  T.bidding_strategy_system_status = S.bidding_strategy_system_status,

  T.manual_cpa = S.manual_cpa,
  T.manual_cpm = S.manual_cpm,
  T.manual_cpc_enhanced_cpc_enabled = S.manual_cpc_enhanced_cpc_enabled,

  T.max_conv_value_target_roas = S.max_conv_value_target_roas,
  T.max_convs_target_cpa_micros = S.max_convs_target_cpa_micros,

  T.percent_cpc_bid_ceiling_micros = S.percent_cpc_bid_ceiling_micros,
  T.percent_cpc_enhanced_cpc_enabled = S.percent_cpc_enhanced_cpc_enabled,

  T.target_cpa_target_cpa_micros = S.target_cpa_target_cpa_micros,
  T.target_cpa_cpc_bid_ceiling_micros = S.target_cpa_cpc_bid_ceiling_micros,
  T.target_cpa_cpc_bid_floor_micros = S.target_cpa_cpc_bid_floor_micros,

  T.target_roas_target_roas = S.target_roas_target_roas,
  T.target_roas_cpc_bid_ceiling_micros = S.target_roas_cpc_bid_ceiling_micros,
  T.target_roas_cpc_bid_floor_micros = S.target_roas_cpc_bid_floor_micros,

  T.target_spend_micros = S.target_spend_micros,
  T.target_spend_cpc_bid_ceiling_micros = S.target_spend_cpc_bid_ceiling_micros,

  T.target_imp_share_cpc_bid_ceiling_micros = S.target_imp_share_cpc_bid_ceiling_micros,
  T.target_imp_share_location = S.target_imp_share_location,
  T.target_imp_share_location_fraction_micros = S.target_imp_share_location_fraction_micros,
  T.target_cpm = S.target_cpm,

  T.positive_geo_target_type = S.positive_geo_target_type,
  T.negative_geo_target_type = S.negative_geo_target_type,
  T.target_search_network = S.target_search_network,
  T.target_partner_search_network = S.target_partner_search_network,
  T.target_google_search = S.target_google_search,
  T.target_content_network = S.target_content_network,
  T.enable_local = S.enable_local,
  T.frequency_caps = S.frequency_caps,
  T.excluded_parent_asset_field_types = S.excluded_parent_asset_field_types,
  T.use_supplied_urls_only = S.use_supplied_urls_only,
  T.url_expansion_opt_out = S.url_expansion_opt_out,
  T.use_vehicle_inventory = S.use_vehicle_inventory,

  T.final_url_suffix = S.final_url_suffix,
  T.tracking_url = S.tracking_url,
  T.tracking_url_template = S.tracking_url_template,
  T.url_custom_parameters = S.url_custom_parameters,

  T.bronze_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT (
  file_load_datetime, filename,
  account_id, account_name, customer_id, campaign_id, resource_name, name,
  date_yyyymmdd, snapshot_date,
  ad_serving_optimization_status, advertising_channel_type, advertising_channel_sub_type,
  status, serving_status, campaign_budget, campaign_priority,
  conversion_actions, optimization_goal_types, creation_time, start_date, end_date,
  sales_country, domain_name, engine_id, merchant_id, feed_label, labels, language_code, opt_in,
  bidding_strategy, bidding_strategy_id, bidding_strategy_type, bidding_strategy_system_status,
  manual_cpa, manual_cpm, manual_cpc_enhanced_cpc_enabled,
  max_conv_value_target_roas, max_convs_target_cpa_micros,
  percent_cpc_bid_ceiling_micros, percent_cpc_enhanced_cpc_enabled,
  target_cpa_target_cpa_micros, target_cpa_cpc_bid_ceiling_micros, target_cpa_cpc_bid_floor_micros,
  target_roas_target_roas, target_roas_cpc_bid_ceiling_micros, target_roas_cpc_bid_floor_micros,
  target_spend_micros, target_spend_cpc_bid_ceiling_micros,
  target_imp_share_cpc_bid_ceiling_micros, target_imp_share_location, target_imp_share_location_fraction_micros,
  target_cpm,
  positive_geo_target_type, negative_geo_target_type, target_search_network, target_partner_search_network,
  target_google_search, target_content_network,
  enable_local, frequency_caps, excluded_parent_asset_field_types,
  use_supplied_urls_only, url_expansion_opt_out, use_vehicle_inventory,
  final_url_suffix, tracking_url, tracking_url_template, url_custom_parameters,
  bronze_inserted_at
)
VALUES (
  S.file_load_datetime, S.filename,
  S.account_id, S.account_name, S.customer_id, S.campaign_id, S.resource_name, S.name,
  S.date_yyyymmdd, S.snapshot_date,
  S.ad_serving_optimization_status, S.advertising_channel_type, S.advertising_channel_sub_type,
  S.status, S.serving_status, S.campaign_budget, S.campaign_priority,
  S.conversion_actions, S.optimization_goal_types, S.creation_time, S.start_date, S.end_date,
  S.sales_country, S.domain_name, S.engine_id, S.merchant_id, S.feed_label, S.labels, S.language_code, S.opt_in,
  S.bidding_strategy, S.bidding_strategy_id, S.bidding_strategy_type, S.bidding_strategy_system_status,
  S.manual_cpa, S.manual_cpm, S.manual_cpc_enhanced_cpc_enabled,
  S.max_conv_value_target_roas, S.max_convs_target_cpa_micros,
  S.percent_cpc_bid_ceiling_micros, S.percent_cpc_enhanced_cpc_enabled,
  S.target_cpa_target_cpa_micros, S.target_cpa_cpc_bid_ceiling_micros, S.target_cpa_cpc_bid_floor_micros,
  S.target_roas_target_roas, S.target_roas_cpc_bid_ceiling_micros, S.target_roas_cpc_bid_floor_micros,
  S.target_spend_micros, S.target_spend_cpc_bid_ceiling_micros,
  S.target_imp_share_cpc_bid_ceiling_micros, S.target_imp_share_location, S.target_imp_share_location_fraction_micros,
  S.target_cpm,
  S.positive_geo_target_type, S.negative_geo_target_type, S.target_search_network, S.target_partner_search_network,
  S.target_google_search, S.target_content_network,
  S.enable_local, S.frequency_caps, S.excluded_parent_asset_field_types,
  S.use_supplied_urls_only, S.url_expansion_opt_out, S.use_vehicle_inventory,
  S.final_url_suffix, S.tracking_url, S.tracking_url_template, S.url_custom_parameters,
  CURRENT_TIMESTAMP()
);
