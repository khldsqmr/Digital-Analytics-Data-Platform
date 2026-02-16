/*
===============================================================================
INCREMENTAL | BRONZE | SA360 | CAMPAIGN ENTITY (SCHEDULED DAILY)
===============================================================================

KEY (no duplicates)
-------------------
(account_id, campaign_id, date_yyyymmdd)

DATE REQUIREMENT
----------------
Derived DATE from date_yyyymmdd is named `date`.
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
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS date,

    ad_serving_optimization_status,
    advertising_channel_sub_type,
    advertising_channel_type,
    bidding_strategy,
    bidding_strategy_id,
    bidding_strategy_system_status,
    bidding_strategy_type,
    campaign_budget,
    campaign_priority,
    conversion_actions,
    creation_time,
    domain_name,
    enable_local,
    end_date,
    engine_id,
    excluded_parent_asset_field_types,
    feed_label,
    final_url_suffix,
    frequency_caps,
    labels,
    language_code,
    manual_cpa,
    manual_cpc_enhanced_cpc_enabled,
    manual_cpm,
    max_conv_value_target_roas,
    max_convs_target_cpa_micros,
    merchant_id,
    negative_geo_target_type,
    opt_in,
    optimization_goal_types,
    percent_cpc_cpc_bid_ceiling_micros,
    percent_cpc_enhanced_cpc_enabled,
    positive_geo_target_type,
    sales_country,
    serving_status,
    start_date,
    status,
    target_content_network,
    target_cpa_cpc_bid_ceiling_micros,
    target_cpa_cpc_bid_floor_micros,
    target_cpa_target_cpa_micros,
    target_cpm,
    target_google_search,
    target_imp_share_cpc_bid_ceiling_micros,
    target_imp_share_location,
    target_imp_share_location_fraction_micros,
    target_partner_search_network,
    target_roas_cpc_bid_ceiling_micros,
    target_roas_cpc_bid_floor_micros,
    target_roas_target_roas,
    target_search_network,
    target_spend_cpc_bid_ceiling_micros,
    target_spend_micros,
    tracking_url,
    tracking_url_template,
    url_custom_parameters,
    url_expansion_opt_out,
    use_supplied_urls_only,
    use_vehicle_inventory

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

  T.date = S.date,

  T.ad_serving_optimization_status = S.ad_serving_optimization_status,
  T.advertising_channel_sub_type = S.advertising_channel_sub_type,
  T.advertising_channel_type = S.advertising_channel_type,
  T.bidding_strategy = S.bidding_strategy,
  T.bidding_strategy_id = S.bidding_strategy_id,
  T.bidding_strategy_system_status = S.bidding_strategy_system_status,
  T.bidding_strategy_type = S.bidding_strategy_type,
  T.campaign_budget = S.campaign_budget,
  T.campaign_priority = S.campaign_priority,
  T.conversion_actions = S.conversion_actions,
  T.creation_time = S.creation_time,
  T.domain_name = S.domain_name,
  T.enable_local = S.enable_local,
  T.end_date = S.end_date,
  T.engine_id = S.engine_id,
  T.excluded_parent_asset_field_types = S.excluded_parent_asset_field_types,
  T.feed_label = S.feed_label,
  T.final_url_suffix = S.final_url_suffix,
  T.frequency_caps = S.frequency_caps,
  T.labels = S.labels,
  T.language_code = S.language_code,
  T.manual_cpa = S.manual_cpa,
  T.manual_cpc_enhanced_cpc_enabled = S.manual_cpc_enhanced_cpc_enabled,
  T.manual_cpm = S.manual_cpm,
  T.max_conv_value_target_roas = S.max_conv_value_target_roas,
  T.max_convs_target_cpa_micros = S.max_convs_target_cpa_micros,
  T.merchant_id = S.merchant_id,
  T.negative_geo_target_type = S.negative_geo_target_type,
  T.opt_in = S.opt_in,
  T.optimization_goal_types = S.optimization_goal_types,
  T.percent_cpc_cpc_bid_ceiling_micros = S.percent_cpc_cpc_bid_ceiling_micros,
  T.percent_cpc_enhanced_cpc_enabled = S.percent_cpc_enhanced_cpc_enhanced_cpc_enabled,
  T.positive_geo_target_type = S.positive_geo_target_type,
  T.sales_country = S.sales_country,
  T.serving_status = S.serving_status,
  T.start_date = S.start_date,
  T.status = S.status,
  T.target_content_network = S.target_content_network,
  T.target_cpa_cpc_bid_ceiling_micros = S.target_cpa_cpc_bid_ceiling_micros,
  T.target_cpa_cpc_bid_floor_micros = S.target_cpa_cpc_bid_floor_micros,
  T.target_cpa_target_cpa_micros = S.target_cpa_target_cpa_micros,
  T.target_cpm = S.target_cpm,
  T.target_google_search = S.target_google_search,
  T.target_imp_share_cpc_bid_ceiling_micros = S.target_imp_share_cpc_bid_ceiling_micros,
  T.target_imp_share_location = S.target_imp_share_location,
  T.target_imp_share_location_fraction_micros = S.target_imp_share_location_fraction_micros,
  T.target_partner_search_network = S.target_partner_search_network,
  T.target_roas_cpc_bid_ceiling_micros = S.target_roas_cpc_bid_ceiling_micros,
  T.target_roas_cpc_bid_floor_micros = S.target_roas_cpc_bid_floor_micros,
  T.target_roas_target_roas = S.target_roas_target_roas,
  T.target_search_network = S.target_search_network,
  T.target_spend_cpc_bid_ceiling_micros = S.target_spend_cpc_bid_ceiling_micros,
  T.target_spend_micros = S.target_spend_micros,
  T.tracking_url = S.tracking_url,
  T.tracking_url_template = S.tracking_url_template,
  T.url_custom_parameters = S.url_custom_parameters,
  T.url_expansion_opt_out = S.url_expansion_opt_out,
  T.use_supplied_urls_only = S.use_supplied_urls_only,
  T.use_vehicle_inventory = S.use_vehicle_inventory,

  T.bronze_updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT (
  file_load_datetime, filename,
  account_id, account_name, customer_id, campaign_id, resource_name, name,
  date_yyyymmdd, date,
  ad_serving_optimization_status, advertising_channel_sub_type, advertising_channel_type,
  bidding_strategy, bidding_strategy_id, bidding_strategy_system_status, bidding_strategy_type,
  campaign_budget, campaign_priority, conversion_actions, creation_time,
  domain_name, enable_local, end_date, engine_id, excluded_parent_asset_field_types, feed_label,
  final_url_suffix, frequency_caps, labels, language_code, manual_cpa, manual_cpc_enhanced_cpc_enabled,
  manual_cpm, max_conv_value_target_roas, max_convs_target_cpa_micros, merchant_id,
  negative_geo_target_type, opt_in, optimization_goal_types, percent_cpc_cpc_bid_ceiling_micros,
  percent_cpc_enhanced_cpc_enabled, positive_geo_target_type, sales_country, serving_status,
  start_date, status, target_content_network, target_cpa_cpc_bid_ceiling_micros, target_cpa_cpc_bid_floor_micros,
  target_cpa_target_cpa_micros, target_cpm, target_google_search, target_imp_share_cpc_bid_ceiling_micros,
  target_imp_share_location, target_imp_share_location_fraction_micros, target_partner_search_network,
  target_roas_cpc_bid_ceiling_micros, target_roas_cpc_bid_floor_micros, target_roas_target_roas,
  target_search_network, target_spend_cpc_bid_ceiling_micros, target_spend_micros,
  tracking_url, tracking_url_template, url_custom_parameters, url_expansion_opt_out,
  use_supplied_urls_only, use_vehicle_inventory,
  bronze_updated_at
)
VALUES (
  S.file_load_datetime, S.filename,
  S.account_id, S.account_name, S.customer_id, S.campaign_id, S.resource_name, S.name,
  S.date_yyyymmdd, S.date,
  S.ad_serving_optimization_status, S.advertising_channel_sub_type, S.advertising_channel_type,
  S.bidding_strategy, S.bidding_strategy_id, S.bidding_strategy_system_status, S.bidding_strategy_type,
  S.campaign_budget, S.campaign_priority, S.conversion_actions, S.creation_time,
  S.domain_name, S.enable_local, S.end_date, S.engine_id, S.excluded_parent_asset_field_types, S.feed_label,
  S.final_url_suffix, S.frequency_caps, S.labels, S.language_code, S.manual_cpa, S.manual_cpc_enhanced_cpc_enabled,
  S.manual_cpm, S.max_conv_value_target_roas, S.max_convs_target_cpa_micros, S.merchant_id,
  S.negative_geo_target_type, S.opt_in, S.optimization_goal_types, S.percent_cpc_cpc_bid_ceiling_micros,
  S.percent_cpc_enhanced_cpc_enabled, S.positive_geo_target_type, S.sales_country, S.serving_status,
  S.start_date, S.status, S.target_content_network, S.target_cpa_cpc_bid_ceiling_micros, S.target_cpa_cpc_bid_floor_micros,
  S.target_cpa_target_cpa_micros, S.target_cpm, S.target_google_search, S.target_imp_share_cpc_bid_ceiling_micros,
  S.target_imp_share_location, S.target_imp_share_location_fraction_micros, S.target_partner_search_network,
  S.target_roas_cpc_bid_ceiling_micros, S.target_roas_cpc_bid_floor_micros, S.target_roas_target_roas,
  S.target_search_network, S.target_spend_cpc_bid_ceiling_micros, S.target_spend_micros,
  S.tracking_url, S.tracking_url_template, S.url_custom_parameters, S.url_expansion_opt_out,
  S.use_supplied_urls_only, S.use_vehicle_inventory,
  CURRENT_TIMESTAMP()
);
