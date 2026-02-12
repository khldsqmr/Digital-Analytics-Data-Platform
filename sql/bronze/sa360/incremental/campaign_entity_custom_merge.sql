/*
===============================================================================
BRONZE | SA 360 | CAMPAIGN ENTITY | INCREMENTAL MERGE
===============================================================================

GRAIN
account_id + campaign_id + file_load_datetime
--
-- File: campaign_entity_custom_merge.sql
--
NOTES
- 7-day lookback window for late-arriving files
- Safe for daily scheduling
- Snapshot-style ingestion

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` T

USING (

  SELECT
    account_id,
    account_name,
    campaign_id,
    resource_name,
    customer_id,
    date_yyyymmdd,
    PARSE_DATE('%Y%m%d', date_yyyymmdd) AS date,
    creation_time,
    start_date,
    end_date,
    advertising_channel_type,
    advertising_channel_sub_type,
    status,
    serving_status,
    ad_serving_optimization_status,
    bidding_strategy,
    bidding_strategy_id,
    bidding_strategy_system_status,
    bidding_strategy_type,
    campaign_budget,
    manual_cpa,
    manual_cpc_enhanced_cpc_enabled,
    manual_cpm,
    max_conv_value_target_roas,
    max_convs_target_cpa_micros,
    target_cpa_target_cpa_micros,
    target_roas_target_roas,
    target_search_network,
    target_google_search,
    target_partner_search_network,
    positive_geo_target_type,
    negative_geo_target_type,
    language_code,
    merchant_id,
    sales_country,
    domain_name,
    final_url_suffix,
    tracking_url,
    tracking_url_template,
    url_custom_parameters,
    url_expansion_opt_out,
    use_supplied_urls_only,
    name,
    labels,
    optimization_goal_types,
    enable_local,
    use_vehicle_inventory,
    campaign_priority,
    engine_id,
    excluded_parent_asset_field_types,
    feed_label,
    File_Load_datetime AS file_load_datetime,
    Filename AS filename,
    CURRENT_TIMESTAMP() AS bronze_inserted_at

  FROM
  `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`

  WHERE
    TIMESTAMP(File_Load_datetime)
    >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL lookback_days DAY)

) S

ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN
  UPDATE SET
    serving_status = S.serving_status,
    status = S.status,
    bidding_strategy_type = S.bidding_strategy_type,
    campaign_budget = S.campaign_budget,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename,
    bronze_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    account_id,
    account_name,
    campaign_id,
    resource_name,
    customer_id,
    date_yyyymmdd,
    date,
    creation_time,
    start_date,
    end_date,
    advertising_channel_type,
    advertising_channel_sub_type,
    status,
    serving_status,
    ad_serving_optimization_status,
    bidding_strategy,
    bidding_strategy_id,
    bidding_strategy_system_status,
    bidding_strategy_type,
    campaign_budget,
    manual_cpa,
    manual_cpc_enhanced_cpc_enabled,
    manual_cpm,
    max_conv_value_target_roas,
    max_convs_target_cpa_micros,
    target_cpa_target_cpa_micros,
    target_roas_target_roas,
    target_search_network,
    target_google_search,
    target_partner_search_network,
    positive_geo_target_type,
    negative_geo_target_type,
    language_code,
    merchant_id,
    sales_country,
    domain_name,
    final_url_suffix,
    tracking_url,
    tracking_url_template,
    url_custom_parameters,
    url_expansion_opt_out,
    use_supplied_urls_only,
    name,
    labels,
    optimization_goal_types,
    enable_local,
    use_vehicle_inventory,
    campaign_priority,
    engine_id,
    excluded_parent_asset_field_types,
    feed_label,
    file_load_datetime,
    filename,
    bronze_inserted_at
  )
  VALUES (
    S.account_id,
    S.account_name,
    S.campaign_id,
    S.resource_name,
    S.customer_id,
    S.date_yyyymmdd,
    S.date,
    S.creation_time,
    S.start_date,
    S.end_date,
    S.advertising_channel_type,
    S.advertising_channel_sub_type,
    S.status,
    S.serving_status,
    S.ad_serving_optimization_status,
    S.bidding_strategy,
    S.bidding_strategy_id,
    S.bidding_strategy_system_status,
    S.bidding_strategy_type,
    S.campaign_budget,
    S.manual_cpa,
    S.manual_cpc_enhanced_cpc_enabled,
    S.manual_cpm,
    S.max_conv_value_target_roas,
    S.max_convs_target_cpa_micros,
    S.target_cpa_target_cpa_micros,
    S.target_roas_target_roas,
    S.target_search_network,
    S.target_google_search,
    S.target_partner_search_network,
    S.positive_geo_target_type,
    S.negative_geo_target_type,
    S.language_code,
    S.merchant_id,
    S.sales_country,
    S.domain_name,
    S.final_url_suffix,
    S.tracking_url,
    S.tracking_url_template,
    S.url_custom_parameters,
    S.url_expansion_opt_out,
    S.use_supplied_urls_only,
    S.name,
    S.labels,
    S.optimization_goal_types,
    S.enable_local,
    S.use_vehicle_inventory,
    S.campaign_priority,
    S.engine_id,
    S.excluded_parent_asset_field_types,
    S.feed_label,
    S.file_load_datetime,
    S.filename,
    S.bronze_inserted_at
  );
