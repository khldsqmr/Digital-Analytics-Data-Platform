/*
===============================================================================
BRONZE | SA 360 | CAMPAIGN ENTITY | INCREMENTAL MERGE
===============================================================================

GRAIN
account_id + campaign_id + file_load_datetime

NOTES
- 7-day lookback window for late-arriving files
- Safe for daily scheduling
- Snapshot-style ingestion

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

-- =====================================================================
-- Incremental MERGE for Bronze SA360 Campaign Entity table
-- =====================================================================

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

    File_Load_datetime,
    Filename

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
    account_name = S.account_name,
    serving_status = S.serving_status,
    status = S.status,
    bidding_strategy_type = S.bidding_strategy_type,
    campaign_budget = S.campaign_budget,
    file_load_datetime = S.File_Load_datetime,
    filename = S.Filename,
    bronze_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT ROW;
