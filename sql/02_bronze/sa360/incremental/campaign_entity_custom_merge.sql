/*
===============================================================================
FILE: 01_merge_sdi_bronze_sa360_campaign_entity.sql
LAYER: Bronze
TABLE: sdi_bronze_sa360_campaign_entity

PURPOSE:
  Incrementally upsert recent campaign entity/settings snapshots:
    - lookback window for late-arriving files
    - dedup within the window by (file_load_datetime desc, filename desc)
    - enforce canonical date from date_yyyymmdd
    - no-garbage: drop rows where parsed date IS NULL

MERGE KEY:
  (account_id, campaign_id, date_yyyymmdd)

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` T
USING (
  WITH src AS (
    SELECT
      SAFE_CAST(raw.account_id AS STRING) AS account_id,
      SAFE_CAST(raw.campaign_id AS STRING) AS campaign_id,
      SAFE_CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING)) AS date,

      NULLIF(TRIM(SAFE_CAST(raw.account_name AS STRING)), '') AS account_name,
      NULLIF(TRIM(SAFE_CAST(raw.customer_id AS STRING)), '') AS customer_id,

      NULLIF(TRIM(SAFE_CAST(raw.ad_serving_optimization_status AS STRING)), '') AS ad_serving_optimization_status,
      NULLIF(TRIM(SAFE_CAST(raw.advertising_channel_sub_type AS STRING)), '') AS advertising_channel_sub_type,
      NULLIF(TRIM(SAFE_CAST(raw.advertising_channel_type AS STRING)), '') AS advertising_channel_type,

      NULLIF(TRIM(SAFE_CAST(raw.bidding_strategy AS STRING)), '') AS bidding_strategy,
      NULLIF(TRIM(SAFE_CAST(raw.bidding_strategy_id AS STRING)), '') AS bidding_strategy_id,
      NULLIF(TRIM(SAFE_CAST(raw.bidding_strategy_system_status AS STRING)), '') AS bidding_strategy_system_status,
      NULLIF(TRIM(SAFE_CAST(raw.bidding_strategy_type AS STRING)), '') AS bidding_strategy_type,

      NULLIF(TRIM(SAFE_CAST(raw.campaign_budget AS STRING)), '') AS campaign_budget,
      NULLIF(TRIM(SAFE_CAST(raw.campaign_priority AS STRING)), '') AS campaign_priority,
      NULLIF(TRIM(SAFE_CAST(raw.conversion_actions AS STRING)), '') AS conversion_actions,
      NULLIF(TRIM(SAFE_CAST(raw.creation_time AS STRING)), '') AS creation_time,

      NULLIF(TRIM(SAFE_CAST(raw.domain_name AS STRING)), '') AS domain_name,
      NULLIF(TRIM(SAFE_CAST(raw.enable_local AS STRING)), '') AS enable_local,
      NULLIF(TRIM(SAFE_CAST(raw.end_date AS STRING)), '') AS end_date,
      NULLIF(TRIM(SAFE_CAST(raw.engine_id AS STRING)), '') AS engine_id,
      NULLIF(TRIM(SAFE_CAST(raw.excluded_parent_asset_field_types AS STRING)), '') AS excluded_parent_asset_field_types,
      NULLIF(TRIM(SAFE_CAST(raw.feed_label AS STRING)), '') AS feed_label,
      NULLIF(TRIM(SAFE_CAST(raw.final_url_suffix AS STRING)), '') AS final_url_suffix,
      NULLIF(TRIM(SAFE_CAST(raw.frequency_caps AS STRING)), '') AS frequency_caps,
      NULLIF(TRIM(SAFE_CAST(raw.labels AS STRING)), '') AS labels,
      NULLIF(TRIM(SAFE_CAST(raw.language_code AS STRING)), '') AS language_code,

      NULLIF(TRIM(SAFE_CAST(raw.manual_cpa AS STRING)), '') AS manual_cpa,
      NULLIF(TRIM(SAFE_CAST(raw.manual_cpc_enhanced_cpc_enabled AS STRING)), '') AS manual_cpc_enhanced_cpc_enabled,
      NULLIF(TRIM(SAFE_CAST(raw.manual_cpm AS STRING)), '') AS manual_cpm,

      NULLIF(TRIM(SAFE_CAST(raw.max_convs_target_cpa_micros AS STRING)), '') AS max_convs_target_cpa_micros,
      NULLIF(TRIM(SAFE_CAST(raw.max_conv_value_target_roas AS STRING)), '') AS max_conv_value_target_roas,

      NULLIF(TRIM(SAFE_CAST(raw.merchant_id AS STRING)), '') AS merchant_id,
      NULLIF(TRIM(SAFE_CAST(raw.name AS STRING)), '') AS campaign_name,

      NULLIF(TRIM(SAFE_CAST(raw.negative_geo_target_type AS STRING)), '') AS negative_geo_target_type,
      NULLIF(TRIM(SAFE_CAST(raw.optimization_goal_types AS STRING)), '') AS optimization_goal_types,
      NULLIF(TRIM(SAFE_CAST(raw.opt_in AS STRING)), '') AS opt_in,

      NULLIF(TRIM(SAFE_CAST(raw.percent_cpc_cpc_bid_ceiling_micros AS STRING)), '') AS percent_cpc_cpc_bid_ceiling_micros,
      NULLIF(TRIM(SAFE_CAST(raw.percent_cpc_enhanced_cpc_enabled AS STRING)), '') AS percent_cpc_enhanced_cpc_enabled,

      NULLIF(TRIM(SAFE_CAST(raw.positive_geo_target_type AS STRING)), '') AS positive_geo_target_type,
      NULLIF(TRIM(SAFE_CAST(raw.resource_name AS STRING)), '') AS resource_name,
      NULLIF(TRIM(SAFE_CAST(raw.sales_country AS STRING)), '') AS sales_country,
      NULLIF(TRIM(SAFE_CAST(raw.serving_status AS STRING)), '') AS serving_status,
      NULLIF(TRIM(SAFE_CAST(raw.start_date AS STRING)), '') AS start_date,
      NULLIF(TRIM(SAFE_CAST(raw.status AS STRING)), '') AS status,

      NULLIF(TRIM(SAFE_CAST(raw.target_content_network AS STRING)), '') AS target_content_network,
      NULLIF(TRIM(SAFE_CAST(raw.target_cpa_cpc_bid_ceiling_micros AS STRING)), '') AS target_cpa_cpc_bid_ceiling_micros,
      NULLIF(TRIM(SAFE_CAST(raw.target_cpa_cpc_bid_floor_micros AS STRING)), '') AS target_cpa_cpc_bid_floor_micros,
      NULLIF(TRIM(SAFE_CAST(raw.target_cpa_target_cpa_micros AS STRING)), '') AS target_cpa_target_cpa_micros,
      NULLIF(TRIM(SAFE_CAST(raw.target_cpm AS STRING)), '') AS target_cpm,
      NULLIF(TRIM(SAFE_CAST(raw.target_google_search AS STRING)), '') AS target_google_search,

      NULLIF(TRIM(SAFE_CAST(raw.target_imp_share_cpc_bid_ceiling_micros AS STRING)), '') AS target_imp_share_cpc_bid_ceiling_micros,
      NULLIF(TRIM(SAFE_CAST(raw.target_imp_share_location AS STRING)), '') AS target_imp_share_location,
      NULLIF(TRIM(SAFE_CAST(raw.target_imp_share_location_fraction_micros AS STRING)), '') AS target_imp_share_location_fraction_micros,

      NULLIF(TRIM(SAFE_CAST(raw.target_partner_search_network AS STRING)), '') AS target_partner_search_network,

      NULLIF(TRIM(SAFE_CAST(raw.target_roas_cpc_bid_ceiling_micros AS STRING)), '') AS target_roas_cpc_bid_ceiling_micros,
      NULLIF(TRIM(SAFE_CAST(raw.target_roas_cpc_bid_floor_micros AS STRING)), '') AS target_roas_cpc_bid_floor_micros,
      NULLIF(TRIM(SAFE_CAST(raw.target_roas_target_roas AS STRING)), '') AS target_roas_target_roas,

      NULLIF(TRIM(SAFE_CAST(raw.target_search_network AS STRING)), '') AS target_search_network,

      NULLIF(TRIM(SAFE_CAST(raw.target_spend_cpc_bid_ceiling_micros AS STRING)), '') AS target_spend_cpc_bid_ceiling_micros,
      NULLIF(TRIM(SAFE_CAST(raw.target_spend_micros AS STRING)), '') AS target_spend_micros,

      NULLIF(TRIM(SAFE_CAST(raw.tracking_url AS STRING)), '') AS tracking_url,
      NULLIF(TRIM(SAFE_CAST(raw.tracking_url_template AS STRING)), '') AS tracking_url_template,
      NULLIF(TRIM(SAFE_CAST(raw.url_custom_parameters AS STRING)), '') AS url_custom_parameters,
      NULLIF(TRIM(SAFE_CAST(raw.url_expansion_opt_out AS STRING)), '') AS url_expansion_opt_out,
      NULLIF(TRIM(SAFE_CAST(raw.use_supplied_urls_only AS STRING)), '') AS use_supplied_urls_only,
      NULLIF(TRIM(SAFE_CAST(raw.use_vehicle_inventory AS STRING)), '') AS use_vehicle_inventory,

      SAFE_CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      NULLIF(TRIM(SAFE_CAST(raw.Filename AS STRING)), '') AS filename

    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo` raw
    WHERE SAFE.PARSE_DATE('%Y%m%d', SAFE_CAST(raw.date_yyyymmdd AS STRING))
          >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),

  cleaned AS (
    SELECT *
    FROM src
    WHERE date IS NOT NULL
  ),

  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        cleaned.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC
        ) AS rn
      FROM cleaned
    )
    WHERE rn = 1
  )

  SELECT * FROM dedup
) S
ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN UPDATE SET
  date = S.date,
  account_name = S.account_name,
  customer_id = S.customer_id,

  ad_serving_optimization_status = S.ad_serving_optimization_status,
  advertising_channel_sub_type = S.advertising_channel_sub_type,
  advertising_channel_type = S.advertising_channel_type,

  bidding_strategy = S.bidding_strategy,
  bidding_strategy_id = S.bidding_strategy_id,
  bidding_strategy_system_status = S.bidding_strategy_system_status,
  bidding_strategy_type = S.bidding_strategy_type,

  campaign_budget = S.campaign_budget,
  campaign_priority = S.campaign_priority,
  conversion_actions = S.conversion_actions,
  creation_time = S.creation_time,

  domain_name = S.domain_name,
  enable_local = S.enable_local,
  end_date = S.end_date,
  engine_id = S.engine_id,
  excluded_parent_asset_field_types = S.excluded_parent_asset_field_types,
  feed_label = S.feed_label,
  final_url_suffix = S.final_url_suffix,
  frequency_caps = S.frequency_caps,
  labels = S.labels,
  language_code = S.language_code,

  manual_cpa = S.manual_cpa,
  manual_cpc_enhanced_cpc_enabled = S.manual_cpc_enhanced_cpc_enabled,
  manual_cpm = S.manual_cpm,

  max_convs_target_cpa_micros = S.max_convs_target_cpa_micros,
  max_conv_value_target_roas = S.max_conv_value_target_roas,

  merchant_id = S.merchant_id,
  campaign_name = S.campaign_name,

  negative_geo_target_type = S.negative_geo_target_type,
  optimization_goal_types = S.optimization_goal_types,
  opt_in = S.opt_in,

  percent_cpc_cpc_bid_ceiling_micros = S.percent_cpc_cpc_bid_ceiling_micros,
  percent_cpc_enhanced_cpc_enabled = S.percent_cpc_enhanced_cpc_enabled,

  positive_geo_target_type = S.positive_geo_target_type,
  resource_name = S.resource_name,
  sales_country = S.sales_country,
  serving_status = S.serving_status,
  start_date = S.start_date,
  status = S.status,

  target_content_network = S.target_content_network,
  target_cpa_cpc_bid_ceiling_micros = S.target_cpa_cpc_bid_ceiling_micros,
  target_cpa_cpc_bid_floor_micros = S.target_cpa_cpc_bid_floor_micros,
  target_cpa_target_cpa_micros = S.target_cpa_target_cpa_micros,
  target_cpm = S.target_cpm,
  target_google_search = S.target_google_search,

  target_imp_share_cpc_bid_ceiling_micros = S.target_imp_share_cpc_bid_ceiling_micros,
  target_imp_share_location = S.target_imp_share_location,
  target_imp_share_location_fraction_micros = S.target_imp_share_location_fraction_micros,

  target_partner_search_network = S.target_partner_search_network,

  target_roas_cpc_bid_ceiling_micros = S.target_roas_cpc_bid_ceiling_micros,
  target_roas_cpc_bid_floor_micros = S.target_roas_cpc_bid_floor_micros,
  target_roas_target_roas = S.target_roas_target_roas,

  target_search_network = S.target_search_network,

  target_spend_cpc_bid_ceiling_micros = S.target_spend_cpc_bid_ceiling_micros,
  target_spend_micros = S.target_spend_micros,

  tracking_url = S.tracking_url,
  tracking_url_template = S.tracking_url_template,
  url_custom_parameters = S.url_custom_parameters,
  url_expansion_opt_out = S.url_expansion_opt_out,
  use_supplied_urls_only = S.use_supplied_urls_only,
  use_vehicle_inventory = S.use_vehicle_inventory,

  file_load_datetime = S.file_load_datetime,
  filename = S.filename

WHEN NOT MATCHED THEN INSERT ROW;
