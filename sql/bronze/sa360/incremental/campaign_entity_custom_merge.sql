/*
===============================================================================
INCREMENTAL | BRONZE | SA360 | CAMPAIGN ENTITY (SETTINGS SNAPSHOT)
===============================================================================

SOURCE (RAW):
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo

TARGET (BRONZE):
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

COST CONTROL:
  Process only the most recent N days of snapshot data using lookback_days variable.

NO DUPLICATES:
  MERGE KEY:
    (account_id, campaign_id, date_yyyymmdd)

  DEDUP WITHIN WINDOW:
    Keep latest row by:
      File_Load_datetime DESC, Filename DESC

STANDARDIZATION:
  - Parsed date from date_yyyymmdd is named "date"
  - Raw column "name" becomes campaign_name

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` T
USING (
  WITH src AS (
    SELECT
      CAST(raw.account_id AS STRING) AS account_id,
      CAST(raw.campaign_id AS STRING) AS campaign_id,
      CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
      SAFE.PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING)) AS date,

      CAST(raw.account_name AS STRING) AS account_name,
      CAST(raw.customer_id AS STRING) AS customer_id,

      CAST(raw.ad_serving_optimization_status AS STRING) AS ad_serving_optimization_status,
      CAST(raw.advertising_channel_sub_type AS STRING) AS advertising_channel_sub_type,
      CAST(raw.advertising_channel_type AS STRING) AS advertising_channel_type,

      CAST(raw.bidding_strategy AS STRING) AS bidding_strategy,
      CAST(raw.bidding_strategy_id AS STRING) AS bidding_strategy_id,
      CAST(raw.bidding_strategy_system_status AS STRING) AS bidding_strategy_system_status,
      CAST(raw.bidding_strategy_type AS STRING) AS bidding_strategy_type,

      CAST(raw.campaign_budget AS STRING) AS campaign_budget,
      CAST(raw.campaign_priority AS STRING) AS campaign_priority,
      CAST(raw.conversion_actions AS STRING) AS conversion_actions,
      CAST(raw.creation_time AS STRING) AS creation_time,

      CAST(raw.domain_name AS STRING) AS domain_name,
      CAST(raw.enable_local AS STRING) AS enable_local,
      CAST(raw.end_date AS STRING) AS end_date,
      CAST(raw.engine_id AS STRING) AS engine_id,
      CAST(raw.excluded_parent_asset_field_types AS STRING) AS excluded_parent_asset_field_types,
      CAST(raw.feed_label AS STRING) AS feed_label,
      CAST(raw.final_url_suffix AS STRING) AS final_url_suffix,
      CAST(raw.frequency_caps AS STRING) AS frequency_caps,
      CAST(raw.labels AS STRING) AS labels,
      CAST(raw.language_code AS STRING) AS language_code,

      CAST(raw.manual_cpa AS STRING) AS manual_cpa,
      CAST(raw.manual_cpc_enhanced_cpc_enabled AS STRING) AS manual_cpc_enhanced_cpc_enabled,
      CAST(raw.manual_cpm AS STRING) AS manual_cpm,

      CAST(raw.max_convs_target_cpa_micros AS STRING) AS max_convs_target_cpa_micros,
      CAST(raw.max_conv_value_target_roas AS STRING) AS max_conv_value_target_roas,

      CAST(raw.merchant_id AS STRING) AS merchant_id,
      CAST(raw.name AS STRING) AS campaign_name,

      CAST(raw.negative_geo_target_type AS STRING) AS negative_geo_target_type,
      CAST(raw.optimization_goal_types AS STRING) AS optimization_goal_types,
      CAST(raw.opt_in AS STRING) AS opt_in,

      CAST(raw.percent_cpc_cpc_bid_ceiling_micros AS STRING) AS percent_cpc_cpc_bid_ceiling_micros,
      CAST(raw.percent_cpc_enhanced_cpc_enabled AS STRING) AS percent_cpc_enhanced_cpc_enabled,

      CAST(raw.positive_geo_target_type AS STRING) AS positive_geo_target_type,
      CAST(raw.resource_name AS STRING) AS resource_name,
      CAST(raw.sales_country AS STRING) AS sales_country,
      CAST(raw.serving_status AS STRING) AS serving_status,
      CAST(raw.start_date AS STRING) AS start_date,
      CAST(raw.status AS STRING) AS status,

      CAST(raw.target_content_network AS STRING) AS target_content_network,
      CAST(raw.target_cpa_cpc_bid_ceiling_micros AS STRING) AS target_cpa_cpc_bid_ceiling_micros,
      CAST(raw.target_cpa_cpc_bid_floor_micros AS STRING) AS target_cpa_cpc_bid_floor_micros,
      CAST(raw.target_cpa_target_cpa_micros AS STRING) AS target_cpa_target_cpa_micros,
      CAST(raw.target_cpm AS STRING) AS target_cpm,
      CAST(raw.target_google_search AS STRING) AS target_google_search,

      CAST(raw.target_imp_share_cpc_bid_ceiling_micros AS STRING) AS target_imp_share_cpc_bid_ceiling_micros,
      CAST(raw.target_imp_share_location AS STRING) AS target_imp_share_location,
      CAST(raw.target_imp_share_location_fraction_micros AS STRING) AS target_imp_share_location_fraction_micros,

      CAST(raw.target_partner_search_network AS STRING) AS target_partner_search_network,

      CAST(raw.target_roas_cpc_bid_ceiling_micros AS STRING) AS target_roas_cpc_bid_ceiling_micros,
      CAST(raw.target_roas_cpc_bid_floor_micros AS STRING) AS target_roas_cpc_bid_floor_micros,
      CAST(raw.target_roas_target_roas AS STRING) AS target_roas_target_roas,

      CAST(raw.target_search_network AS STRING) AS target_search_network,

      CAST(raw.target_spend_cpc_bid_ceiling_micros AS STRING) AS target_spend_cpc_bid_ceiling_micros,
      CAST(raw.target_spend_micros AS STRING) AS target_spend_micros,

      CAST(raw.tracking_url AS STRING) AS tracking_url,
      CAST(raw.tracking_url_template AS STRING) AS tracking_url_template,
      CAST(raw.url_custom_parameters AS STRING) AS url_custom_parameters,
      CAST(raw.url_expansion_opt_out AS STRING) AS url_expansion_opt_out,
      CAST(raw.use_supplied_urls_only AS STRING) AS use_supplied_urls_only,
      CAST(raw.use_vehicle_inventory AS STRING) AS use_vehicle_inventory,

      CAST(raw.File_Load_datetime AS DATETIME) AS file_load_datetime,
      CAST(raw.Filename AS STRING) AS filename

    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo` raw
    WHERE
      SAFE.PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING))
        >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
  ),

  dedup AS (
    SELECT * EXCEPT(rn)
    FROM (
      SELECT
        src.*,
        ROW_NUMBER() OVER (
          PARTITION BY account_id, campaign_id, date_yyyymmdd
          ORDER BY file_load_datetime DESC, filename DESC
        ) AS rn
      FROM src
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

WHEN NOT MATCHED THEN INSERT (
  account_id, campaign_id, date_yyyymmdd, date,
  account_name, customer_id,
  ad_serving_optimization_status, advertising_channel_sub_type, advertising_channel_type,
  bidding_strategy, bidding_strategy_id, bidding_strategy_system_status, bidding_strategy_type,
  campaign_budget, campaign_priority, conversion_actions, creation_time,
  domain_name, enable_local, end_date, engine_id, excluded_parent_asset_field_types,
  feed_label, final_url_suffix, frequency_caps, labels, language_code,
  manual_cpa, manual_cpc_enhanced_cpc_enabled, manual_cpm,
  max_convs_target_cpa_micros, max_conv_value_target_roas,
  merchant_id, campaign_name,
  negative_geo_target_type, optimization_goal_types, opt_in,
  percent_cpc_cpc_bid_ceiling_micros, percent_cpc_enhanced_cpc_enabled,
  positive_geo_target_type, resource_name, sales_country, serving_status, start_date, status,
  target_content_network, target_cpa_cpc_bid_ceiling_micros, target_cpa_cpc_bid_floor_micros,
  target_cpa_target_cpa_micros, target_cpm, target_google_search,
  target_imp_share_cpc_bid_ceiling_micros, target_imp_share_location, target_imp_share_location_fraction_micros,
  target_partner_search_network,
  target_roas_cpc_bid_ceiling_micros, target_roas_cpc_bid_floor_micros, target_roas_target_roas,
  target_search_network,
  target_spend_cpc_bid_ceiling_micros, target_spend_micros,
  tracking_url, tracking_url_template, url_custom_parameters, url_expansion_opt_out,
  use_supplied_urls_only, use_vehicle_inventory,
  file_load_datetime, filename
)
VALUES (
  S.account_id, S.campaign_id, S.date_yyyymmdd, S.date,
  S.account_name, S.customer_id,
  S.ad_serving_optimization_status, S.advertising_channel_sub_type, S.advertising_channel_type,
  S.bidding_strategy, S.bidding_strategy_id, S.bidding_strategy_system_status, S.bidding_strategy_type,
  S.campaign_budget, S.campaign_priority, S.conversion_actions, S.creation_time,
  S.domain_name, S.enable_local, S.end_date, S.engine_id, S.excluded_parent_asset_field_types,
  S.feed_label, S.final_url_suffix, S.frequency_caps, S.labels, S.language_code,
  S.manual_cpa, S.manual_cpc_enhanced_cpc_enabled, S.manual_cpm,
  S.max_convs_target_cpa_micros, S.max_conv_value_target_roas,
  S.merchant_id, S.campaign_name,
  S.negative_geo_target_type, S.optimization_goal_types, S.opt_in,
  S.percent_cpc_cpc_bid_ceiling_micros, S.percent_cpc_enhanced_cpc_enabled,
  S.positive_geo_target_type, S.resource_name, S.sales_country, S.serving_status, S.start_date, S.status,
  S.target_content_network, S.target_cpa_cpc_bid_ceiling_micros, S.target_cpa_cpc_bid_floor_micros,
  S.target_cpa_target_cpa_micros, S.target_cpm, S.target_google_search,
  S.target_imp_share_cpc_bid_ceiling_micros, S.target_imp_share_location, S.target_imp_share_location_fraction_micros,
  S.target_partner_search_network,
  S.target_roas_cpc_bid_ceiling_micros, S.target_roas_cpc_bid_floor_micros, S.target_roas_target_roas,
  S.target_search_network,
  S.target_spend_cpc_bid_ceiling_micros, S.target_spend_micros,
  S.tracking_url, S.tracking_url_template, S.url_custom_parameters, S.url_expansion_opt_out,
  S.use_supplied_urls_only, S.use_vehicle_inventory,
  S.file_load_datetime, S.filename
);
