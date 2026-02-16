/*
===============================================================================
INCREMENTAL | BRONZE | SA360 | CAMPAIGN ENTITY (DAILY SNAPSHOT)
===============================================================================

SOURCE TABLE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo

TARGET TABLE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity

GRAIN (ONE ROW PER SNAPSHOT PER CAMPAIGN):
  account_id + campaign_id + date_yyyymmdd

WHY THIS TABLE EXISTS:
  - This is the "entity / settings" table (campaign metadata & config).
  - It is snapshot-based (date_yyyymmdd is the snapshot partition date).
  - It is joined to Campaign Daily (performance metrics) using:
      account_id + campaign_id (and optionally date/date_yyyymmdd if you want
      point-in-time settings).

KEY REQUIREMENTS:
  1) No duplicates: enforced by MERGE key.
  2) Always load the latest version of a key (if multiple files contain same key)
     by taking the most recent File_Load_datetime.
  3) Keep raw columns intact, but use clean Bronze aliases for readability:
     - date_yyyymmdd -> date_yyyymmdd (kept)
     - parsed DATE from date_yyyymmdd -> date
     - name -> campaign_name
===============================================================================
*/

MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` T
USING (
  WITH src AS (
    SELECT
      -- -----------------------------
      -- Keys / Identity
      -- -----------------------------
      CAST(account_id AS STRING)   AS account_id,
      CAST(campaign_id AS STRING)  AS campaign_id,
      CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,

      -- Parsed snapshot date (per your requirement: call it "date")
      SAFE.PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS date,

      -- -----------------------------
      -- Dimensions / Campaign Settings
      -- (Only exact raw column names used below)
      -- -----------------------------
      CAST(account_name AS STRING) AS account_name,
      CAST(customer_id AS STRING)  AS customer_id,

      CAST(ad_serving_optimization_status AS STRING) AS ad_serving_optimization_status,
      CAST(advertising_channel_sub_type AS STRING)   AS advertising_channel_sub_type,
      CAST(advertising_channel_type AS STRING)       AS advertising_channel_type,

      CAST(bidding_strategy AS STRING)               AS bidding_strategy,
      CAST(bidding_strategy_id AS STRING)            AS bidding_strategy_id,
      CAST(bidding_strategy_system_status AS STRING) AS bidding_strategy_system_status,
      CAST(bidding_strategy_type AS STRING)          AS bidding_strategy_type,

      CAST(campaign_budget AS STRING)                AS campaign_budget,
      CAST(campaign_priority AS STRING)              AS campaign_priority,
      CAST(conversion_actions AS STRING)             AS conversion_actions,

      CAST(creation_time AS STRING)                  AS creation_time,
      CAST(domain_name AS STRING)                    AS domain_name,
      CAST(enable_local AS STRING)                   AS enable_local,
      CAST(end_date AS STRING)                       AS end_date,
      CAST(engine_id AS STRING)                      AS engine_id,
      CAST(excluded_parent_asset_field_types AS STRING) AS excluded_parent_asset_field_types,
      CAST(feed_label AS STRING)                     AS feed_label,
      CAST(final_url_suffix AS STRING)               AS final_url_suffix,
      CAST(frequency_caps AS STRING)                 AS frequency_caps,
      CAST(labels AS STRING)                         AS labels,
      CAST(language_code AS STRING)                  AS language_code,

      CAST(manual_cpa AS STRING)                     AS manual_cpa,
      CAST(manual_cpc_enhanced_cpc_enabled AS STRING) AS manual_cpc_enhanced_cpc_enabled,
      CAST(manual_cpm AS STRING)                     AS manual_cpm,

      CAST(max_convs_target_cpa_micros AS STRING)    AS max_convs_target_cpa_micros,
      CAST(max_conv_value_target_roas AS STRING)     AS max_conv_value_target_roas,

      CAST(merchant_id AS STRING)                    AS merchant_id,

      -- Raw column is "name" (too generic). Keep it but expose as campaign_name.
      CAST(name AS STRING)                           AS campaign_name,

      CAST(negative_geo_target_type AS STRING)       AS negative_geo_target_type,
      CAST(optimization_goal_types AS STRING)        AS optimization_goal_types,
      CAST(opt_in AS STRING)                         AS opt_in,

      CAST(percent_cpc_cpc_bid_ceiling_micros AS STRING) AS percent_cpc_cpc_bid_ceiling_micros,

      -- ✅ CORRECT COLUMN NAME (your error was here):
      CAST(percent_cpc_enhanced_cpc_enabled AS STRING) AS percent_cpc_enhanced_cpc_enabled,

      CAST(positive_geo_target_type AS STRING)       AS positive_geo_target_type,
      CAST(resource_name AS STRING)                  AS resource_name,
      CAST(sales_country AS STRING)                  AS sales_country,
      CAST(serving_status AS STRING)                 AS serving_status,
      CAST(start_date AS STRING)                     AS start_date,
      CAST(status AS STRING)                         AS status,

      CAST(target_content_network AS STRING)         AS target_content_network,
      CAST(target_cpa_cpc_bid_ceiling_micros AS STRING) AS target_cpa_cpc_bid_ceiling_micros,
      CAST(target_cpa_cpc_bid_floor_micros AS STRING)   AS target_cpa_cpc_bid_floor_micros,
      CAST(target_cpa_target_cpa_micros AS STRING)      AS target_cpa_target_cpa_micros,

      CAST(target_cpm AS STRING)                     AS target_cpm,
      CAST(target_google_search AS STRING)           AS target_google_search,

      CAST(target_imp_share_cpc_bid_ceiling_micros AS STRING) AS target_imp_share_cpc_bid_ceiling_micros,
      CAST(target_imp_share_location AS STRING)      AS target_imp_share_location,
      CAST(target_imp_share_location_fraction_micros AS STRING) AS target_imp_share_location_fraction_micros,

      CAST(target_partner_search_network AS STRING)  AS target_partner_search_network,

      CAST(target_roas_cpc_bid_ceiling_micros AS STRING) AS target_roas_cpc_bid_ceiling_micros,
      CAST(target_roas_cpc_bid_floor_micros AS STRING)   AS target_roas_cpc_bid_floor_micros,
      CAST(target_roas_target_roas AS STRING)            AS target_roas_target_roas,

      CAST(target_search_network AS STRING)          AS target_search_network,
      CAST(target_spend_cpc_bid_ceiling_micros AS STRING) AS target_spend_cpc_bid_ceiling_micros,
      CAST(target_spend_micros AS STRING)            AS target_spend_micros,

      CAST(tracking_url AS STRING)                   AS tracking_url,
      CAST(tracking_url_template AS STRING)          AS tracking_url_template,

      CAST(url_custom_parameters AS STRING)          AS url_custom_parameters,
      CAST(url_expansion_opt_out AS STRING)          AS url_expansion_opt_out,
      CAST(use_supplied_urls_only AS STRING)         AS use_supplied_urls_only,
      CAST(use_vehicle_inventory AS STRING)          AS use_vehicle_inventory,

      -- -----------------------------
      -- Ingestion metadata
      -- -----------------------------
      CAST(File_Load_datetime AS DATETIME) AS file_load_datetime,
      CAST(Filename AS STRING)             AS filename

    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo`
  ),

  -- Deduplicate in case multiple files contain the same key.
  -- Keep the latest by file_load_datetime (and filename as tie-breaker).
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
  T.date = S.date,
  T.account_name = S.account_name,
  T.customer_id = S.customer_id,

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
  T.max_convs_target_cpa_micros = S.max_convs_target_cpa_micros,
  T.max_conv_value_target_roas = S.max_conv_value_target_roas,
  T.merchant_id = S.merchant_id,
  T.campaign_name = S.campaign_name,
  T.negative_geo_target_type = S.negative_geo_target_type,
  T.optimization_goal_types = S.optimization_goal_types,
  T.opt_in = S.opt_in,
  T.percent_cpc_cpc_bid_ceiling_micros = S.percent_cpc_cpc_bid_ceiling_micros,

  -- ✅ Correct field:
  T.percent_cpc_enhanced_cpc_enabled = S.percent_cpc_enhanced_cpc_enabled,

  T.positive_geo_target_type = S.positive_geo_target_type,
  T.resource_name = S.resource_name,
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
  T.file_load_datetime = S.file_load_datetime,
  T.filename = S.filename

WHEN NOT MATCHED THEN INSERT (
  account_id,
  campaign_id,
  date_yyyymmdd,
  date,
  account_name,
  customer_id,
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
  max_convs_target_cpa_micros,
  max_conv_value_target_roas,
  merchant_id,
  campaign_name,
  negative_geo_target_type,
  optimization_goal_types,
  opt_in,
  percent_cpc_cpc_bid_ceiling_micros,
  percent_cpc_enhanced_cpc_enabled,
  positive_geo_target_type,
  resource_name,
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
  use_vehicle_inventory,
  file_load_datetime,
  filename
)
VALUES (
  S.account_id,
  S.campaign_id,
  S.date_yyyymmdd,
  S.date,
  S.account_name,
  S.customer_id,
  S.ad_serving_optimization_status,
  S.advertising_channel_sub_type,
  S.advertising_channel_type,
  S.bidding_strategy,
  S.bidding_strategy_id,
  S.bidding_strategy_system_status,
  S.bidding_strategy_type,
  S.campaign_budget,
  S.campaign_priority,
  S.conversion_actions,
  S.creation_time,
  S.domain_name,
  S.enable_local,
  S.end_date,
  S.engine_id,
  S.excluded_parent_asset_field_types,
  S.feed_label,
  S.final_url_suffix,
  S.frequency_caps,
  S.labels,
  S.language_code,
  S.manual_cpa,
  S.manual_cpc_enhanced_cpc_enabled,
  S.manual_cpm,
  S.max_convs_target_cpa_micros,
  S.max_conv_value_target_roas,
  S.merchant_id,
  S.campaign_name,
  S.negative_geo_target_type,
  S.optimization_goal_types,
  S.opt_in,
  S.percent_cpc_cpc_bid_ceiling_micros,
  S.percent_cpc_enhanced_cpc_enabled,
  S.positive_geo_target_type,
  S.resource_name,
  S.sales_country,
  S.serving_status,
  S.start_date,
  S.status,
  S.target_content_network,
  S.target_cpa_cpc_bid_ceiling_micros,
  S.target_cpa_cpc_bid_floor_micros,
  S.target_cpa_target_cpa_micros,
  S.target_cpm,
  S.target_google_search,
  S.target_imp_share_cpc_bid_ceiling_micros,
  S.target_imp_share_location,
  S.target_imp_share_location_fraction_micros,
  S.target_partner_search_network,
  S.target_roas_cpc_bid_ceiling_micros,
  S.target_roas_cpc_bid_floor_micros,
  S.target_roas_target_roas,
  S.target_search_network,
  S.target_spend_cpc_bid_ceiling_micros,
  S.target_spend_micros,
  S.tracking_url,
  S.tracking_url_template,
  S.url_custom_parameters,
  S.url_expansion_opt_out,
  S.use_supplied_urls_only,
  S.use_vehicle_inventory,
  S.file_load_datetime,
  S.filename
);
