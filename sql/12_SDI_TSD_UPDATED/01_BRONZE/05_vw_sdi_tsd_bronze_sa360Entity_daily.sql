/* =================================================================================================
FILE: 05_vw_sdi_tsd_bronze_sa360Entity_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_sa360Entity_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Entity_daily

PURPOSE:
  Canonical Bronze SA360 entity daily view for the Total Search Dashboard.
  This view deduplicates the SA360 campaign entity/settings snapshot, derives a stable
  campaign name using the latest non-null campaign name fallback, and classifies each
  campaign into campaign_type.

BUSINESS GRAIN:
  One row per:
      account_id
      campaign_id
      event_date

DEDUPE LOGIC:
  Latest row per:
      account_id + campaign_id + date_yyyymmdd
  ordered by:
      File_Load_datetime DESC,
      Filename DESC

CAMPAIGN TYPE LOGIC:
  Derived from campaign_name with fallback to latest non-null campaign name:
      Brand
      Generic
      Shopping
      PMax
      DemandGen
      Unclassified

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Entity_daily`
AS

WITH base AS (
    SELECT
        raw.account_id,
        raw.campaign_id,
        CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING)) AS event_date,

        raw.account_name,
        raw.customer_id,
        raw.name AS campaign_name,

        raw.ad_serving_optimization_status,
        raw.advertising_channel_sub_type,
        raw.advertising_channel_type,
        raw.bidding_strategy,
        raw.bidding_strategy_id,
        raw.bidding_strategy_system_status,
        raw.bidding_strategy_type,
        raw.campaign_budget,
        raw.campaign_priority,
        raw.conversion_actions,
        raw.creation_time,
        raw.domain_name,
        raw.enable_local,
        raw.end_date,
        raw.engine_id,
        raw.excluded_parent_asset_field_types,
        raw.feed_label,
        raw.final_url_suffix,
        raw.frequency_caps,
        raw.labels,
        raw.language_code,
        raw.manual_cpa,
        raw.manual_cpc_enhanced_cpc_enabled,
        raw.manual_cpm,
        raw.max_convs_target_cpa_micros,
        raw.max_conv_value_target_roas,
        raw.merchant_id,
        raw.negative_geo_target_type,
        raw.optimization_goal_types,
        raw.opt_in,
        raw.percent_cpc_cpc_bid_ceiling_micros,
        raw.percent_cpc_enhanced_cpc_enabled,
        raw.positive_geo_target_type,
        raw.resource_name,
        raw.sales_country,
        raw.serving_status,
        raw.start_date,
        raw.status,
        raw.target_content_network,
        raw.target_cpa_cpc_bid_ceiling_micros,
        raw.target_cpa_cpc_bid_floor_micros,
        raw.target_cpa_target_cpa_micros,
        raw.target_cpm,
        raw.target_google_search,
        raw.target_imp_share_cpc_bid_ceiling_micros,
        raw.target_imp_share_location,
        raw.target_imp_share_location_fraction_micros,
        raw.target_partner_search_network,
        raw.target_roas_cpc_bid_ceiling_micros,
        raw.target_roas_cpc_bid_floor_micros,
        raw.target_roas_target_roas,
        raw.target_search_network,
        raw.target_spend_cpc_bid_ceiling_micros,
        raw.target_spend_micros,
        raw.tracking_url,
        raw.tracking_url_template,
        raw.url_custom_parameters,
        raw.url_expansion_opt_out,
        raw.use_supplied_urls_only,
        raw.use_vehicle_inventory,

        DATETIME(raw.File_Load_datetime) AS file_load_datetime,
        raw.Filename AS filename
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo` raw
),

nonnull_name AS (
    SELECT
        account_id,
        campaign_id,
        campaign_name AS latest_nonnull_campaign_name
    FROM (
        SELECT
            account_id,
            campaign_id,
            campaign_name,
            ROW_NUMBER() OVER (
                PARTITION BY account_id, campaign_id
                ORDER BY file_load_datetime DESC, filename DESC
            ) AS rn
        FROM base
        WHERE NULLIF(campaign_name, '') IS NOT NULL
    )
    WHERE rn = 1
),

ranked AS (
    SELECT
        a.account_id,
        a.campaign_id,
        a.date_yyyymmdd,
        a.event_date,

        a.account_name,
        a.customer_id,
        a.campaign_name,
        n.latest_nonnull_campaign_name,

        CASE
          WHEN COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name) IS NULL THEN 'Unclassified'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'(^|[^a-z])brand([^a-z]|$)') THEN 'Brand'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'(^|[^a-z])generic([^a-z]|$)') THEN 'Generic'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'shopping|shop') THEN 'Shopping'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'pmax|performance\s*max') THEN 'PMax'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'demand\s*gen|demandgen') THEN 'DemandGen'
          ELSE 'Unclassified'
        END AS campaign_type,

        a.ad_serving_optimization_status,
        a.advertising_channel_sub_type,
        a.advertising_channel_type,
        a.bidding_strategy,
        a.bidding_strategy_id,
        a.bidding_strategy_system_status,
        a.bidding_strategy_type,
        a.campaign_budget,
        a.campaign_priority,
        a.conversion_actions,
        a.creation_time,
        a.domain_name,
        a.enable_local,
        a.end_date,
        a.engine_id,
        a.excluded_parent_asset_field_types,
        a.feed_label,
        a.final_url_suffix,
        a.frequency_caps,
        a.labels,
        a.language_code,
        a.manual_cpa,
        a.manual_cpc_enhanced_cpc_enabled,
        a.manual_cpm,
        a.max_convs_target_cpa_micros,
        a.max_conv_value_target_roas,
        a.merchant_id,
        a.negative_geo_target_type,
        a.optimization_goal_types,
        a.opt_in,
        a.percent_cpc_cpc_bid_ceiling_micros,
        a.percent_cpc_enhanced_cpc_enabled,
        a.positive_geo_target_type,
        a.resource_name,
        a.sales_country,
        a.serving_status,
        a.start_date,
        a.status,
        a.target_content_network,
        a.target_cpa_cpc_bid_ceiling_micros,
        a.target_cpa_cpc_bid_floor_micros,
        a.target_cpa_target_cpa_micros,
        a.target_cpm,
        a.target_google_search,
        a.target_imp_share_cpc_bid_ceiling_micros,
        a.target_imp_share_location,
        a.target_imp_share_location_fraction_micros,
        a.target_partner_search_network,
        a.target_roas_cpc_bid_ceiling_micros,
        a.target_roas_cpc_bid_floor_micros,
        a.target_roas_target_roas,
        a.target_search_network,
        a.target_spend_cpc_bid_ceiling_micros,
        a.target_spend_micros,
        a.tracking_url,
        a.tracking_url_template,
        a.url_custom_parameters,
        a.url_expansion_opt_out,
        a.use_supplied_urls_only,
        a.use_vehicle_inventory,

        a.file_load_datetime,
        a.filename,

        ROW_NUMBER() OVER (
            PARTITION BY a.account_id, a.campaign_id, a.date_yyyymmdd
            ORDER BY a.file_load_datetime DESC, a.filename DESC
        ) AS rn
    FROM base a
    LEFT JOIN nonnull_name n
      ON a.account_id = n.account_id
     AND a.campaign_id = n.campaign_id
)

SELECT
    account_id,
    campaign_id,
    date_yyyymmdd,
    event_date,
    account_name,
    customer_id,
    campaign_name,
    latest_nonnull_campaign_name,
    campaign_type,
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
FROM ranked
WHERE rn = 1;