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
  This view deduplicates the SA360 campaign entity/settings snapshot and preserves
  campaign-level metadata used for brand / nonbrand classification and reporting joins.

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

KEY MODELING NOTES:
  - This Bronze object stays close to source structure
  - No business classification is applied here
  - This view is later joined to performance Bronze for campaign type mapping
  - This query assumes campaign_type exists in the raw entity source

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Entity_daily`
AS

WITH ranked AS (
    SELECT
        raw.account_id,
        raw.campaign_id,
        CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING)) AS event_date,

        raw.account_name,
        raw.customer_id,

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
        raw.name AS campaign_name,
        raw.campaign_type,

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
        raw.Filename AS filename,

        ROW_NUMBER() OVER (
            PARTITION BY raw.account_id, raw.campaign_id, CAST(raw.date_yyyymmdd AS STRING)
            ORDER BY
                DATETIME(raw.File_Load_datetime) DESC,
                raw.Filename DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo` raw
)

SELECT
    account_id,
    campaign_id,
    date_yyyymmdd,
    event_date,

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
    campaign_type,

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