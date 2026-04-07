/* =================================================================================================
FILE: 05_vw_sdi_tsd_bronze_sa360Entity_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_sa360Entity_daily

SOURCE:
  Replace with your SA360 campaign/settings source table

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Campaign_daily

PURPOSE:
  Canonical Bronze SA360 campaign metadata daily view for the Total Search Dashboard.
  This view deduplicates the SA360 campaign settings/entity snapshot and preserves
  campaign-level dimensions used for brand/nonbrand logic and channel classification.

BUSINESS GRAIN:
  One row per:
      account_id
      campaign_id
      event_date

DEDUPE LOGIC:
  Latest row per:
      account_id + campaign_id + date_yyyymmdd
  ordered by:
      file_load_datetime DESC,
      filename DESC

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Entity_daily`
AS

WITH ranked AS (
    SELECT
        account_id,
        campaign_id,
        date_yyyymmdd,
        DATE(date) AS event_date,

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
        filename,

        ROW_NUMBER() OVER (
            PARTITION BY account_id, campaign_id, date_yyyymmdd
            ORDER BY
                file_load_datetime DESC,
                filename DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity_daily`
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