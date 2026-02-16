/*
===============================================================================
BRONZE | SA360 | CAMPAIGN ENTITY | INCREMENTAL MERGE (NORMALIZED)
===============================================================================

PURPOSE
-------
Incrementally load snapshot-based campaign entity metadata
from:

  google_search_ads_360_beta_campaign_entity_custom_tmo

DESIGN
------
• Snapshot style ingestion
• 7-day rolling lookback
• Idempotent MERGE
• Explicit column mapping
• Normalized output schema
• Raw names documented in bootstrap table

GRAIN
-----
account_id + campaign_id + date_yyyymmdd

PARTITION
---------
snapshot_date

CLUSTER
-------
account_id, campaign_id
===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` T

USING (

  SELECT

    /* =============================
       IDENTIFIERS
    ============================== */

    account_id,
    account_name,
    campaign_id,
    resource_name,
    customer_id,

    /* =============================
       SNAPSHOT DATE
    ============================== */

    date_yyyymmdd,
    PARSE_DATE('%Y%m%d', date_yyyymmdd) AS snapshot_date,

    /* =============================
       LOAD METADATA
    ============================== */

    File_Load_datetime AS file_load_datetime,
    Filename AS filename,

    /* =============================
       CONFIGURATION
    ============================== */

    name,
    advertising_channel_type,
    advertising_channel_sub_type,
    status,
    serving_status,
    campaign_budget,
    campaign_priority,
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

    /* =============================
       BIDDING
    ============================== */

    bidding_strategy,
    bidding_strategy_id,
    bidding_strategy_type,
    bidding_strategy_system_status,
    manual_cpa,
    manual_cpm,
    manual_cpc_enhanced_cpc_enabled,
    max_conv_value_target_roas,
    max_convs_target_cpa_micros,
    target_cpa_target_cpa_micros,
    target_cpa_cpc_bid_ceiling_micros,
    target_cpa_cpc_bid_floor_micros,
    target_roas_target_roas,
    target_roas_cpc_bid_ceiling_micros,
    target_roas_cpc_bid_floor_micros,
    target_spend_micros,
    target_spend_cpc_bid_ceiling_micros,

    percent_cpc_cpc_bid_ceiling_micros AS percent_cpc_bid_ceiling_micros,
    percent_cpc_enhanced_cpc_enabled,

    target_imp_share_location,
    target_imp_share_location_fraction_micros,
    target_imp_share_cpc_bid_ceiling_micros,
    target_cpm,
    optimization_goal_types,
    conversion_actions,

    /* =============================
       TARGETING
    ============================== */

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

    /* =============================
       TRACKING
    ============================== */

    tracking_url,
    tracking_url_template,
    url_custom_parameters,

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

/* ============================================================================
   UPDATE — Only mutable configuration fields
============================================================================ */

WHEN MATCHED THEN
  UPDATE SET

    serving_status = S.serving_status,
    status = S.status,
    campaign_budget = S.campaign_budget,
    bidding_strategy_type = S.bidding_strategy_type,
    bidding_strategy_system_status = S.bidding_strategy_system_status,
    file_load_datetime = S.file_load_datetime,
    filename = S.filename,
    bronze_inserted_at = CURRENT_TIMESTAMP()

/* ============================================================================
   INSERT — FULL SNAPSHOT ROW
============================================================================ */

WHEN NOT MATCHED THEN
INSERT (

account_id,
account_name,
campaign_id,
resource_name,
customer_id,

date_yyyymmdd,
snapshot_date,

file_load_datetime,
filename,
bronze_inserted_at,

name,
advertising_channel_type,
advertising_channel_sub_type,
status,
serving_status,
campaign_budget,
campaign_priority,
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
target_cpa_target_cpa_micros,
target_cpa_cpc_bid_ceiling_micros,
target_cpa_cpc_bid_floor_micros,
target_roas_target_roas,
target_roas_cpc_bid_ceiling_micros,
target_roas_cpc_bid_floor_micros,
target_spend_micros,
target_spend_cpc_bid_ceiling_micros,
percent_cpc_bid_ceiling_micros,
percent_cpc_enhanced_cpc_enabled,
target_imp_share_location,
target_imp_share_location_fraction_micros,
target_imp_share_cpc_bid_ceiling_micros,
target_cpm,
optimization_goal_types,
conversion_actions,

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

tracking_url,
tracking_url_template,
url_custom_parameters

)

VALUES (

S.account_id,
S.account_name,
S.campaign_id,
S.resource_name,
S.customer_id,

S.date_yyyymmdd,
S.snapshot_date,

S.file_load_datetime,
S.filename,
S.bronze_inserted_at,

S.name,
S.advertising_channel_type,
S.advertising_channel_sub_type,
S.status,
S.serving_status,
S.campaign_budget,
S.campaign_priority,
S.creation_time,
S.start_date,
S.end_date,
S.sales_country,
S.domain_name,
S.engine_id,
S.merchant_id,
S.feed_label,
S.labels,
S.language_code,
S.opt_in,

S.bidding_strategy,
S.bidding_strategy_id,
S.bidding_strategy_type,
S.bidding_strategy_system_status,
S.manual_cpa,
S.manual_cpm,
S.manual_cpc_enhanced_cpc_enabled,
S.max_conv_value_target_roas,
S.max_convs_target_cpa_micros,
S.target_cpa_target_cpa_micros,
S.target_cpa_cpc_bid_ceiling_micros,
S.target_cpa_cpc_bid_floor_micros,
S.target_roas_target_roas,
S.target_roas_cpc_bid_ceiling_micros,
S.target_roas_cpc_bid_floor_micros,
S.target_spend_micros,
S.target_spend_cpc_bid_ceiling_micros,
S.percent_cpc_bid_ceiling_micros,
S.percent_cpc_enhanced_cpc_enabled,
S.target_imp_share_location,
S.target_imp_share_location_fraction_micros,
S.target_imp_share_cpc_bid_ceiling_micros,
S.target_cpm,
S.optimization_goal_types,
S.conversion_actions,

S.positive_geo_target_type,
S.negative_geo_target_type,
S.target_search_network,
S.target_partner_search_network,
S.target_google_search,
S.target_content_network,
S.enable_local,
S.frequency_caps,
S.excluded_parent_asset_field_types,
S.use_supplied_urls_only,
S.url_expansion_opt_out,
S.use_vehicle_inventory,

S.tracking_url,
S.tracking_url_template,
S.url_custom_parameters

);
