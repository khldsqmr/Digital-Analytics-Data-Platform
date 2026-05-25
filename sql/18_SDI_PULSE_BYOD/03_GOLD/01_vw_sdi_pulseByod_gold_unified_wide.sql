/* =================================================================================================
FILE:         01_vw_sdi_pulseByod_gold_unified_wide.sql
LAYER:        Gold View — Wide
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_gold_unified_wide

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide

PURPOSE:
  Gold Wide view for the Pulse BYOD pipeline.
  One row per week with all metrics from all sources as columns.
  All metric columns are source-prefixed for unambiguous identification.
  No data_source or channel columns — source and channel are implicit
  in the column prefix (e.g. sa360_ = SA360 / PAID SEARCH).
  time_granularity = 'WEEKLY' for self-describing schema.
  Used for: ad-hoc analysis and Excel exports.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

SOURCE PREFIX MAPPING:
  profound_  → PROFOUND   / AI SEARCH
  gofish_    → GOFISH     / AI SEARCH
  sa360_     → SA360      / PAID SEARCH
  gsc_       → GSC        / ORGANIC SEARCH
  trends_    → TRENDS     / ORGANIC SEARCH
  adobe_     → ADOBE      / multiple channels (allChannels, paidSearch,
                            organicSearch, direct, social, programmatic, other)

COLUMN ORDER PER SOURCE BLOCK:
  {source}_max_data_date
  {source}_{metric}           — current value
  {source}_{metric}_wow       — prior week value
  {source}_{metric}_ly        — same week last year
  {source}_{metric}_wow_pct   — WoW% as decimal
  {source}_{metric}_yoy_pct   — YoY% as decimal

PERFORMANCE NOTES:
  - FULL OUTER JOIN chain — each Silver view scanned exactly once
  - SA360 used as base (longest history ~1 year)
  - COALESCE on week_sun_to_sat ensures no week is lost even if
    one source has no data for that week
  - Silver views are already 1 row per week — joins are tiny and cheap
  - No spine CTE — avoids double scanning

ADDING NEW SOURCES:
  1. Create Bronze + Silver views
  2. Add new Silver as FULL OUTER JOIN
  3. Add new source name to COALESCE on week_sun_to_sat
  4. Add new source columns in appropriate block

DOWNSTREAM:
  Gold Long : vw_sdi_pulseByod_gold_unified_long (reads Silver directly)

AUTHOR:       Pulse BYOD Team
CREATED:      2026-05-24
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
AS

SELECT

    -- -----------------------------------------------------------------------
    -- TIME DIMENSIONS
    -- COALESCE across all sources ensures no week is lost
    -- even if one source has no data for that week
    -- SA360 first — longest history
    -- -----------------------------------------------------------------------
    COALESCE(
        sa.week_sun_to_sat,
        gsc.week_sun_to_sat,
        p.week_sun_to_sat,
        g.week_sun_to_sat,
        t.week_sun_to_sat,
        ab.week_sun_to_sat
    )                                                                   AS week_sun_to_sat,
    'WEEKLY'                                                            AS time_granularity,

    -- -----------------------------------------------------------------------
    -- PROFOUND: NON-BRAND AI Visibility
    -- prefix: profound_  |  channel: AI SEARCH
    -- -----------------------------------------------------------------------
    p.max_data_date                                                     AS profound_max_data_date,

    -- T-Mobile
    p.profound_tmo_nonbrand_visibility_score,
    p.profound_tmo_nonbrand_visibility_score_wow,
    p.profound_tmo_nonbrand_visibility_score_ly,
    p.profound_tmo_nonbrand_visibility_score_wow_pct,
    p.profound_tmo_nonbrand_visibility_score_yoy_pct,

    p.profound_tmo_nonbrand_executions,
    p.profound_tmo_nonbrand_executions_wow,
    p.profound_tmo_nonbrand_executions_ly,
    p.profound_tmo_nonbrand_executions_wow_pct,
    p.profound_tmo_nonbrand_executions_yoy_pct,

    p.profound_tmo_nonbrand_mentions_count,
    p.profound_tmo_nonbrand_mentions_count_wow,
    p.profound_tmo_nonbrand_mentions_count_ly,
    p.profound_tmo_nonbrand_mentions_count_wow_pct,
    p.profound_tmo_nonbrand_mentions_count_yoy_pct,

    p.profound_tmo_nonbrand_share_of_voice,
    p.profound_tmo_nonbrand_share_of_voice_wow,
    p.profound_tmo_nonbrand_share_of_voice_ly,
    p.profound_tmo_nonbrand_share_of_voice_wow_pct,
    p.profound_tmo_nonbrand_share_of_voice_yoy_pct,

    -- Verizon
    p.profound_verizon_nonbrand_visibility_score,
    p.profound_verizon_nonbrand_visibility_score_wow,
    p.profound_verizon_nonbrand_visibility_score_ly,
    p.profound_verizon_nonbrand_visibility_score_wow_pct,
    p.profound_verizon_nonbrand_visibility_score_yoy_pct,

    p.profound_verizon_nonbrand_executions,
    p.profound_verizon_nonbrand_executions_wow,
    p.profound_verizon_nonbrand_executions_ly,
    p.profound_verizon_nonbrand_executions_wow_pct,
    p.profound_verizon_nonbrand_executions_yoy_pct,

    p.profound_verizon_nonbrand_mentions_count,
    p.profound_verizon_nonbrand_mentions_count_wow,
    p.profound_verizon_nonbrand_mentions_count_ly,
    p.profound_verizon_nonbrand_mentions_count_wow_pct,
    p.profound_verizon_nonbrand_mentions_count_yoy_pct,

    p.profound_verizon_nonbrand_share_of_voice,
    p.profound_verizon_nonbrand_share_of_voice_wow,
    p.profound_verizon_nonbrand_share_of_voice_ly,
    p.profound_verizon_nonbrand_share_of_voice_wow_pct,
    p.profound_verizon_nonbrand_share_of_voice_yoy_pct,

    -- AT&T
    p.profound_att_nonbrand_visibility_score,
    p.profound_att_nonbrand_visibility_score_wow,
    p.profound_att_nonbrand_visibility_score_ly,
    p.profound_att_nonbrand_visibility_score_wow_pct,
    p.profound_att_nonbrand_visibility_score_yoy_pct,

    p.profound_att_nonbrand_executions,
    p.profound_att_nonbrand_executions_wow,
    p.profound_att_nonbrand_executions_ly,
    p.profound_att_nonbrand_executions_wow_pct,
    p.profound_att_nonbrand_executions_yoy_pct,

    p.profound_att_nonbrand_mentions_count,
    p.profound_att_nonbrand_mentions_count_wow,
    p.profound_att_nonbrand_mentions_count_ly,
    p.profound_att_nonbrand_mentions_count_wow_pct,
    p.profound_att_nonbrand_mentions_count_yoy_pct,

    p.profound_att_nonbrand_share_of_voice,
    p.profound_att_nonbrand_share_of_voice_wow,
    p.profound_att_nonbrand_share_of_voice_ly,
    p.profound_att_nonbrand_share_of_voice_wow_pct,
    p.profound_att_nonbrand_share_of_voice_yoy_pct,

    -- -----------------------------------------------------------------------
    -- GOFISH: BRAND AI Visibility
    -- prefix: gofish_  |  channel: AI SEARCH
    -- -----------------------------------------------------------------------
    g.max_data_date                                                     AS gofish_max_data_date,

    -- T-Mobile
    g.gofish_tmo_brand_visibility_score,
    g.gofish_tmo_brand_visibility_score_wow,
    g.gofish_tmo_brand_visibility_score_ly,
    g.gofish_tmo_brand_visibility_score_wow_pct,
    g.gofish_tmo_brand_visibility_score_yoy_pct,

    g.gofish_tmo_brand_executions,
    g.gofish_tmo_brand_executions_wow,
    g.gofish_tmo_brand_executions_ly,
    g.gofish_tmo_brand_executions_wow_pct,
    g.gofish_tmo_brand_executions_yoy_pct,

    g.gofish_tmo_brand_mentions_count,
    g.gofish_tmo_brand_mentions_count_wow,
    g.gofish_tmo_brand_mentions_count_ly,
    g.gofish_tmo_brand_mentions_count_wow_pct,
    g.gofish_tmo_brand_mentions_count_yoy_pct,

    g.gofish_tmo_brand_share_of_voice,
    g.gofish_tmo_brand_share_of_voice_wow,
    g.gofish_tmo_brand_share_of_voice_ly,
    g.gofish_tmo_brand_share_of_voice_wow_pct,
    g.gofish_tmo_brand_share_of_voice_yoy_pct,

    -- Verizon
    g.gofish_verizon_brand_visibility_score,
    g.gofish_verizon_brand_visibility_score_wow,
    g.gofish_verizon_brand_visibility_score_ly,
    g.gofish_verizon_brand_visibility_score_wow_pct,
    g.gofish_verizon_brand_visibility_score_yoy_pct,

    g.gofish_verizon_brand_executions,
    g.gofish_verizon_brand_executions_wow,
    g.gofish_verizon_brand_executions_ly,
    g.gofish_verizon_brand_executions_wow_pct,
    g.gofish_verizon_brand_executions_yoy_pct,

    g.gofish_verizon_brand_mentions_count,
    g.gofish_verizon_brand_mentions_count_wow,
    g.gofish_verizon_brand_mentions_count_ly,
    g.gofish_verizon_brand_mentions_count_wow_pct,
    g.gofish_verizon_brand_mentions_count_yoy_pct,

    g.gofish_verizon_brand_share_of_voice,
    g.gofish_verizon_brand_share_of_voice_wow,
    g.gofish_verizon_brand_share_of_voice_ly,
    g.gofish_verizon_brand_share_of_voice_wow_pct,
    g.gofish_verizon_brand_share_of_voice_yoy_pct,

    -- AT&T
    g.gofish_att_brand_visibility_score,
    g.gofish_att_brand_visibility_score_wow,
    g.gofish_att_brand_visibility_score_ly,
    g.gofish_att_brand_visibility_score_wow_pct,
    g.gofish_att_brand_visibility_score_yoy_pct,

    g.gofish_att_brand_executions,
    g.gofish_att_brand_executions_wow,
    g.gofish_att_brand_executions_ly,
    g.gofish_att_brand_executions_wow_pct,
    g.gofish_att_brand_executions_yoy_pct,

    g.gofish_att_brand_mentions_count,
    g.gofish_att_brand_mentions_count_wow,
    g.gofish_att_brand_mentions_count_ly,
    g.gofish_att_brand_mentions_count_wow_pct,
    g.gofish_att_brand_mentions_count_yoy_pct,

    g.gofish_att_brand_share_of_voice,
    g.gofish_att_brand_share_of_voice_wow,
    g.gofish_att_brand_share_of_voice_ly,
    g.gofish_att_brand_share_of_voice_wow_pct,
    g.gofish_att_brand_share_of_voice_yoy_pct,

    -- -----------------------------------------------------------------------
    -- SA360: Paid Search Performance
    -- prefix: sa360_  |  channel: PAID SEARCH
    -- -----------------------------------------------------------------------
    sa.max_data_date                                                    AS sa360_max_data_date,

    -- Brand
    sa.sa360_tmo_brand_impressions,
    sa.sa360_tmo_brand_impressions_wow,
    sa.sa360_tmo_brand_impressions_ly,
    sa.sa360_tmo_brand_impressions_wow_pct,
    sa.sa360_tmo_brand_impressions_yoy_pct,

    sa.sa360_tmo_brand_clicks,
    sa.sa360_tmo_brand_clicks_wow,
    sa.sa360_tmo_brand_clicks_ly,
    sa.sa360_tmo_brand_clicks_wow_pct,
    sa.sa360_tmo_brand_clicks_yoy_pct,

    sa.sa360_tmo_brand_cost,
    sa.sa360_tmo_brand_cost_wow,
    sa.sa360_tmo_brand_cost_ly,
    sa.sa360_tmo_brand_cost_wow_pct,
    sa.sa360_tmo_brand_cost_yoy_pct,

    sa.sa360_tmo_brand_orders,
    sa.sa360_tmo_brand_orders_wow,
    sa.sa360_tmo_brand_orders_ly,
    sa.sa360_tmo_brand_orders_wow_pct,
    sa.sa360_tmo_brand_orders_yoy_pct,

    sa.sa360_tmo_brand_cart_start,
    sa.sa360_tmo_brand_cart_start_wow,
    sa.sa360_tmo_brand_cart_start_ly,
    sa.sa360_tmo_brand_cart_start_wow_pct,
    sa.sa360_tmo_brand_cart_start_yoy_pct,

    sa.sa360_tmo_brand_postpaid_pspv,
    sa.sa360_tmo_brand_postpaid_pspv_wow,
    sa.sa360_tmo_brand_postpaid_pspv_ly,
    sa.sa360_tmo_brand_postpaid_pspv_wow_pct,
    sa.sa360_tmo_brand_postpaid_pspv_yoy_pct,

    -- Nonbrand
    sa.sa360_tmo_nonbrand_impressions,
    sa.sa360_tmo_nonbrand_impressions_wow,
    sa.sa360_tmo_nonbrand_impressions_ly,
    sa.sa360_tmo_nonbrand_impressions_wow_pct,
    sa.sa360_tmo_nonbrand_impressions_yoy_pct,

    sa.sa360_tmo_nonbrand_clicks,
    sa.sa360_tmo_nonbrand_clicks_wow,
    sa.sa360_tmo_nonbrand_clicks_ly,
    sa.sa360_tmo_nonbrand_clicks_wow_pct,
    sa.sa360_tmo_nonbrand_clicks_yoy_pct,

    sa.sa360_tmo_nonbrand_cost,
    sa.sa360_tmo_nonbrand_cost_wow,
    sa.sa360_tmo_nonbrand_cost_ly,
    sa.sa360_tmo_nonbrand_cost_wow_pct,
    sa.sa360_tmo_nonbrand_cost_yoy_pct,

    sa.sa360_tmo_nonbrand_orders,
    sa.sa360_tmo_nonbrand_orders_wow,
    sa.sa360_tmo_nonbrand_orders_ly,
    sa.sa360_tmo_nonbrand_orders_wow_pct,
    sa.sa360_tmo_nonbrand_orders_yoy_pct,

    sa.sa360_tmo_nonbrand_cart_start,
    sa.sa360_tmo_nonbrand_cart_start_wow,
    sa.sa360_tmo_nonbrand_cart_start_ly,
    sa.sa360_tmo_nonbrand_cart_start_wow_pct,
    sa.sa360_tmo_nonbrand_cart_start_yoy_pct,

    sa.sa360_tmo_nonbrand_postpaid_pspv,
    sa.sa360_tmo_nonbrand_postpaid_pspv_wow,
    sa.sa360_tmo_nonbrand_postpaid_pspv_ly,
    sa.sa360_tmo_nonbrand_postpaid_pspv_wow_pct,
    sa.sa360_tmo_nonbrand_postpaid_pspv_yoy_pct,

    -- -----------------------------------------------------------------------
    -- GSC: Organic Search Performance
    -- prefix: gsc_  |  channel: ORGANIC SEARCH
    -- -----------------------------------------------------------------------
    gsc.max_data_date                                                   AS gsc_max_data_date,

    -- Brand
    gsc.gsc_tmo_brand_impressions,
    gsc.gsc_tmo_brand_impressions_wow,
    gsc.gsc_tmo_brand_impressions_ly,
    gsc.gsc_tmo_brand_impressions_wow_pct,
    gsc.gsc_tmo_brand_impressions_yoy_pct,

    gsc.gsc_tmo_brand_clicks,
    gsc.gsc_tmo_brand_clicks_wow,
    gsc.gsc_tmo_brand_clicks_ly,
    gsc.gsc_tmo_brand_clicks_wow_pct,
    gsc.gsc_tmo_brand_clicks_yoy_pct,

    -- Nonbrand
    gsc.gsc_tmo_nonbrand_impressions,
    gsc.gsc_tmo_nonbrand_impressions_wow,
    gsc.gsc_tmo_nonbrand_impressions_ly,
    gsc.gsc_tmo_nonbrand_impressions_wow_pct,
    gsc.gsc_tmo_nonbrand_impressions_yoy_pct,

    gsc.gsc_tmo_nonbrand_clicks,
    gsc.gsc_tmo_nonbrand_clicks_wow,
    gsc.gsc_tmo_nonbrand_clicks_ly,
    gsc.gsc_tmo_nonbrand_clicks_wow_pct,
    gsc.gsc_tmo_nonbrand_clicks_yoy_pct,

    -- -----------------------------------------------------------------------
    -- TRENDS: Market Interest + Keywords (wide)
    -- prefix: trends_  |  channel: ORGANIC SEARCH
    -- Keywords kept wide — rank implicit in column name (kw1, kw2 etc.)
    -- Unpivoted with KEYWORD_RANK_1 through KEYWORD_RANK_5 in Gold Long
    -- -----------------------------------------------------------------------
    t.max_data_date                                                     AS trends_max_data_date,

    -- byod_index
    t.trends_byod_index,
    t.trends_byod_index_wow,
    t.trends_byod_index_ly,
    t.trends_byod_index_wow_pct,
    t.trends_byod_index_yoy_pct,

    -- Keywords wide
    t.trends_top_kw_1,
    t.trends_kw1_interest,
    t.trends_kw1_change,
    t.trends_top_kw_2,
    t.trends_kw2_interest,
    t.trends_kw2_change,
    t.trends_top_kw_3,
    t.trends_kw3_interest,
    t.trends_kw3_change,
    t.trends_top_kw_4,
    t.trends_kw4_interest,
    t.trends_kw4_change,
    t.trends_top_kw_5,
    t.trends_kw5_interest,
    t.trends_kw5_change,

    -- -----------------------------------------------------------------------
    -- ADOBE: BYOD Analytics
    -- prefix: adobe_  |  channel: multiple (allChannels, paidSearch, etc.)
    -- -----------------------------------------------------------------------
    ab.max_data_date                                                    AS adobe_max_data_date,

    -- All Channels
    ab.adobe_uvnbByod_allChannels,
    ab.adobe_uvnbByod_allChannels_wow,
    ab.adobe_uvnbByod_allChannels_ly,
    ab.adobe_uvnbByod_allChannels_wow_pct,
    ab.adobe_uvnbByod_allChannels_yoy_pct,

    ab.adobe_uvnbFlowTotal_allChannels,
    ab.adobe_uvnbFlowTotal_allChannels_wow,
    ab.adobe_uvnbFlowTotal_allChannels_ly,
    ab.adobe_uvnbFlowTotal_allChannels_wow_pct,
    ab.adobe_uvnbFlowTotal_allChannels_yoy_pct,

    ab.adobe_uvnbByodPctOfUvnbFlow_allChannels,
    ab.adobe_uvnbByodPctOfUvnbFlow_allChannels_wow,
    ab.adobe_uvnbByodPctOfUvnbFlow_allChannels_ly,
    ab.adobe_uvnbByodPctOfUvnbFlow_allChannels_wow_pct,
    ab.adobe_uvnbByodPctOfUvnbFlow_allChannels_yoy_pct,

    ab.adobe_cartStartByod_allChannels,
    ab.adobe_cartStartByod_allChannels_wow,
    ab.adobe_cartStartByod_allChannels_ly,
    ab.adobe_cartStartByod_allChannels_wow_pct,
    ab.adobe_cartStartByod_allChannels_yoy_pct,

    ab.adobe_ordersUnassistedByod_allChannels,
    ab.adobe_ordersUnassistedByod_allChannels_wow,
    ab.adobe_ordersUnassistedByod_allChannels_ly,
    ab.adobe_ordersUnassistedByod_allChannels_wow_pct,
    ab.adobe_ordersUnassistedByod_allChannels_yoy_pct,

    ab.adobe_ordersAssistedByod_allChannels,
    ab.adobe_ordersAssistedByod_allChannels_wow,
    ab.adobe_ordersAssistedByod_allChannels_ly,
    ab.adobe_ordersAssistedByod_allChannels_wow_pct,
    ab.adobe_ordersAssistedByod_allChannels_yoy_pct,

    ab.adobe_ordersTotalByod_allChannels,
    ab.adobe_ordersTotalByod_allChannels_wow,
    ab.adobe_ordersTotalByod_allChannels_ly,
    ab.adobe_ordersTotalByod_allChannels_wow_pct,
    ab.adobe_ordersTotalByod_allChannels_yoy_pct,

    -- Paid Search
    ab.adobe_uvnbByod_paidSearch,
    ab.adobe_uvnbByod_paidSearch_wow,
    ab.adobe_uvnbByod_paidSearch_ly,
    ab.adobe_uvnbByod_paidSearch_wow_pct,
    ab.adobe_uvnbByod_paidSearch_yoy_pct,

    ab.adobe_cartStartByod_paidSearch,
    ab.adobe_cartStartByod_paidSearch_wow,
    ab.adobe_cartStartByod_paidSearch_ly,
    ab.adobe_cartStartByod_paidSearch_wow_pct,
    ab.adobe_cartStartByod_paidSearch_yoy_pct,

    ab.adobe_ordersUnassistedByod_paidSearch,
    ab.adobe_ordersUnassistedByod_paidSearch_wow,
    ab.adobe_ordersUnassistedByod_paidSearch_ly,
    ab.adobe_ordersUnassistedByod_paidSearch_wow_pct,
    ab.adobe_ordersUnassistedByod_paidSearch_yoy_pct,

    ab.adobe_ordersAssistedByod_paidSearch,
    ab.adobe_ordersAssistedByod_paidSearch_wow,
    ab.adobe_ordersAssistedByod_paidSearch_ly,
    ab.adobe_ordersAssistedByod_paidSearch_wow_pct,
    ab.adobe_ordersAssistedByod_paidSearch_yoy_pct,

    ab.adobe_ordersTotalByod_paidSearch,
    ab.adobe_ordersTotalByod_paidSearch_wow,
    ab.adobe_ordersTotalByod_paidSearch_ly,
    ab.adobe_ordersTotalByod_paidSearch_wow_pct,
    ab.adobe_ordersTotalByod_paidSearch_yoy_pct,

    -- Organic Search
    ab.adobe_uvnbByod_organicSearch,
    ab.adobe_uvnbByod_organicSearch_wow,
    ab.adobe_uvnbByod_organicSearch_ly,
    ab.adobe_uvnbByod_organicSearch_wow_pct,
    ab.adobe_uvnbByod_organicSearch_yoy_pct,

    ab.adobe_cartStartByod_organicSearch,
    ab.adobe_cartStartByod_organicSearch_wow,
    ab.adobe_cartStartByod_organicSearch_ly,
    ab.adobe_cartStartByod_organicSearch_wow_pct,
    ab.adobe_cartStartByod_organicSearch_yoy_pct,

    ab.adobe_ordersUnassistedByod_organicSearch,
    ab.adobe_ordersUnassistedByod_organicSearch_wow,
    ab.adobe_ordersUnassistedByod_organicSearch_ly,
    ab.adobe_ordersUnassistedByod_organicSearch_wow_pct,
    ab.adobe_ordersUnassistedByod_organicSearch_yoy_pct,

    ab.adobe_ordersAssistedByod_organicSearch,
    ab.adobe_ordersAssistedByod_organicSearch_wow,
    ab.adobe_ordersAssistedByod_organicSearch_ly,
    ab.adobe_ordersAssistedByod_organicSearch_wow_pct,
    ab.adobe_ordersAssistedByod_organicSearch_yoy_pct,

    ab.adobe_ordersTotalByod_organicSearch,
    ab.adobe_ordersTotalByod_organicSearch_wow,
    ab.adobe_ordersTotalByod_organicSearch_ly,
    ab.adobe_ordersTotalByod_organicSearch_wow_pct,
    ab.adobe_ordersTotalByod_organicSearch_yoy_pct,

    -- Direct
    ab.adobe_uvnbByod_direct,
    ab.adobe_uvnbByod_direct_wow,
    ab.adobe_uvnbByod_direct_ly,
    ab.adobe_uvnbByod_direct_wow_pct,
    ab.adobe_uvnbByod_direct_yoy_pct,

    ab.adobe_cartStartByod_direct,
    ab.adobe_cartStartByod_direct_wow,
    ab.adobe_cartStartByod_direct_ly,
    ab.adobe_cartStartByod_direct_wow_pct,
    ab.adobe_cartStartByod_direct_yoy_pct,

    ab.adobe_ordersUnassistedByod_direct,
    ab.adobe_ordersUnassistedByod_direct_wow,
    ab.adobe_ordersUnassistedByod_direct_ly,
    ab.adobe_ordersUnassistedByod_direct_wow_pct,
    ab.adobe_ordersUnassistedByod_direct_yoy_pct,

    ab.adobe_ordersAssistedByod_direct,
    ab.adobe_ordersAssistedByod_direct_wow,
    ab.adobe_ordersAssistedByod_direct_ly,
    ab.adobe_ordersAssistedByod_direct_wow_pct,
    ab.adobe_ordersAssistedByod_direct_yoy_pct,

    ab.adobe_ordersTotalByod_direct,
    ab.adobe_ordersTotalByod_direct_wow,
    ab.adobe_ordersTotalByod_direct_ly,
    ab.adobe_ordersTotalByod_direct_wow_pct,
    ab.adobe_ordersTotalByod_direct_yoy_pct,

    -- Social
    ab.adobe_uvnbByod_social,
    ab.adobe_uvnbByod_social_wow,
    ab.adobe_uvnbByod_social_ly,
    ab.adobe_uvnbByod_social_wow_pct,
    ab.adobe_uvnbByod_social_yoy_pct,

    ab.adobe_cartStartByod_social,
    ab.adobe_cartStartByod_social_wow,
    ab.adobe_cartStartByod_social_ly,
    ab.adobe_cartStartByod_social_wow_pct,
    ab.adobe_cartStartByod_social_yoy_pct,

    ab.adobe_ordersUnassistedByod_social,
    ab.adobe_ordersUnassistedByod_social_wow,
    ab.adobe_ordersUnassistedByod_social_ly,
    ab.adobe_ordersUnassistedByod_social_wow_pct,
    ab.adobe_ordersUnassistedByod_social_yoy_pct,

    ab.adobe_ordersAssistedByod_social,
    ab.adobe_ordersAssistedByod_social_wow,
    ab.adobe_ordersAssistedByod_social_ly,
    ab.adobe_ordersAssistedByod_social_wow_pct,
    ab.adobe_ordersAssistedByod_social_yoy_pct,

    ab.adobe_ordersTotalByod_social,
    ab.adobe_ordersTotalByod_social_wow,
    ab.adobe_ordersTotalByod_social_ly,
    ab.adobe_ordersTotalByod_social_wow_pct,
    ab.adobe_ordersTotalByod_social_yoy_pct,

    -- Programmatic
    ab.adobe_uvnbByod_programmatic,
    ab.adobe_uvnbByod_programmatic_wow,
    ab.adobe_uvnbByod_programmatic_ly,
    ab.adobe_uvnbByod_programmatic_wow_pct,
    ab.adobe_uvnbByod_programmatic_yoy_pct,

    ab.adobe_cartStartByod_programmatic,
    ab.adobe_cartStartByod_programmatic_wow,
    ab.adobe_cartStartByod_programmatic_ly,
    ab.adobe_cartStartByod_programmatic_wow_pct,
    ab.adobe_cartStartByod_programmatic_yoy_pct,

    ab.adobe_ordersUnassistedByod_programmatic,
    ab.adobe_ordersUnassistedByod_programmatic_wow,
    ab.adobe_ordersUnassistedByod_programmatic_ly,
    ab.adobe_ordersUnassistedByod_programmatic_wow_pct,
    ab.adobe_ordersUnassistedByod_programmatic_yoy_pct,

    ab.adobe_ordersAssistedByod_programmatic,
    ab.adobe_ordersAssistedByod_programmatic_wow,
    ab.adobe_ordersAssistedByod_programmatic_ly,
    ab.adobe_ordersAssistedByod_programmatic_wow_pct,
    ab.adobe_ordersAssistedByod_programmatic_yoy_pct,

    ab.adobe_ordersTotalByod_programmatic,
    ab.adobe_ordersTotalByod_programmatic_wow,
    ab.adobe_ordersTotalByod_programmatic_ly,
    ab.adobe_ordersTotalByod_programmatic_wow_pct,
    ab.adobe_ordersTotalByod_programmatic_yoy_pct,

    -- Other
    ab.adobe_uvnbByod_other,
    ab.adobe_uvnbByod_other_wow,
    ab.adobe_uvnbByod_other_ly,
    ab.adobe_uvnbByod_other_wow_pct,
    ab.adobe_uvnbByod_other_yoy_pct,

    ab.adobe_cartStartByod_other,
    ab.adobe_cartStartByod_other_wow,
    ab.adobe_cartStartByod_other_ly,
    ab.adobe_cartStartByod_other_wow_pct,
    ab.adobe_cartStartByod_other_yoy_pct,

    ab.adobe_ordersUnassistedByod_other,
    ab.adobe_ordersUnassistedByod_other_wow,
    ab.adobe_ordersUnassistedByod_other_ly,
    ab.adobe_ordersUnassistedByod_other_wow_pct,
    ab.adobe_ordersUnassistedByod_other_yoy_pct,

    ab.adobe_ordersAssistedByod_other,
    ab.adobe_ordersAssistedByod_other_wow,
    ab.adobe_ordersAssistedByod_other_ly,
    ab.adobe_ordersAssistedByod_other_wow_pct,
    ab.adobe_ordersAssistedByod_other_yoy_pct,

    ab.adobe_ordersTotalByod_other,
    ab.adobe_ordersTotalByod_other_wow,
    ab.adobe_ordersTotalByod_other_ly,
    ab.adobe_ordersTotalByod_other_wow_pct,
    ab.adobe_ordersTotalByod_other_yoy_pct

-- -----------------------------------------------------------------------
-- FULL OUTER JOIN chain
-- SA360 as base (longest history)
-- Each Silver view scanned exactly once
-- COALESCE on week_sun_to_sat handles any week where one source
-- has no data
-- -----------------------------------------------------------------------
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`             sa
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`            gsc
    ON sa.week_sun_to_sat = gsc.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`       p
    ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat) = p.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly` g
    ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat) = g.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`   t
    ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat) = t.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly`          ab
    ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat, t.week_sun_to_sat) = ab.week_sun_to_sat

ORDER BY week_sun_to_sat ASC
;