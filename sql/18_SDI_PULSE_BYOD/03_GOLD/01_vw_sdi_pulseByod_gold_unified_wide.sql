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

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide

PURPOSE:
  Gold Wide view for the Pulse BYOD pipeline.
  Spine join of all 5 Silver views on week_sun_to_sat.
  One row per week with all metrics from all sources as columns.
  All metric columns are source-prefixed (profound_, gofish_, sa360_,
  gsc_, trends_) so every column is unambiguous.
  Per-source data_source and channel columns included for future
  compatibility when Adobe brings multiple channels per source.
  time_granularity = 'WEEKLY' for self-describing schema.
  Used for: ad-hoc analysis, Excel exports, and as the source
  for Gold Long unpivot.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

COLUMN ORDER:
  1. week_sun_to_sat   — time dimension
  2. time_granularity  — always 'WEEKLY'
  3. Per source blocks in order:
       {source}_data_source
       {source}_channel
       {source}_max_data_date
       {source}_{metric}
       {source}_{metric}_wow
       {source}_{metric}_ly
       {source}_{metric}_wow_pct
       {source}_{metric}_yoy_pct

JOIN LOGIC:
  - Spine: DISTINCT week_sun_to_sat from all Silver views via UNION DISTINCT
  - LEFT JOIN each Silver view on week_sun_to_sat
  - NULL where source has no data for a given week

KEY MODELING NOTES:
  - No computation in Gold Wide — all WoW/LY/pct done in Silver
  - No ORDER BY in CTEs — final ORDER BY on week_sun_to_sat only
  - Adding Adobe: create Bronze + Silver views, add LEFT JOIN here,
    add adobe_data_source and adobe_channel columns

DOWNSTREAM:
  Gold Long : vw_sdi_pulseByod_gold_unified_long

AUTHOR:       Pulse BYOD Team
CREATED:      2026-05-24
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
AS

-- -----------------------------------------------------------------------
-- SPINE: All distinct week_sun_to_sat values across all Silver sources
-- Ensures every week appears even if one source has no data
-- -----------------------------------------------------------------------
WITH spine AS (
    SELECT DISTINCT week_sun_to_sat
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`
    UNION DISTINCT
    SELECT DISTINCT week_sun_to_sat
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`
    UNION DISTINCT
    SELECT DISTINCT week_sun_to_sat
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`
    UNION DISTINCT
    SELECT DISTINCT week_sun_to_sat
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`
    UNION DISTINCT
    SELECT DISTINCT week_sun_to_sat
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
)

SELECT
    -- -----------------------------------------------------------------------
    -- TIME DIMENSIONS
    -- -----------------------------------------------------------------------
    s.week_sun_to_sat,
    'WEEKLY'                                        AS time_granularity,

    -- -----------------------------------------------------------------------
    -- PROFOUND: NON-BRAND AI Visibility
    -- -----------------------------------------------------------------------
    p.data_source                                   AS profound_data_source,
    p.channel                                       AS profound_channel,
    p.max_data_date                                 AS profound_max_data_date,

    -- T-Mobile visibility_score
    p.profound_tmo_nonbrand_visibility_score,
    p.profound_tmo_nonbrand_visibility_score_wow,
    p.profound_tmo_nonbrand_visibility_score_ly,
    p.profound_tmo_nonbrand_visibility_score_wow_pct,
    p.profound_tmo_nonbrand_visibility_score_yoy_pct,

    -- T-Mobile executions
    p.profound_tmo_nonbrand_executions,
    p.profound_tmo_nonbrand_executions_wow,
    p.profound_tmo_nonbrand_executions_ly,
    p.profound_tmo_nonbrand_executions_wow_pct,
    p.profound_tmo_nonbrand_executions_yoy_pct,

    -- T-Mobile mentions_count
    p.profound_tmo_nonbrand_mentions_count,
    p.profound_tmo_nonbrand_mentions_count_wow,
    p.profound_tmo_nonbrand_mentions_count_ly,
    p.profound_tmo_nonbrand_mentions_count_wow_pct,
    p.profound_tmo_nonbrand_mentions_count_yoy_pct,

    -- T-Mobile share_of_voice
    p.profound_tmo_nonbrand_share_of_voice,
    p.profound_tmo_nonbrand_share_of_voice_wow,
    p.profound_tmo_nonbrand_share_of_voice_ly,
    p.profound_tmo_nonbrand_share_of_voice_wow_pct,
    p.profound_tmo_nonbrand_share_of_voice_yoy_pct,

    -- Verizon visibility_score
    p.profound_verizon_nonbrand_visibility_score,
    p.profound_verizon_nonbrand_visibility_score_wow,
    p.profound_verizon_nonbrand_visibility_score_ly,
    p.profound_verizon_nonbrand_visibility_score_wow_pct,
    p.profound_verizon_nonbrand_visibility_score_yoy_pct,

    -- Verizon executions
    p.profound_verizon_nonbrand_executions,
    p.profound_verizon_nonbrand_executions_wow,
    p.profound_verizon_nonbrand_executions_ly,
    p.profound_verizon_nonbrand_executions_wow_pct,
    p.profound_verizon_nonbrand_executions_yoy_pct,

    -- Verizon mentions_count
    p.profound_verizon_nonbrand_mentions_count,
    p.profound_verizon_nonbrand_mentions_count_wow,
    p.profound_verizon_nonbrand_mentions_count_ly,
    p.profound_verizon_nonbrand_mentions_count_wow_pct,
    p.profound_verizon_nonbrand_mentions_count_yoy_pct,

    -- Verizon share_of_voice
    p.profound_verizon_nonbrand_share_of_voice,
    p.profound_verizon_nonbrand_share_of_voice_wow,
    p.profound_verizon_nonbrand_share_of_voice_ly,
    p.profound_verizon_nonbrand_share_of_voice_wow_pct,
    p.profound_verizon_nonbrand_share_of_voice_yoy_pct,

    -- AT&T visibility_score
    p.profound_att_nonbrand_visibility_score,
    p.profound_att_nonbrand_visibility_score_wow,
    p.profound_att_nonbrand_visibility_score_ly,
    p.profound_att_nonbrand_visibility_score_wow_pct,
    p.profound_att_nonbrand_visibility_score_yoy_pct,

    -- AT&T executions
    p.profound_att_nonbrand_executions,
    p.profound_att_nonbrand_executions_wow,
    p.profound_att_nonbrand_executions_ly,
    p.profound_att_nonbrand_executions_wow_pct,
    p.profound_att_nonbrand_executions_yoy_pct,

    -- AT&T mentions_count
    p.profound_att_nonbrand_mentions_count,
    p.profound_att_nonbrand_mentions_count_wow,
    p.profound_att_nonbrand_mentions_count_ly,
    p.profound_att_nonbrand_mentions_count_wow_pct,
    p.profound_att_nonbrand_mentions_count_yoy_pct,

    -- AT&T share_of_voice
    p.profound_att_nonbrand_share_of_voice,
    p.profound_att_nonbrand_share_of_voice_wow,
    p.profound_att_nonbrand_share_of_voice_ly,
    p.profound_att_nonbrand_share_of_voice_wow_pct,
    p.profound_att_nonbrand_share_of_voice_yoy_pct,

    -- -----------------------------------------------------------------------
    -- GOFISH: BRAND AI Visibility
    -- -----------------------------------------------------------------------
    g.data_source                                   AS gofish_data_source,
    g.channel                                       AS gofish_channel,
    g.max_data_date                                 AS gofish_max_data_date,

    -- T-Mobile visibility_score
    g.gofish_tmo_brand_visibility_score,
    g.gofish_tmo_brand_visibility_score_wow,
    g.gofish_tmo_brand_visibility_score_ly,
    g.gofish_tmo_brand_visibility_score_wow_pct,
    g.gofish_tmo_brand_visibility_score_yoy_pct,

    -- T-Mobile executions
    g.gofish_tmo_brand_executions,
    g.gofish_tmo_brand_executions_wow,
    g.gofish_tmo_brand_executions_ly,
    g.gofish_tmo_brand_executions_wow_pct,
    g.gofish_tmo_brand_executions_yoy_pct,

    -- T-Mobile mentions_count
    g.gofish_tmo_brand_mentions_count,
    g.gofish_tmo_brand_mentions_count_wow,
    g.gofish_tmo_brand_mentions_count_ly,
    g.gofish_tmo_brand_mentions_count_wow_pct,
    g.gofish_tmo_brand_mentions_count_yoy_pct,

    -- T-Mobile share_of_voice
    g.gofish_tmo_brand_share_of_voice,
    g.gofish_tmo_brand_share_of_voice_wow,
    g.gofish_tmo_brand_share_of_voice_ly,
    g.gofish_tmo_brand_share_of_voice_wow_pct,
    g.gofish_tmo_brand_share_of_voice_yoy_pct,

    -- Verizon visibility_score
    g.gofish_verizon_brand_visibility_score,
    g.gofish_verizon_brand_visibility_score_wow,
    g.gofish_verizon_brand_visibility_score_ly,
    g.gofish_verizon_brand_visibility_score_wow_pct,
    g.gofish_verizon_brand_visibility_score_yoy_pct,

    -- Verizon executions
    g.gofish_verizon_brand_executions,
    g.gofish_verizon_brand_executions_wow,
    g.gofish_verizon_brand_executions_ly,
    g.gofish_verizon_brand_executions_wow_pct,
    g.gofish_verizon_brand_executions_yoy_pct,

    -- Verizon mentions_count
    g.gofish_verizon_brand_mentions_count,
    g.gofish_verizon_brand_mentions_count_wow,
    g.gofish_verizon_brand_mentions_count_ly,
    g.gofish_verizon_brand_mentions_count_wow_pct,
    g.gofish_verizon_brand_mentions_count_yoy_pct,

    -- Verizon share_of_voice
    g.gofish_verizon_brand_share_of_voice,
    g.gofish_verizon_brand_share_of_voice_wow,
    g.gofish_verizon_brand_share_of_voice_ly,
    g.gofish_verizon_brand_share_of_voice_wow_pct,
    g.gofish_verizon_brand_share_of_voice_yoy_pct,

    -- AT&T visibility_score
    g.gofish_att_brand_visibility_score,
    g.gofish_att_brand_visibility_score_wow,
    g.gofish_att_brand_visibility_score_ly,
    g.gofish_att_brand_visibility_score_wow_pct,
    g.gofish_att_brand_visibility_score_yoy_pct,

    -- AT&T executions
    g.gofish_att_brand_executions,
    g.gofish_att_brand_executions_wow,
    g.gofish_att_brand_executions_ly,
    g.gofish_att_brand_executions_wow_pct,
    g.gofish_att_brand_executions_yoy_pct,

    -- AT&T mentions_count
    g.gofish_att_brand_mentions_count,
    g.gofish_att_brand_mentions_count_wow,
    g.gofish_att_brand_mentions_count_ly,
    g.gofish_att_brand_mentions_count_wow_pct,
    g.gofish_att_brand_mentions_count_yoy_pct,

    -- AT&T share_of_voice
    g.gofish_att_brand_share_of_voice,
    g.gofish_att_brand_share_of_voice_wow,
    g.gofish_att_brand_share_of_voice_ly,
    g.gofish_att_brand_share_of_voice_wow_pct,
    g.gofish_att_brand_share_of_voice_yoy_pct,

    -- -----------------------------------------------------------------------
    -- SA360: Paid Search Performance
    -- -----------------------------------------------------------------------
    sa.data_source                                  AS sa360_data_source,
    sa.channel                                      AS sa360_channel,
    sa.max_data_date                                AS sa360_max_data_date,

    -- Brand impressions
    sa.sa360_tmo_brand_impressions,
    sa.sa360_tmo_brand_impressions_wow,
    sa.sa360_tmo_brand_impressions_ly,
    sa.sa360_tmo_brand_impressions_wow_pct,
    sa.sa360_tmo_brand_impressions_yoy_pct,

    -- Brand clicks
    sa.sa360_tmo_brand_clicks,
    sa.sa360_tmo_brand_clicks_wow,
    sa.sa360_tmo_brand_clicks_ly,
    sa.sa360_tmo_brand_clicks_wow_pct,
    sa.sa360_tmo_brand_clicks_yoy_pct,

    -- Brand cost
    sa.sa360_tmo_brand_cost,
    sa.sa360_tmo_brand_cost_wow,
    sa.sa360_tmo_brand_cost_ly,
    sa.sa360_tmo_brand_cost_wow_pct,
    sa.sa360_tmo_brand_cost_yoy_pct,

    -- Brand orders
    sa.sa360_tmo_brand_orders,
    sa.sa360_tmo_brand_orders_wow,
    sa.sa360_tmo_brand_orders_ly,
    sa.sa360_tmo_brand_orders_wow_pct,
    sa.sa360_tmo_brand_orders_yoy_pct,

    -- Brand cart_start
    sa.sa360_tmo_brand_cart_start,
    sa.sa360_tmo_brand_cart_start_wow,
    sa.sa360_tmo_brand_cart_start_ly,
    sa.sa360_tmo_brand_cart_start_wow_pct,
    sa.sa360_tmo_brand_cart_start_yoy_pct,

    -- Brand postpaid_pspv
    sa.sa360_tmo_brand_postpaid_pspv,
    sa.sa360_tmo_brand_postpaid_pspv_wow,
    sa.sa360_tmo_brand_postpaid_pspv_ly,
    sa.sa360_tmo_brand_postpaid_pspv_wow_pct,
    sa.sa360_tmo_brand_postpaid_pspv_yoy_pct,

    -- Nonbrand impressions
    sa.sa360_tmo_nonbrand_impressions,
    sa.sa360_tmo_nonbrand_impressions_wow,
    sa.sa360_tmo_nonbrand_impressions_ly,
    sa.sa360_tmo_nonbrand_impressions_wow_pct,
    sa.sa360_tmo_nonbrand_impressions_yoy_pct,

    -- Nonbrand clicks
    sa.sa360_tmo_nonbrand_clicks,
    sa.sa360_tmo_nonbrand_clicks_wow,
    sa.sa360_tmo_nonbrand_clicks_ly,
    sa.sa360_tmo_nonbrand_clicks_wow_pct,
    sa.sa360_tmo_nonbrand_clicks_yoy_pct,

    -- Nonbrand cost
    sa.sa360_tmo_nonbrand_cost,
    sa.sa360_tmo_nonbrand_cost_wow,
    sa.sa360_tmo_nonbrand_cost_ly,
    sa.sa360_tmo_nonbrand_cost_wow_pct,
    sa.sa360_tmo_nonbrand_cost_yoy_pct,

    -- Nonbrand orders
    sa.sa360_tmo_nonbrand_orders,
    sa.sa360_tmo_nonbrand_orders_wow,
    sa.sa360_tmo_nonbrand_orders_ly,
    sa.sa360_tmo_nonbrand_orders_wow_pct,
    sa.sa360_tmo_nonbrand_orders_yoy_pct,

    -- Nonbrand cart_start
    sa.sa360_tmo_nonbrand_cart_start,
    sa.sa360_tmo_nonbrand_cart_start_wow,
    sa.sa360_tmo_nonbrand_cart_start_ly,
    sa.sa360_tmo_nonbrand_cart_start_wow_pct,
    sa.sa360_tmo_nonbrand_cart_start_yoy_pct,

    -- Nonbrand postpaid_pspv
    sa.sa360_tmo_nonbrand_postpaid_pspv,
    sa.sa360_tmo_nonbrand_postpaid_pspv_wow,
    sa.sa360_tmo_nonbrand_postpaid_pspv_ly,
    sa.sa360_tmo_nonbrand_postpaid_pspv_wow_pct,
    sa.sa360_tmo_nonbrand_postpaid_pspv_yoy_pct,

    -- -----------------------------------------------------------------------
    -- GSC: Organic Search Performance
    -- -----------------------------------------------------------------------
    gsc.data_source                                 AS gsc_data_source,
    gsc.channel                                     AS gsc_channel,
    gsc.max_data_date                               AS gsc_max_data_date,

    -- Brand impressions
    gsc.gsc_tmo_brand_impressions,
    gsc.gsc_tmo_brand_impressions_wow,
    gsc.gsc_tmo_brand_impressions_ly,
    gsc.gsc_tmo_brand_impressions_wow_pct,
    gsc.gsc_tmo_brand_impressions_yoy_pct,

    -- Brand clicks
    gsc.gsc_tmo_brand_clicks,
    gsc.gsc_tmo_brand_clicks_wow,
    gsc.gsc_tmo_brand_clicks_ly,
    gsc.gsc_tmo_brand_clicks_wow_pct,
    gsc.gsc_tmo_brand_clicks_yoy_pct,

    -- Nonbrand impressions
    gsc.gsc_tmo_nonbrand_impressions,
    gsc.gsc_tmo_nonbrand_impressions_wow,
    gsc.gsc_tmo_nonbrand_impressions_ly,
    gsc.gsc_tmo_nonbrand_impressions_wow_pct,
    gsc.gsc_tmo_nonbrand_impressions_yoy_pct,

    -- Nonbrand clicks
    gsc.gsc_tmo_nonbrand_clicks,
    gsc.gsc_tmo_nonbrand_clicks_wow,
    gsc.gsc_tmo_nonbrand_clicks_ly,
    gsc.gsc_tmo_nonbrand_clicks_wow_pct,
    gsc.gsc_tmo_nonbrand_clicks_yoy_pct,

    -- -----------------------------------------------------------------------
    -- TRENDS: Market Interest + Keywords (wide)
    -- -----------------------------------------------------------------------
    t.data_source                                   AS trends_data_source,
    t.channel                                       AS trends_channel,
    t.max_data_date                                 AS trends_max_data_date,

    -- byod_index with WoW/LY
    t.trends_byod_index,
    t.trends_byod_index_wow,
    t.trends_byod_index_ly,
    t.trends_byod_index_wow_pct,
    t.trends_byod_index_yoy_pct,

    -- Keywords wide — unpivoted in Gold Long for Top N visualization
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
    t.trends_kw5_change

FROM spine s
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`       p   ON s.week_sun_to_sat = p.week_sun_to_sat
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly` g   ON s.week_sun_to_sat = g.week_sun_to_sat
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`          sa  ON s.week_sun_to_sat = sa.week_sun_to_sat
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`            gsc ON s.week_sun_to_sat = gsc.week_sun_to_sat
LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`   t   ON s.week_sun_to_sat = t.week_sun_to_sat

ORDER BY s.week_sun_to_sat ASC
;