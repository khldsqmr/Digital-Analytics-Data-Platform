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
  All columns are source-prefixed (profound_, gofish_, sa360_, gsc_, trends_)
  so every column is unambiguous without needing a data_source dimension.
  Used for: ad-hoc analysis, Excel exports, and as the source for Gold Long.

BUSINESS GRAIN:
  One row per:
    week_sun_to_sat

JOIN LOGIC:
  - Spine: DISTINCT week_sun_to_sat from all Silver views via UNION
  - LEFT JOIN each Silver view on week_sun_to_sat
  - NULL where source has no data for a given week (e.g. Profound only
    starts 2026-03-22 so earlier weeks have NULL for all profound_ columns)

KEY MODELING NOTES:
  - No computation in Gold Wide — all WoW/LY/pct done in Silver
  - data_source and channel not needed as columns — source implicit in prefix
  - max_data_date per source carried through from Silver
  - No ORDER BY in CTEs — final ORDER BY on week_sun_to_sat only

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
    SELECT DISTINCT week_sun_to_sat FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`
    UNION DISTINCT
    SELECT DISTINCT week_sun_to_sat FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`
    UNION DISTINCT
    SELECT DISTINCT week_sun_to_sat FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`
    UNION DISTINCT
    SELECT DISTINCT week_sun_to_sat FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`
    UNION DISTINCT
    SELECT DISTINCT week_sun_to_sat FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`
)

SELECT
    s.week_sun_to_sat,

    -- -----------------------------------------------------------------------
    -- PROFOUND: NON-BRAND AI Visibility
    -- max_data_date tells dashboard when Profound data was last available
    -- -----------------------------------------------------------------------
    p.max_data_date                                 AS profound_max_data_date,

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
    -- -----------------------------------------------------------------------
    g.max_data_date                                 AS gofish_max_data_date,

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
    -- -----------------------------------------------------------------------
    sa.max_data_date                                AS sa360_max_data_date,

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
    -- -----------------------------------------------------------------------
    gsc.max_data_date                               AS gsc_max_data_date,

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
    -- -----------------------------------------------------------------------
    t.max_data_date                                 AS trends_max_data_date,

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