/* =================================================================================================
FILE:         01_vw_sdi_pulseByod_gold_unified_wide.sql
LAYER:        Gold View — Wide
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_gold_unified_wide

PURPOSE:
  Gold Wide view. One row per week, all metrics from all sources as columns.
  Used for ad-hoc analysis and Excel exports.
  Gold Long (used by dashboard) reads Silver directly.

CHANGES:
  2026-06-04 — Profound CIT & VIS rename:
               VIS columns renamed: profound_{asset}_nonbrand_{metric} → profoundVis_{asset}_nonbrand_{metric}
               CIT columns added: profoundCit_{asset}_nonbrand_shareOfVoice and
               wow/ly/wow_pct/yoy_pct variants (15 new columns total)
  2026-06-04 — Adobe entry pages and outcomes added:
               aep alias: vw_sdi_pulseByod_silver_adobeByodEntryPages_weekly
               ao  alias: vw_sdi_pulseByod_silver_adobeByodOutcomes_weekly
  2026-06-XX — Adobe cvrPostpaid_allChannels and cvrHsi_allChannels added
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_gold_unified_wide`
AS

SELECT
    COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat, t.week_sun_to_sat, ab.week_sun_to_sat, aep.week_sun_to_sat, ao.week_sun_to_sat) AS week_sun_to_sat,
    'WEEKLY' AS time_granularity,

    -- ================================================================ PROFOUND
    p.max_data_date AS profound_max_data_date,
    p.profoundVis_tmo_nonbrand_visibilityScore, p.profoundVis_tmo_nonbrand_visibilityScore_wow, p.profoundVis_tmo_nonbrand_visibilityScore_ly, p.profoundVis_tmo_nonbrand_visibilityScore_wow_pct, p.profoundVis_tmo_nonbrand_visibilityScore_yoy_pct,
    p.profoundVis_tmo_nonbrand_executions, p.profoundVis_tmo_nonbrand_executions_wow, p.profoundVis_tmo_nonbrand_executions_ly, p.profoundVis_tmo_nonbrand_executions_wow_pct, p.profoundVis_tmo_nonbrand_executions_yoy_pct,
    p.profoundVis_tmo_nonbrand_mentionsCount, p.profoundVis_tmo_nonbrand_mentionsCount_wow, p.profoundVis_tmo_nonbrand_mentionsCount_ly, p.profoundVis_tmo_nonbrand_mentionsCount_wow_pct, p.profoundVis_tmo_nonbrand_mentionsCount_yoy_pct,
    p.profoundVis_tmo_nonbrand_shareOfVoice, p.profoundVis_tmo_nonbrand_shareOfVoice_wow, p.profoundVis_tmo_nonbrand_shareOfVoice_ly, p.profoundVis_tmo_nonbrand_shareOfVoice_wow_pct, p.profoundVis_tmo_nonbrand_shareOfVoice_yoy_pct,
    p.profoundCit_tmo_nonbrand_shareOfVoice, p.profoundCit_tmo_nonbrand_shareOfVoice_wow, p.profoundCit_tmo_nonbrand_shareOfVoice_ly, p.profoundCit_tmo_nonbrand_shareOfVoice_wow_pct, p.profoundCit_tmo_nonbrand_shareOfVoice_yoy_pct,
    p.profoundVis_verizon_nonbrand_visibilityScore, p.profoundVis_verizon_nonbrand_visibilityScore_wow, p.profoundVis_verizon_nonbrand_visibilityScore_ly, p.profoundVis_verizon_nonbrand_visibilityScore_wow_pct, p.profoundVis_verizon_nonbrand_visibilityScore_yoy_pct,
    p.profoundVis_verizon_nonbrand_executions, p.profoundVis_verizon_nonbrand_executions_wow, p.profoundVis_verizon_nonbrand_executions_ly, p.profoundVis_verizon_nonbrand_executions_wow_pct, p.profoundVis_verizon_nonbrand_executions_yoy_pct,
    p.profoundVis_verizon_nonbrand_mentionsCount, p.profoundVis_verizon_nonbrand_mentionsCount_wow, p.profoundVis_verizon_nonbrand_mentionsCount_ly, p.profoundVis_verizon_nonbrand_mentionsCount_wow_pct, p.profoundVis_verizon_nonbrand_mentionsCount_yoy_pct,
    p.profoundVis_verizon_nonbrand_shareOfVoice, p.profoundVis_verizon_nonbrand_shareOfVoice_wow, p.profoundVis_verizon_nonbrand_shareOfVoice_ly, p.profoundVis_verizon_nonbrand_shareOfVoice_wow_pct, p.profoundVis_verizon_nonbrand_shareOfVoice_yoy_pct,
    p.profoundCit_verizon_nonbrand_shareOfVoice, p.profoundCit_verizon_nonbrand_shareOfVoice_wow, p.profoundCit_verizon_nonbrand_shareOfVoice_ly, p.profoundCit_verizon_nonbrand_shareOfVoice_wow_pct, p.profoundCit_verizon_nonbrand_shareOfVoice_yoy_pct,
    p.profoundVis_att_nonbrand_visibilityScore, p.profoundVis_att_nonbrand_visibilityScore_wow, p.profoundVis_att_nonbrand_visibilityScore_ly, p.profoundVis_att_nonbrand_visibilityScore_wow_pct, p.profoundVis_att_nonbrand_visibilityScore_yoy_pct,
    p.profoundVis_att_nonbrand_executions, p.profoundVis_att_nonbrand_executions_wow, p.profoundVis_att_nonbrand_executions_ly, p.profoundVis_att_nonbrand_executions_wow_pct, p.profoundVis_att_nonbrand_executions_yoy_pct,
    p.profoundVis_att_nonbrand_mentionsCount, p.profoundVis_att_nonbrand_mentionsCount_wow, p.profoundVis_att_nonbrand_mentionsCount_ly, p.profoundVis_att_nonbrand_mentionsCount_wow_pct, p.profoundVis_att_nonbrand_mentionsCount_yoy_pct,
    p.profoundVis_att_nonbrand_shareOfVoice, p.profoundVis_att_nonbrand_shareOfVoice_wow, p.profoundVis_att_nonbrand_shareOfVoice_ly, p.profoundVis_att_nonbrand_shareOfVoice_wow_pct, p.profoundVis_att_nonbrand_shareOfVoice_yoy_pct,
    p.profoundCit_att_nonbrand_shareOfVoice, p.profoundCit_att_nonbrand_shareOfVoice_wow, p.profoundCit_att_nonbrand_shareOfVoice_ly, p.profoundCit_att_nonbrand_shareOfVoice_wow_pct, p.profoundCit_att_nonbrand_shareOfVoice_yoy_pct,

    -- ================================================================ GOFISH
    g.max_data_date AS gofish_max_data_date,
    g.gofish_tmo_brand_visibilityScore, g.gofish_tmo_brand_visibilityScore_wow, g.gofish_tmo_brand_visibilityScore_ly, g.gofish_tmo_brand_visibilityScore_wow_pct, g.gofish_tmo_brand_visibilityScore_yoy_pct,
    g.gofish_tmo_brand_executions, g.gofish_tmo_brand_executions_wow, g.gofish_tmo_brand_executions_ly, g.gofish_tmo_brand_executions_wow_pct, g.gofish_tmo_brand_executions_yoy_pct,
    g.gofish_tmo_brand_mentionsCount, g.gofish_tmo_brand_mentionsCount_wow, g.gofish_tmo_brand_mentionsCount_ly, g.gofish_tmo_brand_mentionsCount_wow_pct, g.gofish_tmo_brand_mentionsCount_yoy_pct,
    g.gofish_tmo_brand_shareOfVoice, g.gofish_tmo_brand_shareOfVoice_wow, g.gofish_tmo_brand_shareOfVoice_ly, g.gofish_tmo_brand_shareOfVoice_wow_pct, g.gofish_tmo_brand_shareOfVoice_yoy_pct,
    g.gofish_verizon_brand_visibilityScore, g.gofish_verizon_brand_visibilityScore_wow, g.gofish_verizon_brand_visibilityScore_ly, g.gofish_verizon_brand_visibilityScore_wow_pct, g.gofish_verizon_brand_visibilityScore_yoy_pct,
    g.gofish_verizon_brand_executions, g.gofish_verizon_brand_executions_wow, g.gofish_verizon_brand_executions_ly, g.gofish_verizon_brand_executions_wow_pct, g.gofish_verizon_brand_executions_yoy_pct,
    g.gofish_verizon_brand_mentionsCount, g.gofish_verizon_brand_mentionsCount_wow, g.gofish_verizon_brand_mentionsCount_ly, g.gofish_verizon_brand_mentionsCount_wow_pct, g.gofish_verizon_brand_mentionsCount_yoy_pct,
    g.gofish_verizon_brand_shareOfVoice, g.gofish_verizon_brand_shareOfVoice_wow, g.gofish_verizon_brand_shareOfVoice_ly, g.gofish_verizon_brand_shareOfVoice_wow_pct, g.gofish_verizon_brand_shareOfVoice_yoy_pct,
    g.gofish_att_brand_visibilityScore, g.gofish_att_brand_visibilityScore_wow, g.gofish_att_brand_visibilityScore_ly, g.gofish_att_brand_visibilityScore_wow_pct, g.gofish_att_brand_visibilityScore_yoy_pct,
    g.gofish_att_brand_executions, g.gofish_att_brand_executions_wow, g.gofish_att_brand_executions_ly, g.gofish_att_brand_executions_wow_pct, g.gofish_att_brand_executions_yoy_pct,
    g.gofish_att_brand_mentionsCount, g.gofish_att_brand_mentionsCount_wow, g.gofish_att_brand_mentionsCount_ly, g.gofish_att_brand_mentionsCount_wow_pct, g.gofish_att_brand_mentionsCount_yoy_pct,
    g.gofish_att_brand_shareOfVoice, g.gofish_att_brand_shareOfVoice_wow, g.gofish_att_brand_shareOfVoice_ly, g.gofish_att_brand_shareOfVoice_wow_pct, g.gofish_att_brand_shareOfVoice_yoy_pct,

    -- ================================================================ SA360
    sa.max_data_date AS sa360_max_data_date,
    sa.sa360_tmo_brand_impressions, sa.sa360_tmo_brand_impressions_wow, sa.sa360_tmo_brand_impressions_ly, sa.sa360_tmo_brand_impressions_wow_pct, sa.sa360_tmo_brand_impressions_yoy_pct,
    sa.sa360_tmo_brand_clicks, sa.sa360_tmo_brand_clicks_wow, sa.sa360_tmo_brand_clicks_ly, sa.sa360_tmo_brand_clicks_wow_pct, sa.sa360_tmo_brand_clicks_yoy_pct,
    sa.sa360_tmo_brand_cost, sa.sa360_tmo_brand_cost_wow, sa.sa360_tmo_brand_cost_ly, sa.sa360_tmo_brand_cost_wow_pct, sa.sa360_tmo_brand_cost_yoy_pct,
    sa.sa360_tmo_brand_orders, sa.sa360_tmo_brand_orders_wow, sa.sa360_tmo_brand_orders_ly, sa.sa360_tmo_brand_orders_wow_pct, sa.sa360_tmo_brand_orders_yoy_pct,
    sa.sa360_tmo_brand_cart_start, sa.sa360_tmo_brand_cart_start_wow, sa.sa360_tmo_brand_cart_start_ly, sa.sa360_tmo_brand_cart_start_wow_pct, sa.sa360_tmo_brand_cart_start_yoy_pct,
    sa.sa360_tmo_brand_postpaid_pspv, sa.sa360_tmo_brand_postpaid_pspv_wow, sa.sa360_tmo_brand_postpaid_pspv_ly, sa.sa360_tmo_brand_postpaid_pspv_wow_pct, sa.sa360_tmo_brand_postpaid_pspv_yoy_pct,
    sa.sa360_tmo_nonbrand_impressions, sa.sa360_tmo_nonbrand_impressions_wow, sa.sa360_tmo_nonbrand_impressions_ly, sa.sa360_tmo_nonbrand_impressions_wow_pct, sa.sa360_tmo_nonbrand_impressions_yoy_pct,
    sa.sa360_tmo_nonbrand_clicks, sa.sa360_tmo_nonbrand_clicks_wow, sa.sa360_tmo_nonbrand_clicks_ly, sa.sa360_tmo_nonbrand_clicks_wow_pct, sa.sa360_tmo_nonbrand_clicks_yoy_pct,
    sa.sa360_tmo_nonbrand_cost, sa.sa360_tmo_nonbrand_cost_wow, sa.sa360_tmo_nonbrand_cost_ly, sa.sa360_tmo_nonbrand_cost_wow_pct, sa.sa360_tmo_nonbrand_cost_yoy_pct,
    sa.sa360_tmo_nonbrand_orders, sa.sa360_tmo_nonbrand_orders_wow, sa.sa360_tmo_nonbrand_orders_ly, sa.sa360_tmo_nonbrand_orders_wow_pct, sa.sa360_tmo_nonbrand_orders_yoy_pct,
    sa.sa360_tmo_nonbrand_cart_start, sa.sa360_tmo_nonbrand_cart_start_wow, sa.sa360_tmo_nonbrand_cart_start_ly, sa.sa360_tmo_nonbrand_cart_start_wow_pct, sa.sa360_tmo_nonbrand_cart_start_yoy_pct,
    sa.sa360_tmo_nonbrand_postpaid_pspv, sa.sa360_tmo_nonbrand_postpaid_pspv_wow, sa.sa360_tmo_nonbrand_postpaid_pspv_ly, sa.sa360_tmo_nonbrand_postpaid_pspv_wow_pct, sa.sa360_tmo_nonbrand_postpaid_pspv_yoy_pct,

    -- ================================================================ GSC
    gsc.max_data_date AS gsc_max_data_date,
    gsc.gsc_tmo_brand_impressions, gsc.gsc_tmo_brand_impressions_wow, gsc.gsc_tmo_brand_impressions_ly, gsc.gsc_tmo_brand_impressions_wow_pct, gsc.gsc_tmo_brand_impressions_yoy_pct,
    gsc.gsc_tmo_brand_clicks, gsc.gsc_tmo_brand_clicks_wow, gsc.gsc_tmo_brand_clicks_ly, gsc.gsc_tmo_brand_clicks_wow_pct, gsc.gsc_tmo_brand_clicks_yoy_pct,
    gsc.gsc_tmo_nonbrand_impressions, gsc.gsc_tmo_nonbrand_impressions_wow, gsc.gsc_tmo_nonbrand_impressions_ly, gsc.gsc_tmo_nonbrand_impressions_wow_pct, gsc.gsc_tmo_nonbrand_impressions_yoy_pct,
    gsc.gsc_tmo_nonbrand_clicks, gsc.gsc_tmo_nonbrand_clicks_wow, gsc.gsc_tmo_nonbrand_clicks_ly, gsc.gsc_tmo_nonbrand_clicks_wow_pct, gsc.gsc_tmo_nonbrand_clicks_yoy_pct,

    -- ================================================================ TRENDS
    t.max_data_date AS trends_max_data_date,
    t.trends_byod_index, t.trends_byod_index_wow, t.trends_byod_index_ly, t.trends_byod_index_wow_pct, t.trends_byod_index_yoy_pct,
    t.trends_top_kw_1, t.trends_kw1_interest, t.trends_kw1_change,
    t.trends_top_kw_2, t.trends_kw2_interest, t.trends_kw2_change,
    t.trends_top_kw_3, t.trends_kw3_interest, t.trends_kw3_change,
    t.trends_top_kw_4, t.trends_kw4_interest, t.trends_kw4_change,
    t.trends_top_kw_5, t.trends_kw5_interest, t.trends_kw5_change,

    -- ================================================================ ADOBE — ALL CHANNELS
    ab.max_data_date AS adobe_max_data_date,
    ab.adobe_uvnbByod_allChannels, ab.adobe_uvnbByod_allChannels_wow, ab.adobe_uvnbByod_allChannels_ly, ab.adobe_uvnbByod_allChannels_wow_pct, ab.adobe_uvnbByod_allChannels_yoy_pct,
    ab.adobe_uvnbTotal_allChannels, ab.adobe_uvnbTotal_allChannels_wow, ab.adobe_uvnbTotal_allChannels_ly, ab.adobe_uvnbTotal_allChannels_wow_pct, ab.adobe_uvnbTotal_allChannels_yoy_pct,
    ab.adobe_uvnbFlowTotal_allChannels, ab.adobe_uvnbFlowTotal_allChannels_wow, ab.adobe_uvnbFlowTotal_allChannels_ly, ab.adobe_uvnbFlowTotal_allChannels_wow_pct, ab.adobe_uvnbFlowTotal_allChannels_yoy_pct,
    ab.adobe_pctUvnbByodOfUvnbFlow_allChannels, ab.adobe_pctUvnbByodOfUvnbFlow_allChannels_wow, ab.adobe_pctUvnbByodOfUvnbFlow_allChannels_ly, ab.adobe_pctUvnbByodOfUvnbFlow_allChannels_wow_pct, ab.adobe_pctUvnbByodOfUvnbFlow_allChannels_yoy_pct,
    ab.adobe_cartStartByod_allChannels, ab.adobe_cartStartByod_allChannels_wow, ab.adobe_cartStartByod_allChannels_ly, ab.adobe_cartStartByod_allChannels_wow_pct, ab.adobe_cartStartByod_allChannels_yoy_pct,
    ab.adobe_ordersUnassistedByod_allChannels, ab.adobe_ordersUnassistedByod_allChannels_wow, ab.adobe_ordersUnassistedByod_allChannels_ly, ab.adobe_ordersUnassistedByod_allChannels_wow_pct, ab.adobe_ordersUnassistedByod_allChannels_yoy_pct,
    ab.adobe_ordersAssistedByod_allChannels, ab.adobe_ordersAssistedByod_allChannels_wow, ab.adobe_ordersAssistedByod_allChannels_ly, ab.adobe_ordersAssistedByod_allChannels_wow_pct, ab.adobe_ordersAssistedByod_allChannels_yoy_pct,
    ab.adobe_ordersTotalByod_allChannels, ab.adobe_ordersTotalByod_allChannels_wow, ab.adobe_ordersTotalByod_allChannels_ly, ab.adobe_ordersTotalByod_allChannels_wow_pct, ab.adobe_ordersTotalByod_allChannels_yoy_pct,
    ab.adobe_ordersTotal_allChannels, ab.adobe_ordersTotal_allChannels_wow, ab.adobe_ordersTotal_allChannels_ly, ab.adobe_ordersTotal_allChannels_wow_pct, ab.adobe_ordersTotal_allChannels_yoy_pct,
    ab.adobe_pctOrdersByodOfOrdersTotal_allChannels, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_wow, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_ly, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_wow_pct, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_yoy_pct,
    ab.adobe_cvrByod_allChannels, ab.adobe_cvrByod_allChannels_wow, ab.adobe_cvrByod_allChannels_ly, ab.adobe_cvrByod_allChannels_wow_pct, ab.adobe_cvrByod_allChannels_yoy_pct,
    ab.adobe_cvrSite_allChannels, ab.adobe_cvrSite_allChannels_wow, ab.adobe_cvrSite_allChannels_ly, ab.adobe_cvrSite_allChannels_wow_pct, ab.adobe_cvrSite_allChannels_yoy_pct,
    -- NEW: Postpaid and HSI CVR (allChannels only)
    ab.adobe_cvrPostpaid_allChannels, ab.adobe_cvrPostpaid_allChannels_wow, ab.adobe_cvrPostpaid_allChannels_ly, ab.adobe_cvrPostpaid_allChannels_wow_pct, ab.adobe_cvrPostpaid_allChannels_yoy_pct,
    ab.adobe_cvrHsi_allChannels, ab.adobe_cvrHsi_allChannels_wow, ab.adobe_cvrHsi_allChannels_ly, ab.adobe_cvrHsi_allChannels_wow_pct, ab.adobe_cvrHsi_allChannels_yoy_pct,
    -- PAID SEARCH
    ab.adobe_uvnbByod_paidSearch, ab.adobe_uvnbByod_paidSearch_wow, ab.adobe_uvnbByod_paidSearch_ly, ab.adobe_uvnbByod_paidSearch_wow_pct, ab.adobe_uvnbByod_paidSearch_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_paidSearch, ab.adobe_pctUvnbByodOfTotal_paidSearch_wow, ab.adobe_pctUvnbByodOfTotal_paidSearch_ly, ab.adobe_pctUvnbByodOfTotal_paidSearch_wow_pct, ab.adobe_pctUvnbByodOfTotal_paidSearch_yoy_pct,
    ab.adobe_cartStartByod_paidSearch, ab.adobe_cartStartByod_paidSearch_wow, ab.adobe_cartStartByod_paidSearch_ly, ab.adobe_cartStartByod_paidSearch_wow_pct, ab.adobe_cartStartByod_paidSearch_yoy_pct,
    ab.adobe_ordersUnassistedByod_paidSearch, ab.adobe_ordersUnassistedByod_paidSearch_wow, ab.adobe_ordersUnassistedByod_paidSearch_ly, ab.adobe_ordersUnassistedByod_paidSearch_wow_pct, ab.adobe_ordersUnassistedByod_paidSearch_yoy_pct,
    ab.adobe_ordersAssistedByod_paidSearch, ab.adobe_ordersAssistedByod_paidSearch_wow, ab.adobe_ordersAssistedByod_paidSearch_ly, ab.adobe_ordersAssistedByod_paidSearch_wow_pct, ab.adobe_ordersAssistedByod_paidSearch_yoy_pct,
    ab.adobe_ordersTotalByod_paidSearch, ab.adobe_ordersTotalByod_paidSearch_wow, ab.adobe_ordersTotalByod_paidSearch_ly, ab.adobe_ordersTotalByod_paidSearch_wow_pct, ab.adobe_ordersTotalByod_paidSearch_yoy_pct,
    -- ORGANIC SEARCH
    ab.adobe_uvnbByod_organicSearch, ab.adobe_uvnbByod_organicSearch_wow, ab.adobe_uvnbByod_organicSearch_ly, ab.adobe_uvnbByod_organicSearch_wow_pct, ab.adobe_uvnbByod_organicSearch_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_organicSearch, ab.adobe_pctUvnbByodOfTotal_organicSearch_wow, ab.adobe_pctUvnbByodOfTotal_organicSearch_ly, ab.adobe_pctUvnbByodOfTotal_organicSearch_wow_pct, ab.adobe_pctUvnbByodOfTotal_organicSearch_yoy_pct,
    ab.adobe_cartStartByod_organicSearch, ab.adobe_cartStartByod_organicSearch_wow, ab.adobe_cartStartByod_organicSearch_ly, ab.adobe_cartStartByod_organicSearch_wow_pct, ab.adobe_cartStartByod_organicSearch_yoy_pct,
    ab.adobe_ordersUnassistedByod_organicSearch, ab.adobe_ordersUnassistedByod_organicSearch_wow, ab.adobe_ordersUnassistedByod_organicSearch_ly, ab.adobe_ordersUnassistedByod_organicSearch_wow_pct, ab.adobe_ordersUnassistedByod_organicSearch_yoy_pct,
    ab.adobe_ordersAssistedByod_organicSearch, ab.adobe_ordersAssistedByod_organicSearch_wow, ab.adobe_ordersAssistedByod_organicSearch_ly, ab.adobe_ordersAssistedByod_organicSearch_wow_pct, ab.adobe_ordersAssistedByod_organicSearch_yoy_pct,
    ab.adobe_ordersTotalByod_organicSearch, ab.adobe_ordersTotalByod_organicSearch_wow, ab.adobe_ordersTotalByod_organicSearch_ly, ab.adobe_ordersTotalByod_organicSearch_wow_pct, ab.adobe_ordersTotalByod_organicSearch_yoy_pct,
    -- DIRECT
    ab.adobe_uvnbByod_direct, ab.adobe_uvnbByod_direct_wow, ab.adobe_uvnbByod_direct_ly, ab.adobe_uvnbByod_direct_wow_pct, ab.adobe_uvnbByod_direct_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_direct, ab.adobe_pctUvnbByodOfTotal_direct_wow, ab.adobe_pctUvnbByodOfTotal_direct_ly, ab.adobe_pctUvnbByodOfTotal_direct_wow_pct, ab.adobe_pctUvnbByodOfTotal_direct_yoy_pct,
    ab.adobe_cartStartByod_direct, ab.adobe_cartStartByod_direct_wow, ab.adobe_cartStartByod_direct_ly, ab.adobe_cartStartByod_direct_wow_pct, ab.adobe_cartStartByod_direct_yoy_pct,
    ab.adobe_ordersUnassistedByod_direct, ab.adobe_ordersUnassistedByod_direct_wow, ab.adobe_ordersUnassistedByod_direct_ly, ab.adobe_ordersUnassistedByod_direct_wow_pct, ab.adobe_ordersUnassistedByod_direct_yoy_pct,
    ab.adobe_ordersAssistedByod_direct, ab.adobe_ordersAssistedByod_direct_wow, ab.adobe_ordersAssistedByod_direct_ly, ab.adobe_ordersAssistedByod_direct_wow_pct, ab.adobe_ordersAssistedByod_direct_yoy_pct,
    ab.adobe_ordersTotalByod_direct, ab.adobe_ordersTotalByod_direct_wow, ab.adobe_ordersTotalByod_direct_ly, ab.adobe_ordersTotalByod_direct_wow_pct, ab.adobe_ordersTotalByod_direct_yoy_pct,
    -- SOCIAL
    ab.adobe_uvnbByod_social, ab.adobe_uvnbByod_social_wow, ab.adobe_uvnbByod_social_ly, ab.adobe_uvnbByod_social_wow_pct, ab.adobe_uvnbByod_social_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_social, ab.adobe_pctUvnbByodOfTotal_social_wow, ab.adobe_pctUvnbByodOfTotal_social_ly, ab.adobe_pctUvnbByodOfTotal_social_wow_pct, ab.adobe_pctUvnbByodOfTotal_social_yoy_pct,
    ab.adobe_cartStartByod_social, ab.adobe_cartStartByod_social_wow, ab.adobe_cartStartByod_social_ly, ab.adobe_cartStartByod_social_wow_pct, ab.adobe_cartStartByod_social_yoy_pct,
    ab.adobe_ordersUnassistedByod_social, ab.adobe_ordersUnassistedByod_social_wow, ab.adobe_ordersUnassistedByod_social_ly, ab.adobe_ordersUnassistedByod_social_wow_pct, ab.adobe_ordersUnassistedByod_social_yoy_pct,
    ab.adobe_ordersAssistedByod_social, ab.adobe_ordersAssistedByod_social_wow, ab.adobe_ordersAssistedByod_social_ly, ab.adobe_ordersAssistedByod_social_wow_pct, ab.adobe_ordersAssistedByod_social_yoy_pct,
    ab.adobe_ordersTotalByod_social, ab.adobe_ordersTotalByod_social_wow, ab.adobe_ordersTotalByod_social_ly, ab.adobe_ordersTotalByod_social_wow_pct, ab.adobe_ordersTotalByod_social_yoy_pct,
    -- PROGRAMMATIC
    ab.adobe_uvnbByod_programmatic, ab.adobe_uvnbByod_programmatic_wow, ab.adobe_uvnbByod_programmatic_ly, ab.adobe_uvnbByod_programmatic_wow_pct, ab.adobe_uvnbByod_programmatic_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_programmatic, ab.adobe_pctUvnbByodOfTotal_programmatic_wow, ab.adobe_pctUvnbByodOfTotal_programmatic_ly, ab.adobe_pctUvnbByodOfTotal_programmatic_wow_pct, ab.adobe_pctUvnbByodOfTotal_programmatic_yoy_pct,
    ab.adobe_cartStartByod_programmatic, ab.adobe_cartStartByod_programmatic_wow, ab.adobe_cartStartByod_programmatic_ly, ab.adobe_cartStartByod_programmatic_wow_pct, ab.adobe_cartStartByod_programmatic_yoy_pct,
    ab.adobe_ordersUnassistedByod_programmatic, ab.adobe_ordersUnassistedByod_programmatic_wow, ab.adobe_ordersUnassistedByod_programmatic_ly, ab.adobe_ordersUnassistedByod_programmatic_wow_pct, ab.adobe_ordersUnassistedByod_programmatic_yoy_pct,
    ab.adobe_ordersAssistedByod_programmatic, ab.adobe_ordersAssistedByod_programmatic_wow, ab.adobe_ordersAssistedByod_programmatic_ly, ab.adobe_ordersAssistedByod_programmatic_wow_pct, ab.adobe_ordersAssistedByod_programmatic_yoy_pct,
    ab.adobe_ordersTotalByod_programmatic, ab.adobe_ordersTotalByod_programmatic_wow, ab.adobe_ordersTotalByod_programmatic_ly, ab.adobe_ordersTotalByod_programmatic_wow_pct, ab.adobe_ordersTotalByod_programmatic_yoy_pct,
    -- OTHER
    ab.adobe_uvnbByod_other, ab.adobe_uvnbByod_other_wow, ab.adobe_uvnbByod_other_ly, ab.adobe_uvnbByod_other_wow_pct, ab.adobe_uvnbByod_other_yoy_pct,
    ab.adobe_pctUvnbByodOfTotal_other, ab.adobe_pctUvnbByodOfTotal_other_wow, ab.adobe_pctUvnbByodOfTotal_other_ly, ab.adobe_pctUvnbByodOfTotal_other_wow_pct, ab.adobe_pctUvnbByodOfTotal_other_yoy_pct,
    ab.adobe_cartStartByod_other, ab.adobe_cartStartByod_other_wow, ab.adobe_cartStartByod_other_ly, ab.adobe_cartStartByod_other_wow_pct, ab.adobe_cartStartByod_other_yoy_pct,
    ab.adobe_ordersUnassistedByod_other, ab.adobe_ordersUnassistedByod_other_wow, ab.adobe_ordersUnassistedByod_other_ly, ab.adobe_ordersUnassistedByod_other_wow_pct, ab.adobe_ordersUnassistedByod_other_yoy_pct,
    ab.adobe_ordersAssistedByod_other, ab.adobe_ordersAssistedByod_other_wow, ab.adobe_ordersAssistedByod_other_ly, ab.adobe_ordersAssistedByod_other_wow_pct, ab.adobe_ordersAssistedByod_other_yoy_pct,
    ab.adobe_ordersTotalByod_other, ab.adobe_ordersTotalByod_other_wow, ab.adobe_ordersTotalByod_other_ly, ab.adobe_ordersTotalByod_other_wow_pct, ab.adobe_ordersTotalByod_other_yoy_pct,

    -- ================================================================ ADOBE ENTRY PAGES — ALL CHANNELS
    aep.max_data_date AS adobe_entry_pages_max_data_date,
    aep.adobe_byodUvnbVisitors_allChannels, aep.adobe_byodUvnbVisitors_allChannels_wow, aep.adobe_byodUvnbVisitors_allChannels_ly, aep.adobe_byodUvnbVisitors_allChannels_wow_pct, aep.adobe_byodUvnbVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_allChannels, aep.adobe_byodEntryByodPageVisitors_allChannels_wow, aep.adobe_byodEntryByodPageVisitors_allChannels_ly, aep.adobe_byodEntryByodPageVisitors_allChannels_wow_pct, aep.adobe_byodEntryByodPageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_allChannels, aep.adobe_byodEntryHomePageVisitors_allChannels_wow, aep.adobe_byodEntryHomePageVisitors_allChannels_ly, aep.adobe_byodEntryHomePageVisitors_allChannels_wow_pct, aep.adobe_byodEntryHomePageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_allChannels, aep.adobe_byodEntryDevicePageVisitors_allChannels_wow, aep.adobe_byodEntryDevicePageVisitors_allChannels_ly, aep.adobe_byodEntryDevicePageVisitors_allChannels_wow_pct, aep.adobe_byodEntryDevicePageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_allChannels, aep.adobe_byodEntryPlansPageVisitors_allChannels_wow, aep.adobe_byodEntryPlansPageVisitors_allChannels_ly, aep.adobe_byodEntryPlansPageVisitors_allChannels_wow_pct, aep.adobe_byodEntryPlansPageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_allChannels, aep.adobe_byodEntryOtherPageVisitors_allChannels_wow, aep.adobe_byodEntryOtherPageVisitors_allChannels_ly, aep.adobe_byodEntryOtherPageVisitors_allChannels_wow_pct, aep.adobe_byodEntryOtherPageVisitors_allChannels_yoy_pct,
    aep.adobe_byodUvnbVisitors_paidSearch, aep.adobe_byodUvnbVisitors_paidSearch_wow, aep.adobe_byodUvnbVisitors_paidSearch_ly, aep.adobe_byodUvnbVisitors_paidSearch_wow_pct, aep.adobe_byodUvnbVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_paidSearch, aep.adobe_byodEntryByodPageVisitors_paidSearch_wow, aep.adobe_byodEntryByodPageVisitors_paidSearch_ly, aep.adobe_byodEntryByodPageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryByodPageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_paidSearch, aep.adobe_byodEntryHomePageVisitors_paidSearch_wow, aep.adobe_byodEntryHomePageVisitors_paidSearch_ly, aep.adobe_byodEntryHomePageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryHomePageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_paidSearch, aep.adobe_byodEntryDevicePageVisitors_paidSearch_wow, aep.adobe_byodEntryDevicePageVisitors_paidSearch_ly, aep.adobe_byodEntryDevicePageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryDevicePageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_paidSearch, aep.adobe_byodEntryPlansPageVisitors_paidSearch_wow, aep.adobe_byodEntryPlansPageVisitors_paidSearch_ly, aep.adobe_byodEntryPlansPageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryPlansPageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_paidSearch, aep.adobe_byodEntryOtherPageVisitors_paidSearch_wow, aep.adobe_byodEntryOtherPageVisitors_paidSearch_ly, aep.adobe_byodEntryOtherPageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryOtherPageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodUvnbVisitors_organicSearch, aep.adobe_byodUvnbVisitors_organicSearch_wow, aep.adobe_byodUvnbVisitors_organicSearch_ly, aep.adobe_byodUvnbVisitors_organicSearch_wow_pct, aep.adobe_byodUvnbVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_organicSearch, aep.adobe_byodEntryByodPageVisitors_organicSearch_wow, aep.adobe_byodEntryByodPageVisitors_organicSearch_ly, aep.adobe_byodEntryByodPageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryByodPageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_organicSearch, aep.adobe_byodEntryHomePageVisitors_organicSearch_wow, aep.adobe_byodEntryHomePageVisitors_organicSearch_ly, aep.adobe_byodEntryHomePageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryHomePageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_organicSearch, aep.adobe_byodEntryDevicePageVisitors_organicSearch_wow, aep.adobe_byodEntryDevicePageVisitors_organicSearch_ly, aep.adobe_byodEntryDevicePageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryDevicePageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_organicSearch, aep.adobe_byodEntryPlansPageVisitors_organicSearch_wow, aep.adobe_byodEntryPlansPageVisitors_organicSearch_ly, aep.adobe_byodEntryPlansPageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryPlansPageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_organicSearch, aep.adobe_byodEntryOtherPageVisitors_organicSearch_wow, aep.adobe_byodEntryOtherPageVisitors_organicSearch_ly, aep.adobe_byodEntryOtherPageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryOtherPageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodUvnbVisitors_direct, aep.adobe_byodUvnbVisitors_direct_wow, aep.adobe_byodUvnbVisitors_direct_ly, aep.adobe_byodUvnbVisitors_direct_wow_pct, aep.adobe_byodUvnbVisitors_direct_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_direct, aep.adobe_byodEntryByodPageVisitors_direct_wow, aep.adobe_byodEntryByodPageVisitors_direct_ly, aep.adobe_byodEntryByodPageVisitors_direct_wow_pct, aep.adobe_byodEntryByodPageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_direct, aep.adobe_byodEntryHomePageVisitors_direct_wow, aep.adobe_byodEntryHomePageVisitors_direct_ly, aep.adobe_byodEntryHomePageVisitors_direct_wow_pct, aep.adobe_byodEntryHomePageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_direct, aep.adobe_byodEntryDevicePageVisitors_direct_wow, aep.adobe_byodEntryDevicePageVisitors_direct_ly, aep.adobe_byodEntryDevicePageVisitors_direct_wow_pct, aep.adobe_byodEntryDevicePageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_direct, aep.adobe_byodEntryPlansPageVisitors_direct_wow, aep.adobe_byodEntryPlansPageVisitors_direct_ly, aep.adobe_byodEntryPlansPageVisitors_direct_wow_pct, aep.adobe_byodEntryPlansPageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_direct, aep.adobe_byodEntryOtherPageVisitors_direct_wow, aep.adobe_byodEntryOtherPageVisitors_direct_ly, aep.adobe_byodEntryOtherPageVisitors_direct_wow_pct, aep.adobe_byodEntryOtherPageVisitors_direct_yoy_pct,
    aep.adobe_byodUvnbVisitors_social, aep.adobe_byodUvnbVisitors_social_wow, aep.adobe_byodUvnbVisitors_social_ly, aep.adobe_byodUvnbVisitors_social_wow_pct, aep.adobe_byodUvnbVisitors_social_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_social, aep.adobe_byodEntryByodPageVisitors_social_wow, aep.adobe_byodEntryByodPageVisitors_social_ly, aep.adobe_byodEntryByodPageVisitors_social_wow_pct, aep.adobe_byodEntryByodPageVisitors_social_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_social, aep.adobe_byodEntryHomePageVisitors_social_wow, aep.adobe_byodEntryHomePageVisitors_social_ly, aep.adobe_byodEntryHomePageVisitors_social_wow_pct, aep.adobe_byodEntryHomePageVisitors_social_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_social, aep.adobe_byodEntryDevicePageVisitors_social_wow, aep.adobe_byodEntryDevicePageVisitors_social_ly, aep.adobe_byodEntryDevicePageVisitors_social_wow_pct, aep.adobe_byodEntryDevicePageVisitors_social_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_social, aep.adobe_byodEntryPlansPageVisitors_social_wow, aep.adobe_byodEntryPlansPageVisitors_social_ly, aep.adobe_byodEntryPlansPageVisitors_social_wow_pct, aep.adobe_byodEntryPlansPageVisitors_social_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_social, aep.adobe_byodEntryOtherPageVisitors_social_wow, aep.adobe_byodEntryOtherPageVisitors_social_ly, aep.adobe_byodEntryOtherPageVisitors_social_wow_pct, aep.adobe_byodEntryOtherPageVisitors_social_yoy_pct,
    aep.adobe_byodUvnbVisitors_programmatic, aep.adobe_byodUvnbVisitors_programmatic_wow, aep.adobe_byodUvnbVisitors_programmatic_ly, aep.adobe_byodUvnbVisitors_programmatic_wow_pct, aep.adobe_byodUvnbVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_programmatic, aep.adobe_byodEntryByodPageVisitors_programmatic_wow, aep.adobe_byodEntryByodPageVisitors_programmatic_ly, aep.adobe_byodEntryByodPageVisitors_programmatic_wow_pct, aep.adobe_byodEntryByodPageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_programmatic, aep.adobe_byodEntryHomePageVisitors_programmatic_wow, aep.adobe_byodEntryHomePageVisitors_programmatic_ly, aep.adobe_byodEntryHomePageVisitors_programmatic_wow_pct, aep.adobe_byodEntryHomePageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_programmatic, aep.adobe_byodEntryDevicePageVisitors_programmatic_wow, aep.adobe_byodEntryDevicePageVisitors_programmatic_ly, aep.adobe_byodEntryDevicePageVisitors_programmatic_wow_pct, aep.adobe_byodEntryDevicePageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_programmatic, aep.adobe_byodEntryPlansPageVisitors_programmatic_wow, aep.adobe_byodEntryPlansPageVisitors_programmatic_ly, aep.adobe_byodEntryPlansPageVisitors_programmatic_wow_pct, aep.adobe_byodEntryPlansPageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_programmatic, aep.adobe_byodEntryOtherPageVisitors_programmatic_wow, aep.adobe_byodEntryOtherPageVisitors_programmatic_ly, aep.adobe_byodEntryOtherPageVisitors_programmatic_wow_pct, aep.adobe_byodEntryOtherPageVisitors_programmatic_yoy_pct,
    aep.adobe_byodUvnbVisitors_other, aep.adobe_byodUvnbVisitors_other_wow, aep.adobe_byodUvnbVisitors_other_ly, aep.adobe_byodUvnbVisitors_other_wow_pct, aep.adobe_byodUvnbVisitors_other_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_other, aep.adobe_byodEntryByodPageVisitors_other_wow, aep.adobe_byodEntryByodPageVisitors_other_ly, aep.adobe_byodEntryByodPageVisitors_other_wow_pct, aep.adobe_byodEntryByodPageVisitors_other_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_other, aep.adobe_byodEntryHomePageVisitors_other_wow, aep.adobe_byodEntryHomePageVisitors_other_ly, aep.adobe_byodEntryHomePageVisitors_other_wow_pct, aep.adobe_byodEntryHomePageVisitors_other_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_other, aep.adobe_byodEntryDevicePageVisitors_other_wow, aep.adobe_byodEntryDevicePageVisitors_other_ly, aep.adobe_byodEntryDevicePageVisitors_other_wow_pct, aep.adobe_byodEntryDevicePageVisitors_other_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_other, aep.adobe_byodEntryPlansPageVisitors_other_wow, aep.adobe_byodEntryPlansPageVisitors_other_ly, aep.adobe_byodEntryPlansPageVisitors_other_wow_pct, aep.adobe_byodEntryPlansPageVisitors_other_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_other, aep.adobe_byodEntryOtherPageVisitors_other_wow, aep.adobe_byodEntryOtherPageVisitors_other_ly, aep.adobe_byodEntryOtherPageVisitors_other_wow_pct, aep.adobe_byodEntryOtherPageVisitors_other_yoy_pct,

    -- ================================================================ ADOBE OUTCOMES — ALL CHANNELS
    ao.max_data_date AS adobe_outcomes_max_data_date,
    ao.adobe_byodVrChatVisitors_allChannels, ao.adobe_byodVrChatVisitors_allChannels_wow, ao.adobe_byodVrChatVisitors_allChannels_ly, ao.adobe_byodVrChatVisitors_allChannels_wow_pct, ao.adobe_byodVrChatVisitors_allChannels_yoy_pct,
    ao.adobe_byodCallVisitors_allChannels, ao.adobe_byodCallVisitors_allChannels_wow, ao.adobe_byodCallVisitors_allChannels_ly, ao.adobe_byodCallVisitors_allChannels_wow_pct, ao.adobe_byodCallVisitors_allChannels_yoy_pct,
    ao.adobe_byodStoreLocatorVisitors_allChannels, ao.adobe_byodStoreLocatorVisitors_allChannels_wow, ao.adobe_byodStoreLocatorVisitors_allChannels_ly, ao.adobe_byodStoreLocatorVisitors_allChannels_wow_pct, ao.adobe_byodStoreLocatorVisitors_allChannels_yoy_pct,
    ao.adobe_byodInternalTmoVisitors_allChannels, ao.adobe_byodInternalTmoVisitors_allChannels_wow, ao.adobe_byodInternalTmoVisitors_allChannels_ly, ao.adobe_byodInternalTmoVisitors_allChannels_wow_pct, ao.adobe_byodInternalTmoVisitors_allChannels_yoy_pct,
    ao.adobe_byodBouncersVisitors_allChannels, ao.adobe_byodBouncersVisitors_allChannels_wow, ao.adobe_byodBouncersVisitors_allChannels_ly, ao.adobe_byodBouncersVisitors_allChannels_wow_pct, ao.adobe_byodBouncersVisitors_allChannels_yoy_pct,
    ao.adobe_byodOrders_allChannels, ao.adobe_byodOrders_allChannels_wow, ao.adobe_byodOrders_allChannels_ly, ao.adobe_byodOrders_allChannels_wow_pct, ao.adobe_byodOrders_allChannels_yoy_pct,
    ao.adobe_byodVrChatVisitors_paidSearch, ao.adobe_byodVrChatVisitors_paidSearch_wow, ao.adobe_byodVrChatVisitors_paidSearch_ly, ao.adobe_byodVrChatVisitors_paidSearch_wow_pct, ao.adobe_byodVrChatVisitors_paidSearch_yoy_pct,
    ao.adobe_byodCallVisitors_paidSearch, ao.adobe_byodCallVisitors_paidSearch_wow, ao.adobe_byodCallVisitors_paidSearch_ly, ao.adobe_byodCallVisitors_paidSearch_wow_pct, ao.adobe_byodCallVisitors_paidSearch_yoy_pct,
    ao.adobe_byodStoreLocatorVisitors_paidSearch, ao.adobe_byodStoreLocatorVisitors_paidSearch_wow, ao.adobe_byodStoreLocatorVisitors_paidSearch_ly, ao.adobe_byodStoreLocatorVisitors_paidSearch_wow_pct, ao.adobe_byodStoreLocatorVisitors_paidSearch_yoy_pct,
    ao.adobe_byodInternalTmoVisitors_paidSearch, ao.adobe_byodInternalTmoVisitors_paidSearch_wow, ao.adobe_byodInternalTmoVisitors_paidSearch_ly, ao.adobe_byodInternalTmoVisitors_paidSearch_wow_pct, ao.adobe_byodInternalTmoVisitors_paidSearch_yoy_pct,
    ao.adobe_byodBouncersVisitors_paidSearch, ao.adobe_byodBouncersVisitors_paidSearch_wow, ao.adobe_byodBouncersVisitors_paidSearch_ly, ao.adobe_byodBouncersVisitors_paidSearch_wow_pct, ao.adobe_byodBouncersVisitors_paidSearch_yoy_pct,
    ao.adobe_byodOrders_paidSearch, ao.adobe_byodOrders_paidSearch_wow, ao.adobe_byodOrders_paidSearch_ly, ao.adobe_byodOrders_paidSearch_wow_pct, ao.adobe_byodOrders_paidSearch_yoy_pct,
    ao.adobe_byodVrChatVisitors_organicSearch, ao.adobe_byodVrChatVisitors_organicSearch_wow, ao.adobe_byodVrChatVisitors_organicSearch_ly, ao.adobe_byodVrChatVisitors_organicSearch_wow_pct, ao.adobe_byodVrChatVisitors_organicSearch_yoy_pct,
    ao.adobe_byodCallVisitors_organicSearch, ao.adobe_byodCallVisitors_organicSearch_wow, ao.adobe_byodCallVisitors_organicSearch_ly, ao.adobe_byodCallVisitors_organicSearch_wow_pct, ao.adobe_byodCallVisitors_organicSearch_yoy_pct,
    ao.adobe_byodStoreLocatorVisitors_organicSearch, ao.adobe_byodStoreLocatorVisitors_organicSearch_wow, ao.adobe_byodStoreLocatorVisitors_organicSearch_ly, ao.adobe_byodStoreLocatorVisitors_organicSearch_wow_pct, ao.adobe_byodStoreLocatorVisitors_organicSearch_yoy_pct,
    ao.adobe_byodInternalTmoVisitors_organicSearch, ao.adobe_byodInternalTmoVisitors_organicSearch_wow, ao.adobe_byodInternalTmoVisitors_organicSearch_ly, ao.adobe_byodInternalTmoVisitors_organicSearch_wow_pct, ao.adobe_byodInternalTmoVisitors_organicSearch_yoy_pct,
    ao.adobe_byodBouncersVisitors_organicSearch, ao.adobe_byodBouncersVisitors_organicSearch_wow, ao.adobe_byodBouncersVisitors_organicSearch_ly, ao.adobe_byodBouncersVisitors_organicSearch_wow_pct, ao.adobe_byodBouncersVisitors_organicSearch_yoy_pct,
    ao.adobe_byodOrders_organicSearch, ao.adobe_byodOrders_organicSearch_wow, ao.adobe_byodOrders_organicSearch_ly, ao.adobe_byodOrders_organicSearch_wow_pct, ao.adobe_byodOrders_organicSearch_yoy_pct,
    ao.adobe_byodVrChatVisitors_direct, ao.adobe_byodVrChatVisitors_direct_wow, ao.adobe_byodVrChatVisitors_direct_ly, ao.adobe_byodVrChatVisitors_direct_wow_pct, ao.adobe_byodVrChatVisitors_direct_yoy_pct,
    ao.adobe_byodCallVisitors_direct, ao.adobe_byodCallVisitors_direct_wow, ao.adobe_byodCallVisitors_direct_ly, ao.adobe_byodCallVisitors_direct_wow_pct, ao.adobe_byodCallVisitors_direct_yoy_pct,
    ao.adobe_byodStoreLocatorVisitors_direct, ao.adobe_byodStoreLocatorVisitors_direct_wow, ao.adobe_byodStoreLocatorVisitors_direct_ly, ao.adobe_byodStoreLocatorVisitors_direct_wow_pct, ao.adobe_byodStoreLocatorVisitors_direct_yoy_pct,
    ao.adobe_byodInternalTmoVisitors_direct, ao.adobe_byodInternalTmoVisitors_direct_wow, ao.adobe_byodInternalTmoVisitors_direct_ly, ao.adobe_byodInternalTmoVisitors_direct_wow_pct, ao.adobe_byodInternalTmoVisitors_direct_yoy_pct,
    ao.adobe_byodBouncersVisitors_direct, ao.adobe_byodBouncersVisitors_direct_wow, ao.adobe_byodBouncersVisitors_direct_ly, ao.adobe_byodBouncersVisitors_direct_wow_pct, ao.adobe_byodBouncersVisitors_direct_yoy_pct,
    ao.adobe_byodOrders_direct, ao.adobe_byodOrders_direct_wow, ao.adobe_byodOrders_direct_ly, ao.adobe_byodOrders_direct_wow_pct, ao.adobe_byodOrders_direct_yoy_pct,
    ao.adobe_byodVrChatVisitors_social, ao.adobe_byodVrChatVisitors_social_wow, ao.adobe_byodVrChatVisitors_social_ly, ao.adobe_byodVrChatVisitors_social_wow_pct, ao.adobe_byodVrChatVisitors_social_yoy_pct,
    ao.adobe_byodCallVisitors_social, ao.adobe_byodCallVisitors_social_wow, ao.adobe_byodCallVisitors_social_ly, ao.adobe_byodCallVisitors_social_wow_pct, ao.adobe_byodCallVisitors_social_yoy_pct,
    ao.adobe_byodStoreLocatorVisitors_social, ao.adobe_byodStoreLocatorVisitors_social_wow, ao.adobe_byodStoreLocatorVisitors_social_ly, ao.adobe_byodStoreLocatorVisitors_social_wow_pct, ao.adobe_byodStoreLocatorVisitors_social_yoy_pct,
    ao.adobe_byodInternalTmoVisitors_social, ao.adobe_byodInternalTmoVisitors_social_wow, ao.adobe_byodInternalTmoVisitors_social_ly, ao.adobe_byodInternalTmoVisitors_social_wow_pct, ao.adobe_byodInternalTmoVisitors_social_yoy_pct,
    ao.adobe_byodBouncersVisitors_social, ao.adobe_byodBouncersVisitors_social_wow, ao.adobe_byodBouncersVisitors_social_ly, ao.adobe_byodBouncersVisitors_social_wow_pct, ao.adobe_byodBouncersVisitors_social_yoy_pct,
    ao.adobe_byodOrders_social, ao.adobe_byodOrders_social_wow, ao.adobe_byodOrders_social_ly, ao.adobe_byodOrders_social_wow_pct, ao.adobe_byodOrders_social_yoy_pct,
    ao.adobe_byodVrChatVisitors_programmatic, ao.adobe_byodVrChatVisitors_programmatic_wow, ao.adobe_byodVrChatVisitors_programmatic_ly, ao.adobe_byodVrChatVisitors_programmatic_wow_pct, ao.adobe_byodVrChatVisitors_programmatic_yoy_pct,
    ao.adobe_byodCallVisitors_programmatic, ao.adobe_byodCallVisitors_programmatic_wow, ao.adobe_byodCallVisitors_programmatic_ly, ao.adobe_byodCallVisitors_programmatic_wow_pct, ao.adobe_byodCallVisitors_programmatic_yoy_pct,
    ao.adobe_byodStoreLocatorVisitors_programmatic, ao.adobe_byodStoreLocatorVisitors_programmatic_wow, ao.adobe_byodStoreLocatorVisitors_programmatic_ly, ao.adobe_byodStoreLocatorVisitors_programmatic_wow_pct, ao.adobe_byodStoreLocatorVisitors_programmatic_yoy_pct,
    ao.adobe_byodInternalTmoVisitors_programmatic, ao.adobe_byodInternalTmoVisitors_programmatic_wow, ao.adobe_byodInternalTmoVisitors_programmatic_ly, ao.adobe_byodInternalTmoVisitors_programmatic_wow_pct, ao.adobe_byodInternalTmoVisitors_programmatic_yoy_pct,
    ao.adobe_byodBouncersVisitors_programmatic, ao.adobe_byodBouncersVisitors_programmatic_wow, ao.adobe_byodBouncersVisitors_programmatic_ly, ao.adobe_byodBouncersVisitors_programmatic_wow_pct, ao.adobe_byodBouncersVisitors_programmatic_yoy_pct,
    ao.adobe_byodOrders_programmatic, ao.adobe_byodOrders_programmatic_wow, ao.adobe_byodOrders_programmatic_ly, ao.adobe_byodOrders_programmatic_wow_pct, ao.adobe_byodOrders_programmatic_yoy_pct,
    ao.adobe_byodVrChatVisitors_other, ao.adobe_byodVrChatVisitors_other_wow, ao.adobe_byodVrChatVisitors_other_ly, ao.adobe_byodVrChatVisitors_other_wow_pct, ao.adobe_byodVrChatVisitors_other_yoy_pct,
    ao.adobe_byodCallVisitors_other, ao.adobe_byodCallVisitors_other_wow, ao.adobe_byodCallVisitors_other_ly, ao.adobe_byodCallVisitors_other_wow_pct, ao.adobe_byodCallVisitors_other_yoy_pct,
    ao.adobe_byodStoreLocatorVisitors_other, ao.adobe_byodStoreLocatorVisitors_other_wow, ao.adobe_byodStoreLocatorVisitors_other_ly, ao.adobe_byodStoreLocatorVisitors_other_wow_pct, ao.adobe_byodStoreLocatorVisitors_other_yoy_pct,
    ao.adobe_byodInternalTmoVisitors_other, ao.adobe_byodInternalTmoVisitors_other_wow, ao.adobe_byodInternalTmoVisitors_other_ly, ao.adobe_byodInternalTmoVisitors_other_wow_pct, ao.adobe_byodInternalTmoVisitors_other_yoy_pct,
    ao.adobe_byodBouncersVisitors_other, ao.adobe_byodBouncersVisitors_other_wow, ao.adobe_byodBouncersVisitors_other_ly, ao.adobe_byodBouncersVisitors_other_wow_pct, ao.adobe_byodBouncersVisitors_other_yoy_pct,
    ao.adobe_byodOrders_other, ao.adobe_byodOrders_other_wow, ao.adobe_byodOrders_other_ly, ao.adobe_byodOrders_other_wow_pct, ao.adobe_byodOrders_other_yoy_pct

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_sa360_weekly`                    sa
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_gsc_weekly`                   gsc ON sa.week_sun_to_sat = gsc.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profound_weekly`              p   ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat) = p.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_profoundGofish_weekly`         g   ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat) = g.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_googleTrends_weekly`           t   ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat) = t.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobe_weekly`                 ab  ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat, t.week_sun_to_sat) = ab.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobeByodEntryPages_weekly`   aep ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat, t.week_sun_to_sat, ab.week_sun_to_sat) = aep.week_sun_to_sat
FULL OUTER JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_silver_adobeByodOutcomes_weekly`     ao  ON COALESCE(sa.week_sun_to_sat, gsc.week_sun_to_sat, p.week_sun_to_sat, g.week_sun_to_sat, t.week_sun_to_sat, ab.week_sun_to_sat, aep.week_sun_to_sat) = ao.week_sun_to_sat

ORDER BY week_sun_to_sat ASC
; 