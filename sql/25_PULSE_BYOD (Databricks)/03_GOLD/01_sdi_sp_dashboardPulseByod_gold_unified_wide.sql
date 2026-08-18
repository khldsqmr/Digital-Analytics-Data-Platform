/* =================================================================================================
FILE:         sdi_sp_dashboardPulseByod_gold_unified_wide.sql
LAYER:        Gold Table — Wide (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
TABLE:        sdi_tbl_dashboardPulseByod_gold_unified_wide
PROCEDURE:    sdi_sp_dashboardPulseByod_gold_unified_wide

PURPOSE:
  Gold Wide table. One row per week, all metrics from all sources as columns.
  Built directly from the Silver tables (joins), not from Bronze.
  Used for ad-hoc analysis and Excel exports.
  Gold Long (used by dashboard) reads Silver directly.

SOURCES (active):
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly              (p)
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly       (g)
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly                 (ab)
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly  (aep)
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly    (ao)

SOURCES (commented out — pending sa360/gsc/googleTrends Silver, not yet provided):
  sdi_tbl_dashboardPulseByod_silver_sa360_weekly       (sa)
  sdi_tbl_dashboardPulseByod_silver_gsc_weekly         (gsc)
  sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly (t)
  All SA360/GSC/TRENDS columns and their FULL OUTER JOINs are commented out below.
  Uncomment once those three Silver objects are built and re-add them to the FROM/JOIN
  chain and the top-level week_sun_to_sat COALESCE.

DOWNSTREAM: none (Gold Wide is a terminal ad-hoc/export table)
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_dashboardPulseByod_gold_unified_wide()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_gold_unified_wide
  USING DELTA
  AS

  SELECT
    COALESCE(p.week_sun_to_sat, g.week_sun_to_sat, sa.week_sun_to_sat, gsc.week_sun_to_sat, t.week_sun_to_sat, ab.week_sun_to_sat, aep.week_sun_to_sat, ao.week_sun_to_sat) AS week_sun_to_sat,
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
    ab.adobe_upvByod_allChannels, ab.adobe_upvByod_allChannels_wow, ab.adobe_upvByod_allChannels_ly, ab.adobe_upvByod_allChannels_wow_pct, ab.adobe_upvByod_allChannels_yoy_pct,
    ab.adobe_upvTotal_allChannels, ab.adobe_upvTotal_allChannels_wow, ab.adobe_upvTotal_allChannels_ly, ab.adobe_upvTotal_allChannels_wow_pct, ab.adobe_upvTotal_allChannels_yoy_pct,
    ab.adobe_upvFlowTotal_allChannels, ab.adobe_upvFlowTotal_allChannels_wow, ab.adobe_upvFlowTotal_allChannels_ly, ab.adobe_upvFlowTotal_allChannels_wow_pct, ab.adobe_upvFlowTotal_allChannels_yoy_pct,
    ab.adobe_pctUpvByodOfUpvFlow_allChannels, ab.adobe_pctUpvByodOfUpvFlow_allChannels_wow, ab.adobe_pctUpvByodOfUpvFlow_allChannels_ly, ab.adobe_pctUpvByodOfUpvFlow_allChannels_wow_pct, ab.adobe_pctUpvByodOfUpvFlow_allChannels_yoy_pct,
    ab.adobe_cartStartByod_allChannels, ab.adobe_cartStartByod_allChannels_wow, ab.adobe_cartStartByod_allChannels_ly, ab.adobe_cartStartByod_allChannels_wow_pct, ab.adobe_cartStartByod_allChannels_yoy_pct,
    ab.adobe_ordersUnassistedByod_allChannels, ab.adobe_ordersUnassistedByod_allChannels_wow, ab.adobe_ordersUnassistedByod_allChannels_ly, ab.adobe_ordersUnassistedByod_allChannels_wow_pct, ab.adobe_ordersUnassistedByod_allChannels_yoy_pct,
    ab.adobe_ordersAssistedByod_allChannels, ab.adobe_ordersAssistedByod_allChannels_wow, ab.adobe_ordersAssistedByod_allChannels_ly, ab.adobe_ordersAssistedByod_allChannels_wow_pct, ab.adobe_ordersAssistedByod_allChannels_yoy_pct,
    ab.adobe_ordersTotalByod_allChannels, ab.adobe_ordersTotalByod_allChannels_wow, ab.adobe_ordersTotalByod_allChannels_ly, ab.adobe_ordersTotalByod_allChannels_wow_pct, ab.adobe_ordersTotalByod_allChannels_yoy_pct,
    ab.adobe_ordersTotal_allChannels, ab.adobe_ordersTotal_allChannels_wow, ab.adobe_ordersTotal_allChannels_ly, ab.adobe_ordersTotal_allChannels_wow_pct, ab.adobe_ordersTotal_allChannels_yoy_pct,
    ab.adobe_pctOrdersByodOfOrdersTotal_allChannels, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_wow, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_ly, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_wow_pct, ab.adobe_pctOrdersByodOfOrdersTotal_allChannels_yoy_pct,
    ab.adobe_cvrByod_allChannels, ab.adobe_cvrByod_allChannels_wow, ab.adobe_cvrByod_allChannels_ly, ab.adobe_cvrByod_allChannels_wow_pct, ab.adobe_cvrByod_allChannels_yoy_pct,
    ab.adobe_cvrSite_allChannels, ab.adobe_cvrSite_allChannels_wow, ab.adobe_cvrSite_allChannels_ly, ab.adobe_cvrSite_allChannels_wow_pct, ab.adobe_cvrSite_allChannels_yoy_pct,
    ab.adobe_cvrPostpaid_allChannels, ab.adobe_cvrPostpaid_allChannels_wow, ab.adobe_cvrPostpaid_allChannels_ly, ab.adobe_cvrPostpaid_allChannels_wow_pct, ab.adobe_cvrPostpaid_allChannels_yoy_pct,
    ab.adobe_cvrHsi_allChannels, ab.adobe_cvrHsi_allChannels_wow, ab.adobe_cvrHsi_allChannels_ly, ab.adobe_cvrHsi_allChannels_wow_pct, ab.adobe_cvrHsi_allChannels_yoy_pct,
    -- PAID SEARCH
    ab.adobe_upvByod_paidSearch, ab.adobe_upvByod_paidSearch_wow, ab.adobe_upvByod_paidSearch_ly, ab.adobe_upvByod_paidSearch_wow_pct, ab.adobe_upvByod_paidSearch_yoy_pct,
    ab.adobe_pctUpvByodOfTotal_paidSearch, ab.adobe_pctUpvByodOfTotal_paidSearch_wow, ab.adobe_pctUpvByodOfTotal_paidSearch_ly, ab.adobe_pctUpvByodOfTotal_paidSearch_wow_pct, ab.adobe_pctUpvByodOfTotal_paidSearch_yoy_pct,
    ab.adobe_cartStartByod_paidSearch, ab.adobe_cartStartByod_paidSearch_wow, ab.adobe_cartStartByod_paidSearch_ly, ab.adobe_cartStartByod_paidSearch_wow_pct, ab.adobe_cartStartByod_paidSearch_yoy_pct,
    ab.adobe_ordersUnassistedByod_paidSearch, ab.adobe_ordersUnassistedByod_paidSearch_wow, ab.adobe_ordersUnassistedByod_paidSearch_ly, ab.adobe_ordersUnassistedByod_paidSearch_wow_pct, ab.adobe_ordersUnassistedByod_paidSearch_yoy_pct,
    ab.adobe_ordersAssistedByod_paidSearch, ab.adobe_ordersAssistedByod_paidSearch_wow, ab.adobe_ordersAssistedByod_paidSearch_ly, ab.adobe_ordersAssistedByod_paidSearch_wow_pct, ab.adobe_ordersAssistedByod_paidSearch_yoy_pct,
    ab.adobe_ordersTotalByod_paidSearch, ab.adobe_ordersTotalByod_paidSearch_wow, ab.adobe_ordersTotalByod_paidSearch_ly, ab.adobe_ordersTotalByod_paidSearch_wow_pct, ab.adobe_ordersTotalByod_paidSearch_yoy_pct,
    -- ORGANIC SEARCH
    ab.adobe_upvByod_organicSearch, ab.adobe_upvByod_organicSearch_wow, ab.adobe_upvByod_organicSearch_ly, ab.adobe_upvByod_organicSearch_wow_pct, ab.adobe_upvByod_organicSearch_yoy_pct,
    ab.adobe_pctUpvByodOfTotal_organicSearch, ab.adobe_pctUpvByodOfTotal_organicSearch_wow, ab.adobe_pctUpvByodOfTotal_organicSearch_ly, ab.adobe_pctUpvByodOfTotal_organicSearch_wow_pct, ab.adobe_pctUpvByodOfTotal_organicSearch_yoy_pct,
    ab.adobe_cartStartByod_organicSearch, ab.adobe_cartStartByod_organicSearch_wow, ab.adobe_cartStartByod_organicSearch_ly, ab.adobe_cartStartByod_organicSearch_wow_pct, ab.adobe_cartStartByod_organicSearch_yoy_pct,
    ab.adobe_ordersUnassistedByod_organicSearch, ab.adobe_ordersUnassistedByod_organicSearch_wow, ab.adobe_ordersUnassistedByod_organicSearch_ly, ab.adobe_ordersUnassistedByod_organicSearch_wow_pct, ab.adobe_ordersUnassistedByod_organicSearch_yoy_pct,
    ab.adobe_ordersAssistedByod_organicSearch, ab.adobe_ordersAssistedByod_organicSearch_wow, ab.adobe_ordersAssistedByod_organicSearch_ly, ab.adobe_ordersAssistedByod_organicSearch_wow_pct, ab.adobe_ordersAssistedByod_organicSearch_yoy_pct,
    ab.adobe_ordersTotalByod_organicSearch, ab.adobe_ordersTotalByod_organicSearch_wow, ab.adobe_ordersTotalByod_organicSearch_ly, ab.adobe_ordersTotalByod_organicSearch_wow_pct, ab.adobe_ordersTotalByod_organicSearch_yoy_pct,
    -- DIRECT
    ab.adobe_upvByod_direct, ab.adobe_upvByod_direct_wow, ab.adobe_upvByod_direct_ly, ab.adobe_upvByod_direct_wow_pct, ab.adobe_upvByod_direct_yoy_pct,
    ab.adobe_pctUpvByodOfTotal_direct, ab.adobe_pctUpvByodOfTotal_direct_wow, ab.adobe_pctUpvByodOfTotal_direct_ly, ab.adobe_pctUpvByodOfTotal_direct_wow_pct, ab.adobe_pctUpvByodOfTotal_direct_yoy_pct,
    ab.adobe_cartStartByod_direct, ab.adobe_cartStartByod_direct_wow, ab.adobe_cartStartByod_direct_ly, ab.adobe_cartStartByod_direct_wow_pct, ab.adobe_cartStartByod_direct_yoy_pct,
    ab.adobe_ordersUnassistedByod_direct, ab.adobe_ordersUnassistedByod_direct_wow, ab.adobe_ordersUnassistedByod_direct_ly, ab.adobe_ordersUnassistedByod_direct_wow_pct, ab.adobe_ordersUnassistedByod_direct_yoy_pct,
    ab.adobe_ordersAssistedByod_direct, ab.adobe_ordersAssistedByod_direct_wow, ab.adobe_ordersAssistedByod_direct_ly, ab.adobe_ordersAssistedByod_direct_wow_pct, ab.adobe_ordersAssistedByod_direct_yoy_pct,
    ab.adobe_ordersTotalByod_direct, ab.adobe_ordersTotalByod_direct_wow, ab.adobe_ordersTotalByod_direct_ly, ab.adobe_ordersTotalByod_direct_wow_pct, ab.adobe_ordersTotalByod_direct_yoy_pct,
    -- SOCIAL
    ab.adobe_upvByod_social, ab.adobe_upvByod_social_wow, ab.adobe_upvByod_social_ly, ab.adobe_upvByod_social_wow_pct, ab.adobe_upvByod_social_yoy_pct,
    ab.adobe_pctUpvByodOfTotal_social, ab.adobe_pctUpvByodOfTotal_social_wow, ab.adobe_pctUpvByodOfTotal_social_ly, ab.adobe_pctUpvByodOfTotal_social_wow_pct, ab.adobe_pctUpvByodOfTotal_social_yoy_pct,
    ab.adobe_cartStartByod_social, ab.adobe_cartStartByod_social_wow, ab.adobe_cartStartByod_social_ly, ab.adobe_cartStartByod_social_wow_pct, ab.adobe_cartStartByod_social_yoy_pct,
    ab.adobe_ordersUnassistedByod_social, ab.adobe_ordersUnassistedByod_social_wow, ab.adobe_ordersUnassistedByod_social_ly, ab.adobe_ordersUnassistedByod_social_wow_pct, ab.adobe_ordersUnassistedByod_social_yoy_pct,
    ab.adobe_ordersAssistedByod_social, ab.adobe_ordersAssistedByod_social_wow, ab.adobe_ordersAssistedByod_social_ly, ab.adobe_ordersAssistedByod_social_wow_pct, ab.adobe_ordersAssistedByod_social_yoy_pct,
    ab.adobe_ordersTotalByod_social, ab.adobe_ordersTotalByod_social_wow, ab.adobe_ordersTotalByod_social_ly, ab.adobe_ordersTotalByod_social_wow_pct, ab.adobe_ordersTotalByod_social_yoy_pct,
    -- PROGRAMMATIC
    ab.adobe_upvByod_programmatic, ab.adobe_upvByod_programmatic_wow, ab.adobe_upvByod_programmatic_ly, ab.adobe_upvByod_programmatic_wow_pct, ab.adobe_upvByod_programmatic_yoy_pct,
    ab.adobe_pctUpvByodOfTotal_programmatic, ab.adobe_pctUpvByodOfTotal_programmatic_wow, ab.adobe_pctUpvByodOfTotal_programmatic_ly, ab.adobe_pctUpvByodOfTotal_programmatic_wow_pct, ab.adobe_pctUpvByodOfTotal_programmatic_yoy_pct,
    ab.adobe_cartStartByod_programmatic, ab.adobe_cartStartByod_programmatic_wow, ab.adobe_cartStartByod_programmatic_ly, ab.adobe_cartStartByod_programmatic_wow_pct, ab.adobe_cartStartByod_programmatic_yoy_pct,
    ab.adobe_ordersUnassistedByod_programmatic, ab.adobe_ordersUnassistedByod_programmatic_wow, ab.adobe_ordersUnassistedByod_programmatic_ly, ab.adobe_ordersUnassistedByod_programmatic_wow_pct, ab.adobe_ordersUnassistedByod_programmatic_yoy_pct,
    ab.adobe_ordersAssistedByod_programmatic, ab.adobe_ordersAssistedByod_programmatic_wow, ab.adobe_ordersAssistedByod_programmatic_ly, ab.adobe_ordersAssistedByod_programmatic_wow_pct, ab.adobe_ordersAssistedByod_programmatic_yoy_pct,
    ab.adobe_ordersTotalByod_programmatic, ab.adobe_ordersTotalByod_programmatic_wow, ab.adobe_ordersTotalByod_programmatic_ly, ab.adobe_ordersTotalByod_programmatic_wow_pct, ab.adobe_ordersTotalByod_programmatic_yoy_pct,
    -- OTHER
    ab.adobe_upvByod_other, ab.adobe_upvByod_other_wow, ab.adobe_upvByod_other_ly, ab.adobe_upvByod_other_wow_pct, ab.adobe_upvByod_other_yoy_pct,
    ab.adobe_pctUpvByodOfTotal_other, ab.adobe_pctUpvByodOfTotal_other_wow, ab.adobe_pctUpvByodOfTotal_other_ly, ab.adobe_pctUpvByodOfTotal_other_wow_pct, ab.adobe_pctUpvByodOfTotal_other_yoy_pct,
    ab.adobe_cartStartByod_other, ab.adobe_cartStartByod_other_wow, ab.adobe_cartStartByod_other_ly, ab.adobe_cartStartByod_other_wow_pct, ab.adobe_cartStartByod_other_yoy_pct,
    ab.adobe_ordersUnassistedByod_other, ab.adobe_ordersUnassistedByod_other_wow, ab.adobe_ordersUnassistedByod_other_ly, ab.adobe_ordersUnassistedByod_other_wow_pct, ab.adobe_ordersUnassistedByod_other_yoy_pct,
    ab.adobe_ordersAssistedByod_other, ab.adobe_ordersAssistedByod_other_wow, ab.adobe_ordersAssistedByod_other_ly, ab.adobe_ordersAssistedByod_other_wow_pct, ab.adobe_ordersAssistedByod_other_yoy_pct,
    ab.adobe_ordersTotalByod_other, ab.adobe_ordersTotalByod_other_wow, ab.adobe_ordersTotalByod_other_ly, ab.adobe_ordersTotalByod_other_wow_pct, ab.adobe_ordersTotalByod_other_yoy_pct,

    -- ================================================================ ADOBE ENTRY PAGES
    aep.max_data_date AS adobe_entry_pages_max_data_date,
    aep.adobe_byodUpvVisitors_allChannels, aep.adobe_byodUpvVisitors_allChannels_wow, aep.adobe_byodUpvVisitors_allChannels_ly, aep.adobe_byodUpvVisitors_allChannels_wow_pct, aep.adobe_byodUpvVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_allChannels, aep.adobe_byodEntryByodPageVisitors_allChannels_wow, aep.adobe_byodEntryByodPageVisitors_allChannels_ly, aep.adobe_byodEntryByodPageVisitors_allChannels_wow_pct, aep.adobe_byodEntryByodPageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_allChannels, aep.adobe_byodEntryHomePageVisitors_allChannels_wow, aep.adobe_byodEntryHomePageVisitors_allChannels_ly, aep.adobe_byodEntryHomePageVisitors_allChannels_wow_pct, aep.adobe_byodEntryHomePageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_allChannels, aep.adobe_byodEntryDevicePageVisitors_allChannels_wow, aep.adobe_byodEntryDevicePageVisitors_allChannels_ly, aep.adobe_byodEntryDevicePageVisitors_allChannels_wow_pct, aep.adobe_byodEntryDevicePageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_allChannels, aep.adobe_byodEntryPlansPageVisitors_allChannels_wow, aep.adobe_byodEntryPlansPageVisitors_allChannels_ly, aep.adobe_byodEntryPlansPageVisitors_allChannels_wow_pct, aep.adobe_byodEntryPlansPageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_allChannels, aep.adobe_byodEntryOtherPageVisitors_allChannels_wow, aep.adobe_byodEntryOtherPageVisitors_allChannels_ly, aep.adobe_byodEntryOtherPageVisitors_allChannels_wow_pct, aep.adobe_byodEntryOtherPageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryGetStartedLandingPageVisitors_allChannels, aep.adobe_byodEntryGetStartedLandingPageVisitors_allChannels_wow, aep.adobe_byodEntryGetStartedLandingPageVisitors_allChannels_ly, aep.adobe_byodEntryGetStartedLandingPageVisitors_allChannels_wow_pct, aep.adobe_byodEntryGetStartedLandingPageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryByodLandingPageVisitors_allChannels, aep.adobe_byodEntryByodLandingPageVisitors_allChannels_wow, aep.adobe_byodEntryByodLandingPageVisitors_allChannels_ly, aep.adobe_byodEntryByodLandingPageVisitors_allChannels_wow_pct, aep.adobe_byodEntryByodLandingPageVisitors_allChannels_yoy_pct,
    aep.adobe_byodEntryOffersSwitchVisitors_allChannels, aep.adobe_byodEntryOffersSwitchVisitors_allChannels_wow, aep.adobe_byodEntryOffersSwitchVisitors_allChannels_ly, aep.adobe_byodEntryOffersSwitchVisitors_allChannels_wow_pct, aep.adobe_byodEntryOffersSwitchVisitors_allChannels_yoy_pct,
    aep.adobe_byodUpvVisitors_paidSearch, aep.adobe_byodUpvVisitors_paidSearch_wow, aep.adobe_byodUpvVisitors_paidSearch_ly, aep.adobe_byodUpvVisitors_paidSearch_wow_pct, aep.adobe_byodUpvVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_paidSearch, aep.adobe_byodEntryByodPageVisitors_paidSearch_wow, aep.adobe_byodEntryByodPageVisitors_paidSearch_ly, aep.adobe_byodEntryByodPageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryByodPageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_paidSearch, aep.adobe_byodEntryHomePageVisitors_paidSearch_wow, aep.adobe_byodEntryHomePageVisitors_paidSearch_ly, aep.adobe_byodEntryHomePageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryHomePageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_paidSearch, aep.adobe_byodEntryDevicePageVisitors_paidSearch_wow, aep.adobe_byodEntryDevicePageVisitors_paidSearch_ly, aep.adobe_byodEntryDevicePageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryDevicePageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_paidSearch, aep.adobe_byodEntryPlansPageVisitors_paidSearch_wow, aep.adobe_byodEntryPlansPageVisitors_paidSearch_ly, aep.adobe_byodEntryPlansPageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryPlansPageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_paidSearch, aep.adobe_byodEntryOtherPageVisitors_paidSearch_wow, aep.adobe_byodEntryOtherPageVisitors_paidSearch_ly, aep.adobe_byodEntryOtherPageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryOtherPageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryGetStartedLandingPageVisitors_paidSearch, aep.adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_wow, aep.adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_ly, aep.adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryByodLandingPageVisitors_paidSearch, aep.adobe_byodEntryByodLandingPageVisitors_paidSearch_wow, aep.adobe_byodEntryByodLandingPageVisitors_paidSearch_ly, aep.adobe_byodEntryByodLandingPageVisitors_paidSearch_wow_pct, aep.adobe_byodEntryByodLandingPageVisitors_paidSearch_yoy_pct,
    aep.adobe_byodEntryOffersSwitchVisitors_paidSearch, aep.adobe_byodEntryOffersSwitchVisitors_paidSearch_wow, aep.adobe_byodEntryOffersSwitchVisitors_paidSearch_ly, aep.adobe_byodEntryOffersSwitchVisitors_paidSearch_wow_pct, aep.adobe_byodEntryOffersSwitchVisitors_paidSearch_yoy_pct,
    aep.adobe_byodUpvVisitors_organicSearch, aep.adobe_byodUpvVisitors_organicSearch_wow, aep.adobe_byodUpvVisitors_organicSearch_ly, aep.adobe_byodUpvVisitors_organicSearch_wow_pct, aep.adobe_byodUpvVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_organicSearch, aep.adobe_byodEntryByodPageVisitors_organicSearch_wow, aep.adobe_byodEntryByodPageVisitors_organicSearch_ly, aep.adobe_byodEntryByodPageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryByodPageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_organicSearch, aep.adobe_byodEntryHomePageVisitors_organicSearch_wow, aep.adobe_byodEntryHomePageVisitors_organicSearch_ly, aep.adobe_byodEntryHomePageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryHomePageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_organicSearch, aep.adobe_byodEntryDevicePageVisitors_organicSearch_wow, aep.adobe_byodEntryDevicePageVisitors_organicSearch_ly, aep.adobe_byodEntryDevicePageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryDevicePageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_organicSearch, aep.adobe_byodEntryPlansPageVisitors_organicSearch_wow, aep.adobe_byodEntryPlansPageVisitors_organicSearch_ly, aep.adobe_byodEntryPlansPageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryPlansPageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_organicSearch, aep.adobe_byodEntryOtherPageVisitors_organicSearch_wow, aep.adobe_byodEntryOtherPageVisitors_organicSearch_ly, aep.adobe_byodEntryOtherPageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryOtherPageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryGetStartedLandingPageVisitors_organicSearch, aep.adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_wow, aep.adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_ly, aep.adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryByodLandingPageVisitors_organicSearch, aep.adobe_byodEntryByodLandingPageVisitors_organicSearch_wow, aep.adobe_byodEntryByodLandingPageVisitors_organicSearch_ly, aep.adobe_byodEntryByodLandingPageVisitors_organicSearch_wow_pct, aep.adobe_byodEntryByodLandingPageVisitors_organicSearch_yoy_pct,
    aep.adobe_byodEntryOffersSwitchVisitors_organicSearch, aep.adobe_byodEntryOffersSwitchVisitors_organicSearch_wow, aep.adobe_byodEntryOffersSwitchVisitors_organicSearch_ly, aep.adobe_byodEntryOffersSwitchVisitors_organicSearch_wow_pct, aep.adobe_byodEntryOffersSwitchVisitors_organicSearch_yoy_pct,
    aep.adobe_byodUpvVisitors_direct, aep.adobe_byodUpvVisitors_direct_wow, aep.adobe_byodUpvVisitors_direct_ly, aep.adobe_byodUpvVisitors_direct_wow_pct, aep.adobe_byodUpvVisitors_direct_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_direct, aep.adobe_byodEntryByodPageVisitors_direct_wow, aep.adobe_byodEntryByodPageVisitors_direct_ly, aep.adobe_byodEntryByodPageVisitors_direct_wow_pct, aep.adobe_byodEntryByodPageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_direct, aep.adobe_byodEntryHomePageVisitors_direct_wow, aep.adobe_byodEntryHomePageVisitors_direct_ly, aep.adobe_byodEntryHomePageVisitors_direct_wow_pct, aep.adobe_byodEntryHomePageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_direct, aep.adobe_byodEntryDevicePageVisitors_direct_wow, aep.adobe_byodEntryDevicePageVisitors_direct_ly, aep.adobe_byodEntryDevicePageVisitors_direct_wow_pct, aep.adobe_byodEntryDevicePageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_direct, aep.adobe_byodEntryPlansPageVisitors_direct_wow, aep.adobe_byodEntryPlansPageVisitors_direct_ly, aep.adobe_byodEntryPlansPageVisitors_direct_wow_pct, aep.adobe_byodEntryPlansPageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_direct, aep.adobe_byodEntryOtherPageVisitors_direct_wow, aep.adobe_byodEntryOtherPageVisitors_direct_ly, aep.adobe_byodEntryOtherPageVisitors_direct_wow_pct, aep.adobe_byodEntryOtherPageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryGetStartedLandingPageVisitors_direct, aep.adobe_byodEntryGetStartedLandingPageVisitors_direct_wow, aep.adobe_byodEntryGetStartedLandingPageVisitors_direct_ly, aep.adobe_byodEntryGetStartedLandingPageVisitors_direct_wow_pct, aep.adobe_byodEntryGetStartedLandingPageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryByodLandingPageVisitors_direct, aep.adobe_byodEntryByodLandingPageVisitors_direct_wow, aep.adobe_byodEntryByodLandingPageVisitors_direct_ly, aep.adobe_byodEntryByodLandingPageVisitors_direct_wow_pct, aep.adobe_byodEntryByodLandingPageVisitors_direct_yoy_pct,
    aep.adobe_byodEntryOffersSwitchVisitors_direct, aep.adobe_byodEntryOffersSwitchVisitors_direct_wow, aep.adobe_byodEntryOffersSwitchVisitors_direct_ly, aep.adobe_byodEntryOffersSwitchVisitors_direct_wow_pct, aep.adobe_byodEntryOffersSwitchVisitors_direct_yoy_pct,
    aep.adobe_byodUpvVisitors_social, aep.adobe_byodUpvVisitors_social_wow, aep.adobe_byodUpvVisitors_social_ly, aep.adobe_byodUpvVisitors_social_wow_pct, aep.adobe_byodUpvVisitors_social_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_social, aep.adobe_byodEntryByodPageVisitors_social_wow, aep.adobe_byodEntryByodPageVisitors_social_ly, aep.adobe_byodEntryByodPageVisitors_social_wow_pct, aep.adobe_byodEntryByodPageVisitors_social_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_social, aep.adobe_byodEntryHomePageVisitors_social_wow, aep.adobe_byodEntryHomePageVisitors_social_ly, aep.adobe_byodEntryHomePageVisitors_social_wow_pct, aep.adobe_byodEntryHomePageVisitors_social_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_social, aep.adobe_byodEntryDevicePageVisitors_social_wow, aep.adobe_byodEntryDevicePageVisitors_social_ly, aep.adobe_byodEntryDevicePageVisitors_social_wow_pct, aep.adobe_byodEntryDevicePageVisitors_social_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_social, aep.adobe_byodEntryPlansPageVisitors_social_wow, aep.adobe_byodEntryPlansPageVisitors_social_ly, aep.adobe_byodEntryPlansPageVisitors_social_wow_pct, aep.adobe_byodEntryPlansPageVisitors_social_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_social, aep.adobe_byodEntryOtherPageVisitors_social_wow, aep.adobe_byodEntryOtherPageVisitors_social_ly, aep.adobe_byodEntryOtherPageVisitors_social_wow_pct, aep.adobe_byodEntryOtherPageVisitors_social_yoy_pct,
    aep.adobe_byodEntryGetStartedLandingPageVisitors_social, aep.adobe_byodEntryGetStartedLandingPageVisitors_social_wow, aep.adobe_byodEntryGetStartedLandingPageVisitors_social_ly, aep.adobe_byodEntryGetStartedLandingPageVisitors_social_wow_pct, aep.adobe_byodEntryGetStartedLandingPageVisitors_social_yoy_pct,
    aep.adobe_byodEntryByodLandingPageVisitors_social, aep.adobe_byodEntryByodLandingPageVisitors_social_wow, aep.adobe_byodEntryByodLandingPageVisitors_social_ly, aep.adobe_byodEntryByodLandingPageVisitors_social_wow_pct, aep.adobe_byodEntryByodLandingPageVisitors_social_yoy_pct,
    aep.adobe_byodEntryOffersSwitchVisitors_social, aep.adobe_byodEntryOffersSwitchVisitors_social_wow, aep.adobe_byodEntryOffersSwitchVisitors_social_ly, aep.adobe_byodEntryOffersSwitchVisitors_social_wow_pct, aep.adobe_byodEntryOffersSwitchVisitors_social_yoy_pct,
    aep.adobe_byodUpvVisitors_programmatic, aep.adobe_byodUpvVisitors_programmatic_wow, aep.adobe_byodUpvVisitors_programmatic_ly, aep.adobe_byodUpvVisitors_programmatic_wow_pct, aep.adobe_byodUpvVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_programmatic, aep.adobe_byodEntryByodPageVisitors_programmatic_wow, aep.adobe_byodEntryByodPageVisitors_programmatic_ly, aep.adobe_byodEntryByodPageVisitors_programmatic_wow_pct, aep.adobe_byodEntryByodPageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_programmatic, aep.adobe_byodEntryHomePageVisitors_programmatic_wow, aep.adobe_byodEntryHomePageVisitors_programmatic_ly, aep.adobe_byodEntryHomePageVisitors_programmatic_wow_pct, aep.adobe_byodEntryHomePageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_programmatic, aep.adobe_byodEntryDevicePageVisitors_programmatic_wow, aep.adobe_byodEntryDevicePageVisitors_programmatic_ly, aep.adobe_byodEntryDevicePageVisitors_programmatic_wow_pct, aep.adobe_byodEntryDevicePageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_programmatic, aep.adobe_byodEntryPlansPageVisitors_programmatic_wow, aep.adobe_byodEntryPlansPageVisitors_programmatic_ly, aep.adobe_byodEntryPlansPageVisitors_programmatic_wow_pct, aep.adobe_byodEntryPlansPageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_programmatic, aep.adobe_byodEntryOtherPageVisitors_programmatic_wow, aep.adobe_byodEntryOtherPageVisitors_programmatic_ly, aep.adobe_byodEntryOtherPageVisitors_programmatic_wow_pct, aep.adobe_byodEntryOtherPageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryGetStartedLandingPageVisitors_programmatic, aep.adobe_byodEntryGetStartedLandingPageVisitors_programmatic_wow, aep.adobe_byodEntryGetStartedLandingPageVisitors_programmatic_ly, aep.adobe_byodEntryGetStartedLandingPageVisitors_programmatic_wow_pct, aep.adobe_byodEntryGetStartedLandingPageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryByodLandingPageVisitors_programmatic, aep.adobe_byodEntryByodLandingPageVisitors_programmatic_wow, aep.adobe_byodEntryByodLandingPageVisitors_programmatic_ly, aep.adobe_byodEntryByodLandingPageVisitors_programmatic_wow_pct, aep.adobe_byodEntryByodLandingPageVisitors_programmatic_yoy_pct,
    aep.adobe_byodEntryOffersSwitchVisitors_programmatic, aep.adobe_byodEntryOffersSwitchVisitors_programmatic_wow, aep.adobe_byodEntryOffersSwitchVisitors_programmatic_ly, aep.adobe_byodEntryOffersSwitchVisitors_programmatic_wow_pct, aep.adobe_byodEntryOffersSwitchVisitors_programmatic_yoy_pct,
    aep.adobe_byodUpvVisitors_other, aep.adobe_byodUpvVisitors_other_wow, aep.adobe_byodUpvVisitors_other_ly, aep.adobe_byodUpvVisitors_other_wow_pct, aep.adobe_byodUpvVisitors_other_yoy_pct,
    aep.adobe_byodEntryByodPageVisitors_other, aep.adobe_byodEntryByodPageVisitors_other_wow, aep.adobe_byodEntryByodPageVisitors_other_ly, aep.adobe_byodEntryByodPageVisitors_other_wow_pct, aep.adobe_byodEntryByodPageVisitors_other_yoy_pct,
    aep.adobe_byodEntryHomePageVisitors_other, aep.adobe_byodEntryHomePageVisitors_other_wow, aep.adobe_byodEntryHomePageVisitors_other_ly, aep.adobe_byodEntryHomePageVisitors_other_wow_pct, aep.adobe_byodEntryHomePageVisitors_other_yoy_pct,
    aep.adobe_byodEntryDevicePageVisitors_other, aep.adobe_byodEntryDevicePageVisitors_other_wow, aep.adobe_byodEntryDevicePageVisitors_other_ly, aep.adobe_byodEntryDevicePageVisitors_other_wow_pct, aep.adobe_byodEntryDevicePageVisitors_other_yoy_pct,
    aep.adobe_byodEntryPlansPageVisitors_other, aep.adobe_byodEntryPlansPageVisitors_other_wow, aep.adobe_byodEntryPlansPageVisitors_other_ly, aep.adobe_byodEntryPlansPageVisitors_other_wow_pct, aep.adobe_byodEntryPlansPageVisitors_other_yoy_pct,
    aep.adobe_byodEntryOtherPageVisitors_other, aep.adobe_byodEntryOtherPageVisitors_other_wow, aep.adobe_byodEntryOtherPageVisitors_other_ly, aep.adobe_byodEntryOtherPageVisitors_other_wow_pct, aep.adobe_byodEntryOtherPageVisitors_other_yoy_pct,
    aep.adobe_byodEntryGetStartedLandingPageVisitors_other, aep.adobe_byodEntryGetStartedLandingPageVisitors_other_wow, aep.adobe_byodEntryGetStartedLandingPageVisitors_other_ly, aep.adobe_byodEntryGetStartedLandingPageVisitors_other_wow_pct, aep.adobe_byodEntryGetStartedLandingPageVisitors_other_yoy_pct,
    aep.adobe_byodEntryByodLandingPageVisitors_other, aep.adobe_byodEntryByodLandingPageVisitors_other_wow, aep.adobe_byodEntryByodLandingPageVisitors_other_ly, aep.adobe_byodEntryByodLandingPageVisitors_other_wow_pct, aep.adobe_byodEntryByodLandingPageVisitors_other_yoy_pct,
    aep.adobe_byodEntryOffersSwitchVisitors_other, aep.adobe_byodEntryOffersSwitchVisitors_other_wow, aep.adobe_byodEntryOffersSwitchVisitors_other_ly, aep.adobe_byodEntryOffersSwitchVisitors_other_wow_pct, aep.adobe_byodEntryOffersSwitchVisitors_other_yoy_pct,

    -- ================================================================ ADOBE OUTCOMES
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

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly p
  FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly g
    ON p.week_sun_to_sat = g.week_sun_to_sat
  FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly sa
    ON COALESCE(p.week_sun_to_sat, g.week_sun_to_sat) = sa.week_sun_to_sat
  FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_gsc_weekly gsc
    ON COALESCE(p.week_sun_to_sat, g.week_sun_to_sat, sa.week_sun_to_sat) = gsc.week_sun_to_sat
  FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly t
    ON COALESCE(p.week_sun_to_sat, g.week_sun_to_sat, sa.week_sun_to_sat, gsc.week_sun_to_sat) = t.week_sun_to_sat
  FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly ab
    ON COALESCE(p.week_sun_to_sat, g.week_sun_to_sat, sa.week_sun_to_sat, gsc.week_sun_to_sat, t.week_sun_to_sat) = ab.week_sun_to_sat
  FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly aep
    ON COALESCE(p.week_sun_to_sat, g.week_sun_to_sat, sa.week_sun_to_sat, gsc.week_sun_to_sat, t.week_sun_to_sat, ab.week_sun_to_sat) = aep.week_sun_to_sat
  FULL OUTER JOIN prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly ao
    ON COALESCE(p.week_sun_to_sat, g.week_sun_to_sat, sa.week_sun_to_sat, gsc.week_sun_to_sat, t.week_sun_to_sat, ab.week_sun_to_sat, aep.week_sun_to_sat) = ao.week_sun_to_sat

  ORDER BY week_sun_to_sat ASC
  ;

END;