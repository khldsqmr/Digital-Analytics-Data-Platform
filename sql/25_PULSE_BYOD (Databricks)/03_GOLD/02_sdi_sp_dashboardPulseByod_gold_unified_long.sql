/* =================================================================================================
FILE:         sdi_sp_dashboardPulseByod_gold_unified_long.sql
LAYER:        Gold Table — Long (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
TABLE:        sdi_tbl_dashboardPulseByod_gold_unified_long
PROCEDURE:    sdi_sp_dashboardPulseByod_gold_unified_long

PURPOSE:
  Gold Long table for the Pulse BYOD dashboard.
  Reads directly from Silver tables — NOT from Gold Wide.
  Each Silver table unpivoted independently, then stacked via UNION ALL.
  One row per metric per week.

SOURCES (active): profound, profoundGofish, adobe, adobeByodEntryPages, adobeByodOutcomes
SOURCES (commented out — pending sa360/gsc/googleTrends Silver, not yet provided):
  sa360_long, gsc_long, trends_index_long, trends_keywords_long CTEs, and their
  corresponding UNION ALL blocks in the combined CTE. Uncomment once those three
  Silver objects are built.

DOWNSTREAM: Pulse BYOD dashboard (reads this table directly)
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_dashboardPulseByod_gold_unified_long()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_gold_unified_long
  USING DELTA
  AS

  WITH

  -- -----------------------------------------------------------------------
  -- PROFOUND: 15 rows/week
  -- -----------------------------------------------------------------------
  profound_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNPIVOT (
          (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
          FOR metric_name IN (
              (profoundVis_tmo_nonbrand_visibilityScore,    profoundVis_tmo_nonbrand_visibilityScore_wow,    profoundVis_tmo_nonbrand_visibilityScore_ly,    profoundVis_tmo_nonbrand_visibilityScore_wow_pct,    profoundVis_tmo_nonbrand_visibilityScore_yoy_pct)    'profoundVis_tmo_nonbrand_visibilityScore',
              (profoundVis_tmo_nonbrand_executions,         profoundVis_tmo_nonbrand_executions_wow,         profoundVis_tmo_nonbrand_executions_ly,         profoundVis_tmo_nonbrand_executions_wow_pct,         profoundVis_tmo_nonbrand_executions_yoy_pct)         'profoundVis_tmo_nonbrand_executions',
              (profoundVis_tmo_nonbrand_mentionsCount,      profoundVis_tmo_nonbrand_mentionsCount_wow,      profoundVis_tmo_nonbrand_mentionsCount_ly,      profoundVis_tmo_nonbrand_mentionsCount_wow_pct,      profoundVis_tmo_nonbrand_mentionsCount_yoy_pct)      'profoundVis_tmo_nonbrand_mentionsCount',
              (profoundVis_tmo_nonbrand_shareOfVoice,       profoundVis_tmo_nonbrand_shareOfVoice_wow,       profoundVis_tmo_nonbrand_shareOfVoice_ly,       profoundVis_tmo_nonbrand_shareOfVoice_wow_pct,       profoundVis_tmo_nonbrand_shareOfVoice_yoy_pct)       'profoundVis_tmo_nonbrand_shareOfVoice',
              (profoundVis_verizon_nonbrand_visibilityScore,profoundVis_verizon_nonbrand_visibilityScore_wow,profoundVis_verizon_nonbrand_visibilityScore_ly,profoundVis_verizon_nonbrand_visibilityScore_wow_pct,profoundVis_verizon_nonbrand_visibilityScore_yoy_pct) 'profoundVis_verizon_nonbrand_visibilityScore',
              (profoundVis_verizon_nonbrand_executions,     profoundVis_verizon_nonbrand_executions_wow,     profoundVis_verizon_nonbrand_executions_ly,     profoundVis_verizon_nonbrand_executions_wow_pct,     profoundVis_verizon_nonbrand_executions_yoy_pct)     'profoundVis_verizon_nonbrand_executions',
              (profoundVis_verizon_nonbrand_mentionsCount,  profoundVis_verizon_nonbrand_mentionsCount_wow,  profoundVis_verizon_nonbrand_mentionsCount_ly,  profoundVis_verizon_nonbrand_mentionsCount_wow_pct,  profoundVis_verizon_nonbrand_mentionsCount_yoy_pct)  'profoundVis_verizon_nonbrand_mentionsCount',
              (profoundVis_verizon_nonbrand_shareOfVoice,   profoundVis_verizon_nonbrand_shareOfVoice_wow,   profoundVis_verizon_nonbrand_shareOfVoice_ly,   profoundVis_verizon_nonbrand_shareOfVoice_wow_pct,   profoundVis_verizon_nonbrand_shareOfVoice_yoy_pct)   'profoundVis_verizon_nonbrand_shareOfVoice',
              (profoundVis_att_nonbrand_visibilityScore,    profoundVis_att_nonbrand_visibilityScore_wow,    profoundVis_att_nonbrand_visibilityScore_ly,    profoundVis_att_nonbrand_visibilityScore_wow_pct,    profoundVis_att_nonbrand_visibilityScore_yoy_pct)    'profoundVis_att_nonbrand_visibilityScore',
              (profoundVis_att_nonbrand_executions,         profoundVis_att_nonbrand_executions_wow,         profoundVis_att_nonbrand_executions_ly,         profoundVis_att_nonbrand_executions_wow_pct,         profoundVis_att_nonbrand_executions_yoy_pct)         'profoundVis_att_nonbrand_executions',
              (profoundVis_att_nonbrand_mentionsCount,      profoundVis_att_nonbrand_mentionsCount_wow,      profoundVis_att_nonbrand_mentionsCount_ly,      profoundVis_att_nonbrand_mentionsCount_wow_pct,      profoundVis_att_nonbrand_mentionsCount_yoy_pct)      'profoundVis_att_nonbrand_mentionsCount',
              (profoundVis_att_nonbrand_shareOfVoice,       profoundVis_att_nonbrand_shareOfVoice_wow,       profoundVis_att_nonbrand_shareOfVoice_ly,       profoundVis_att_nonbrand_shareOfVoice_wow_pct,       profoundVis_att_nonbrand_shareOfVoice_yoy_pct)       'profoundVis_att_nonbrand_shareOfVoice',
              (profoundCit_tmo_nonbrand_shareOfVoice,       profoundCit_tmo_nonbrand_shareOfVoice_wow,       profoundCit_tmo_nonbrand_shareOfVoice_ly,       profoundCit_tmo_nonbrand_shareOfVoice_wow_pct,       profoundCit_tmo_nonbrand_shareOfVoice_yoy_pct)       'profoundCit_tmo_nonbrand_shareOfVoice',
              (profoundCit_verizon_nonbrand_shareOfVoice,   profoundCit_verizon_nonbrand_shareOfVoice_wow,   profoundCit_verizon_nonbrand_shareOfVoice_ly,   profoundCit_verizon_nonbrand_shareOfVoice_wow_pct,   profoundCit_verizon_nonbrand_shareOfVoice_yoy_pct)   'profoundCit_verizon_nonbrand_shareOfVoice',
              (profoundCit_att_nonbrand_shareOfVoice,       profoundCit_att_nonbrand_shareOfVoice_wow,       profoundCit_att_nonbrand_shareOfVoice_ly,       profoundCit_att_nonbrand_shareOfVoice_wow_pct,       profoundCit_att_nonbrand_shareOfVoice_yoy_pct)       'profoundCit_att_nonbrand_shareOfVoice'
          )
      )
  ),

  -- -----------------------------------------------------------------------
  -- GOFISH: 12 rows/week
  -- -----------------------------------------------------------------------
  gofish_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNPIVOT (
          (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
          FOR metric_name IN (
              (gofish_tmo_brand_visibilityScore,    gofish_tmo_brand_visibilityScore_wow,    gofish_tmo_brand_visibilityScore_ly,    gofish_tmo_brand_visibilityScore_wow_pct,    gofish_tmo_brand_visibilityScore_yoy_pct)    'gofish_tmo_brand_visibilityScore',
              (gofish_tmo_brand_executions,         gofish_tmo_brand_executions_wow,         gofish_tmo_brand_executions_ly,         gofish_tmo_brand_executions_wow_pct,         gofish_tmo_brand_executions_yoy_pct)         'gofish_tmo_brand_executions',
              (gofish_tmo_brand_mentionsCount,      gofish_tmo_brand_mentionsCount_wow,      gofish_tmo_brand_mentionsCount_ly,      gofish_tmo_brand_mentionsCount_wow_pct,      gofish_tmo_brand_mentionsCount_yoy_pct)      'gofish_tmo_brand_mentionsCount',
              (gofish_tmo_brand_shareOfVoice,       gofish_tmo_brand_shareOfVoice_wow,       gofish_tmo_brand_shareOfVoice_ly,       gofish_tmo_brand_shareOfVoice_wow_pct,       gofish_tmo_brand_shareOfVoice_yoy_pct)       'gofish_tmo_brand_shareOfVoice',
              (gofish_verizon_brand_visibilityScore,gofish_verizon_brand_visibilityScore_wow,gofish_verizon_brand_visibilityScore_ly,gofish_verizon_brand_visibilityScore_wow_pct,gofish_verizon_brand_visibilityScore_yoy_pct) 'gofish_verizon_brand_visibilityScore',
              (gofish_verizon_brand_executions,     gofish_verizon_brand_executions_wow,     gofish_verizon_brand_executions_ly,     gofish_verizon_brand_executions_wow_pct,     gofish_verizon_brand_executions_yoy_pct)     'gofish_verizon_brand_executions',
              (gofish_verizon_brand_mentionsCount,  gofish_verizon_brand_mentionsCount_wow,  gofish_verizon_brand_mentionsCount_ly,  gofish_verizon_brand_mentionsCount_wow_pct,  gofish_verizon_brand_mentionsCount_yoy_pct)  'gofish_verizon_brand_mentionsCount',
              (gofish_verizon_brand_shareOfVoice,   gofish_verizon_brand_shareOfVoice_wow,   gofish_verizon_brand_shareOfVoice_ly,   gofish_verizon_brand_shareOfVoice_wow_pct,   gofish_verizon_brand_shareOfVoice_yoy_pct)   'gofish_verizon_brand_shareOfVoice',
              (gofish_att_brand_visibilityScore,    gofish_att_brand_visibilityScore_wow,    gofish_att_brand_visibilityScore_ly,    gofish_att_brand_visibilityScore_wow_pct,    gofish_att_brand_visibilityScore_yoy_pct)    'gofish_att_brand_visibilityScore',
              (gofish_att_brand_executions,         gofish_att_brand_executions_wow,         gofish_att_brand_executions_ly,         gofish_att_brand_executions_wow_pct,         gofish_att_brand_executions_yoy_pct)         'gofish_att_brand_executions',
              (gofish_att_brand_mentionsCount,      gofish_att_brand_mentionsCount_wow,      gofish_att_brand_mentionsCount_ly,      gofish_att_brand_mentionsCount_wow_pct,      gofish_att_brand_mentionsCount_yoy_pct)      'gofish_att_brand_mentionsCount',
              (gofish_att_brand_shareOfVoice,       gofish_att_brand_shareOfVoice_wow,       gofish_att_brand_shareOfVoice_ly,       gofish_att_brand_shareOfVoice_wow_pct,       gofish_att_brand_shareOfVoice_yoy_pct)       'gofish_att_brand_shareOfVoice'
          )
      )
  ),

  /* -----------------------------------------------------------------------
  -- SA360: 12 rows/week — COMMENTED OUT (Silver not yet built)
  -- -----------------------------------------------------------------------
  sa360_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNPIVOT (
          (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
          FOR metric_name IN (
              (sa360_tmo_brand_impressions,      sa360_tmo_brand_impressions_wow,      sa360_tmo_brand_impressions_ly,      sa360_tmo_brand_impressions_wow_pct,      sa360_tmo_brand_impressions_yoy_pct)      'sa360_tmo_brand_impressions',
              (sa360_tmo_brand_clicks,           sa360_tmo_brand_clicks_wow,           sa360_tmo_brand_clicks_ly,           sa360_tmo_brand_clicks_wow_pct,           sa360_tmo_brand_clicks_yoy_pct)           'sa360_tmo_brand_clicks',
              (sa360_tmo_brand_cost,             sa360_tmo_brand_cost_wow,             sa360_tmo_brand_cost_ly,             sa360_tmo_brand_cost_wow_pct,             sa360_tmo_brand_cost_yoy_pct)             'sa360_tmo_brand_cost',
              (sa360_tmo_brand_orders,           sa360_tmo_brand_orders_wow,           sa360_tmo_brand_orders_ly,           sa360_tmo_brand_orders_wow_pct,           sa360_tmo_brand_orders_yoy_pct)           'sa360_tmo_brand_orders',
              (sa360_tmo_brand_cart_start,       sa360_tmo_brand_cart_start_wow,       sa360_tmo_brand_cart_start_ly,       sa360_tmo_brand_cart_start_wow_pct,       sa360_tmo_brand_cart_start_yoy_pct)       'sa360_tmo_brand_cart_start',
              (sa360_tmo_brand_postpaid_pspv,    sa360_tmo_brand_postpaid_pspv_wow,    sa360_tmo_brand_postpaid_pspv_ly,    sa360_tmo_brand_postpaid_pspv_wow_pct,    sa360_tmo_brand_postpaid_pspv_yoy_pct)    'sa360_tmo_brand_postpaid_pspv',
              (sa360_tmo_nonbrand_impressions,   sa360_tmo_nonbrand_impressions_wow,   sa360_tmo_nonbrand_impressions_ly,   sa360_tmo_nonbrand_impressions_wow_pct,   sa360_tmo_nonbrand_impressions_yoy_pct)   'sa360_tmo_nonbrand_impressions',
              (sa360_tmo_nonbrand_clicks,        sa360_tmo_nonbrand_clicks_wow,        sa360_tmo_nonbrand_clicks_ly,        sa360_tmo_nonbrand_clicks_wow_pct,        sa360_tmo_nonbrand_clicks_yoy_pct)        'sa360_tmo_nonbrand_clicks',
              (sa360_tmo_nonbrand_cost,          sa360_tmo_nonbrand_cost_wow,          sa360_tmo_nonbrand_cost_ly,          sa360_tmo_nonbrand_cost_wow_pct,          sa360_tmo_nonbrand_cost_yoy_pct)          'sa360_tmo_nonbrand_cost',
              (sa360_tmo_nonbrand_orders,        sa360_tmo_nonbrand_orders_wow,        sa360_tmo_nonbrand_orders_ly,        sa360_tmo_nonbrand_orders_wow_pct,        sa360_tmo_nonbrand_orders_yoy_pct)        'sa360_tmo_nonbrand_orders',
              (sa360_tmo_nonbrand_cart_start,    sa360_tmo_nonbrand_cart_start_wow,    sa360_tmo_nonbrand_cart_start_ly,    sa360_tmo_nonbrand_cart_start_wow_pct,    sa360_tmo_nonbrand_cart_start_yoy_pct)    'sa360_tmo_nonbrand_cart_start',
              (sa360_tmo_nonbrand_postpaid_pspv, sa360_tmo_nonbrand_postpaid_pspv_wow, sa360_tmo_nonbrand_postpaid_pspv_ly, sa360_tmo_nonbrand_postpaid_pspv_wow_pct, sa360_tmo_nonbrand_postpaid_pspv_yoy_pct) 'sa360_tmo_nonbrand_postpaid_pspv'
          )
      )
  ),

  -- -----------------------------------------------------------------------
  -- GSC: 4 rows/week — COMMENTED OUT (Silver not yet built)
  -- -----------------------------------------------------------------------
  gsc_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_gsc_weekly
      UNPIVOT (
          (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
          FOR metric_name IN (
              (gsc_tmo_brand_impressions,    gsc_tmo_brand_impressions_wow,    gsc_tmo_brand_impressions_ly,    gsc_tmo_brand_impressions_wow_pct,    gsc_tmo_brand_impressions_yoy_pct)    'gsc_tmo_brand_impressions',
              (gsc_tmo_brand_clicks,         gsc_tmo_brand_clicks_wow,         gsc_tmo_brand_clicks_ly,         gsc_tmo_brand_clicks_wow_pct,         gsc_tmo_brand_clicks_yoy_pct)         'gsc_tmo_brand_clicks',
              (gsc_tmo_nonbrand_impressions, gsc_tmo_nonbrand_impressions_wow, gsc_tmo_nonbrand_impressions_ly, gsc_tmo_nonbrand_impressions_wow_pct, gsc_tmo_nonbrand_impressions_yoy_pct) 'gsc_tmo_nonbrand_impressions',
              (gsc_tmo_nonbrand_clicks,      gsc_tmo_nonbrand_clicks_wow,      gsc_tmo_nonbrand_clicks_ly,      gsc_tmo_nonbrand_clicks_wow_pct,      gsc_tmo_nonbrand_clicks_yoy_pct)      'gsc_tmo_nonbrand_clicks'
          )
      )
  ),

  -- -----------------------------------------------------------------------
  -- TRENDS: byod_index — 1 row/week — COMMENTED OUT (Silver not yet built)
  -- -----------------------------------------------------------------------
  trends_index_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly
      UNPIVOT (
          (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
          FOR metric_name IN (
              (trends_byod_index, trends_byod_index_wow, trends_byod_index_ly, trends_byod_index_wow_pct, trends_byod_index_yoy_pct) 'trends_byod_index'
          )
      )
  ),

  -- -----------------------------------------------------------------------
  -- TRENDS: Keywords — up to 10 rows/week — COMMENTED OUT (Silver not yet built)
  -- -----------------------------------------------------------------------
  trends_keywords_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_1' AS dimension_name, trends_top_kw_1 AS dimension_value, 'trends_kw_interest' AS metric_name, trends_kw1_interest AS metric_value, CAST(NULL AS DOUBLE) AS metric_value_wow, CAST(NULL AS DOUBLE) AS metric_value_ly, CAST(NULL AS DOUBLE) AS wow_pct, CAST(NULL AS DOUBLE) AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL
      UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_1', trends_top_kw_1, 'trends_kw_change', trends_kw1_change, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_1), '') IS NOT NULL
      UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_2', trends_top_kw_2, 'trends_kw_interest', trends_kw2_interest, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL
      UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_2', trends_top_kw_2, 'trends_kw_change', trends_kw2_change, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_2), '') IS NOT NULL
      UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_3', trends_top_kw_3, 'trends_kw_interest', trends_kw3_interest, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL
      UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_3', trends_top_kw_3, 'trends_kw_change', trends_kw3_change, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_3), '') IS NOT NULL
      UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_4', trends_top_kw_4, 'trends_kw_interest', trends_kw4_interest, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL
      UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_4', trends_top_kw_4, 'trends_kw_change', trends_kw4_change, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_4), '') IS NOT NULL
      UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_5', trends_top_kw_5, 'trends_kw_interest', trends_kw5_interest, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
      UNION ALL SELECT week_sun_to_sat, data_source, channel, max_data_date, 'KEYWORD_RANK_5', trends_top_kw_5, 'trends_kw_change', trends_kw5_change, CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE) FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly WHERE NULLIF(TRIM(trends_top_kw_5), '') IS NOT NULL
  ),
  ================================================================ */

  -- -----------------------------------------------------------------------
  -- ADOBE: conversion metrics
  -- -----------------------------------------------------------------------
  adobe_long AS (
      SELECT week_sun_to_sat, data_source, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNPIVOT (
          (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
          FOR metric_name IN (
              (adobe_upvByod_allChannels,                   adobe_upvByod_allChannels_wow,                   adobe_upvByod_allChannels_ly,                   adobe_upvByod_allChannels_wow_pct,                   adobe_upvByod_allChannels_yoy_pct)                   'adobe_upvByod_allChannels',
              (adobe_upvTotal_allChannels,                  adobe_upvTotal_allChannels_wow,                  adobe_upvTotal_allChannels_ly,                  adobe_upvTotal_allChannels_wow_pct,                  adobe_upvTotal_allChannels_yoy_pct)                  'adobe_upvTotal_allChannels',
              (adobe_upvFlowTotal_allChannels,              adobe_upvFlowTotal_allChannels_wow,              adobe_upvFlowTotal_allChannels_ly,              adobe_upvFlowTotal_allChannels_wow_pct,              adobe_upvFlowTotal_allChannels_yoy_pct)              'adobe_upvFlowTotal_allChannels',
              (adobe_pctUpvByodOfUpvFlow_allChannels,       adobe_pctUpvByodOfUpvFlow_allChannels_wow,       adobe_pctUpvByodOfUpvFlow_allChannels_ly,       adobe_pctUpvByodOfUpvFlow_allChannels_wow_pct,       adobe_pctUpvByodOfUpvFlow_allChannels_yoy_pct)       'adobe_pctUpvByodOfUpvFlow_allChannels',
              (adobe_cartStartByod_allChannels,              adobe_cartStartByod_allChannels_wow,              adobe_cartStartByod_allChannels_ly,              adobe_cartStartByod_allChannels_wow_pct,              adobe_cartStartByod_allChannels_yoy_pct)              'adobe_cartStartByod_allChannels',
              (adobe_ordersUnassistedByod_allChannels,       adobe_ordersUnassistedByod_allChannels_wow,       adobe_ordersUnassistedByod_allChannels_ly,       adobe_ordersUnassistedByod_allChannels_wow_pct,       adobe_ordersUnassistedByod_allChannels_yoy_pct)       'adobe_ordersUnassistedByod_allChannels',
              (adobe_ordersAssistedByod_allChannels,         adobe_ordersAssistedByod_allChannels_wow,         adobe_ordersAssistedByod_allChannels_ly,         adobe_ordersAssistedByod_allChannels_wow_pct,         adobe_ordersAssistedByod_allChannels_yoy_pct)         'adobe_ordersAssistedByod_allChannels',
              (adobe_ordersTotalByod_allChannels,            adobe_ordersTotalByod_allChannels_wow,            adobe_ordersTotalByod_allChannels_ly,            adobe_ordersTotalByod_allChannels_wow_pct,            adobe_ordersTotalByod_allChannels_yoy_pct)            'adobe_ordersTotalByod_allChannels',
              (adobe_ordersTotal_allChannels,                adobe_ordersTotal_allChannels_wow,                adobe_ordersTotal_allChannels_ly,                adobe_ordersTotal_allChannels_wow_pct,                adobe_ordersTotal_allChannels_yoy_pct)                'adobe_ordersTotal_allChannels',
              (adobe_pctOrdersByodOfOrdersTotal_allChannels, adobe_pctOrdersByodOfOrdersTotal_allChannels_wow, adobe_pctOrdersByodOfOrdersTotal_allChannels_ly, adobe_pctOrdersByodOfOrdersTotal_allChannels_wow_pct, adobe_pctOrdersByodOfOrdersTotal_allChannels_yoy_pct) 'adobe_pctOrdersByodOfOrdersTotal_allChannels',
              (adobe_cvrByod_allChannels,                    adobe_cvrByod_allChannels_wow,                    adobe_cvrByod_allChannels_ly,                    adobe_cvrByod_allChannels_wow_pct,                    adobe_cvrByod_allChannels_yoy_pct)                    'adobe_cvrByod_allChannels',
              (adobe_cvrSite_allChannels,                    adobe_cvrSite_allChannels_wow,                    adobe_cvrSite_allChannels_ly,                    adobe_cvrSite_allChannels_wow_pct,                    adobe_cvrSite_allChannels_yoy_pct)                    'adobe_cvrSite_allChannels',
              (adobe_cvrPostpaid_allChannels,                adobe_cvrPostpaid_allChannels_wow,                adobe_cvrPostpaid_allChannels_ly,                adobe_cvrPostpaid_allChannels_wow_pct,                adobe_cvrPostpaid_allChannels_yoy_pct)                'adobe_cvrPostpaid_allChannels',
              (adobe_cvrHsi_allChannels,                     adobe_cvrHsi_allChannels_wow,                     adobe_cvrHsi_allChannels_ly,                     adobe_cvrHsi_allChannels_wow_pct,                     adobe_cvrHsi_allChannels_yoy_pct)                     'adobe_cvrHsi_allChannels',
              (adobe_upvByod_paidSearch,                    adobe_upvByod_paidSearch_wow,                    adobe_upvByod_paidSearch_ly,                    adobe_upvByod_paidSearch_wow_pct,                    adobe_upvByod_paidSearch_yoy_pct)                    'adobe_upvByod_paidSearch',
              (adobe_pctUpvByodOfTotal_paidSearch,          adobe_pctUpvByodOfTotal_paidSearch_wow,          adobe_pctUpvByodOfTotal_paidSearch_ly,          adobe_pctUpvByodOfTotal_paidSearch_wow_pct,          adobe_pctUpvByodOfTotal_paidSearch_yoy_pct)          'adobe_pctUpvByodOfTotal_paidSearch',
              (adobe_cartStartByod_paidSearch,               adobe_cartStartByod_paidSearch_wow,               adobe_cartStartByod_paidSearch_ly,               adobe_cartStartByod_paidSearch_wow_pct,               adobe_cartStartByod_paidSearch_yoy_pct)               'adobe_cartStartByod_paidSearch',
              (adobe_ordersUnassistedByod_paidSearch,        adobe_ordersUnassistedByod_paidSearch_wow,        adobe_ordersUnassistedByod_paidSearch_ly,        adobe_ordersUnassistedByod_paidSearch_wow_pct,        adobe_ordersUnassistedByod_paidSearch_yoy_pct)        'adobe_ordersUnassistedByod_paidSearch',
              (adobe_ordersAssistedByod_paidSearch,          adobe_ordersAssistedByod_paidSearch_wow,          adobe_ordersAssistedByod_paidSearch_ly,          adobe_ordersAssistedByod_paidSearch_wow_pct,          adobe_ordersAssistedByod_paidSearch_yoy_pct)          'adobe_ordersAssistedByod_paidSearch',
              (adobe_ordersTotalByod_paidSearch,             adobe_ordersTotalByod_paidSearch_wow,             adobe_ordersTotalByod_paidSearch_ly,             adobe_ordersTotalByod_paidSearch_wow_pct,             adobe_ordersTotalByod_paidSearch_yoy_pct)             'adobe_ordersTotalByod_paidSearch',
              (adobe_upvByod_organicSearch,                 adobe_upvByod_organicSearch_wow,                 adobe_upvByod_organicSearch_ly,                 adobe_upvByod_organicSearch_wow_pct,                 adobe_upvByod_organicSearch_yoy_pct)                 'adobe_upvByod_organicSearch',
              (adobe_pctUpvByodOfTotal_organicSearch,       adobe_pctUpvByodOfTotal_organicSearch_wow,       adobe_pctUpvByodOfTotal_organicSearch_ly,       adobe_pctUpvByodOfTotal_organicSearch_wow_pct,       adobe_pctUpvByodOfTotal_organicSearch_yoy_pct)       'adobe_pctUpvByodOfTotal_organicSearch',
              (adobe_cartStartByod_organicSearch,            adobe_cartStartByod_organicSearch_wow,            adobe_cartStartByod_organicSearch_ly,            adobe_cartStartByod_organicSearch_wow_pct,            adobe_cartStartByod_organicSearch_yoy_pct)            'adobe_cartStartByod_organicSearch',
              (adobe_ordersUnassistedByod_organicSearch,     adobe_ordersUnassistedByod_organicSearch_wow,     adobe_ordersUnassistedByod_organicSearch_ly,     adobe_ordersUnassistedByod_organicSearch_wow_pct,     adobe_ordersUnassistedByod_organicSearch_yoy_pct)     'adobe_ordersUnassistedByod_organicSearch',
              (adobe_ordersAssistedByod_organicSearch,       adobe_ordersAssistedByod_organicSearch_wow,       adobe_ordersAssistedByod_organicSearch_ly,       adobe_ordersAssistedByod_organicSearch_wow_pct,       adobe_ordersAssistedByod_organicSearch_yoy_pct)       'adobe_ordersAssistedByod_organicSearch',
              (adobe_ordersTotalByod_organicSearch,          adobe_ordersTotalByod_organicSearch_wow,          adobe_ordersTotalByod_organicSearch_ly,          adobe_ordersTotalByod_organicSearch_wow_pct,          adobe_ordersTotalByod_organicSearch_yoy_pct)          'adobe_ordersTotalByod_organicSearch',
              (adobe_upvByod_direct,                        adobe_upvByod_direct_wow,                        adobe_upvByod_direct_ly,                        adobe_upvByod_direct_wow_pct,                        adobe_upvByod_direct_yoy_pct)                        'adobe_upvByod_direct',
              (adobe_pctUpvByodOfTotal_direct,              adobe_pctUpvByodOfTotal_direct_wow,              adobe_pctUpvByodOfTotal_direct_ly,              adobe_pctUpvByodOfTotal_direct_wow_pct,              adobe_pctUpvByodOfTotal_direct_yoy_pct)              'adobe_pctUpvByodOfTotal_direct',
              (adobe_cartStartByod_direct,                   adobe_cartStartByod_direct_wow,                   adobe_cartStartByod_direct_ly,                   adobe_cartStartByod_direct_wow_pct,                   adobe_cartStartByod_direct_yoy_pct)                   'adobe_cartStartByod_direct',
              (adobe_ordersUnassistedByod_direct,            adobe_ordersUnassistedByod_direct_wow,            adobe_ordersUnassistedByod_direct_ly,            adobe_ordersUnassistedByod_direct_wow_pct,            adobe_ordersUnassistedByod_direct_yoy_pct)            'adobe_ordersUnassistedByod_direct',
              (adobe_ordersAssistedByod_direct,              adobe_ordersAssistedByod_direct_wow,              adobe_ordersAssistedByod_direct_ly,              adobe_ordersAssistedByod_direct_wow_pct,              adobe_ordersAssistedByod_direct_yoy_pct)              'adobe_ordersAssistedByod_direct',
              (adobe_ordersTotalByod_direct,                 adobe_ordersTotalByod_direct_wow,                 adobe_ordersTotalByod_direct_ly,                 adobe_ordersTotalByod_direct_wow_pct,                 adobe_ordersTotalByod_direct_yoy_pct)                 'adobe_ordersTotalByod_direct',
              (adobe_upvByod_social,                        adobe_upvByod_social_wow,                        adobe_upvByod_social_ly,                        adobe_upvByod_social_wow_pct,                        adobe_upvByod_social_yoy_pct)                        'adobe_upvByod_social',
              (adobe_pctUpvByodOfTotal_social,              adobe_pctUpvByodOfTotal_social_wow,              adobe_pctUpvByodOfTotal_social_ly,              adobe_pctUpvByodOfTotal_social_wow_pct,              adobe_pctUpvByodOfTotal_social_yoy_pct)              'adobe_pctUpvByodOfTotal_social',
              (adobe_cartStartByod_social,                   adobe_cartStartByod_social_wow,                   adobe_cartStartByod_social_ly,                   adobe_cartStartByod_social_wow_pct,                   adobe_cartStartByod_social_yoy_pct)                   'adobe_cartStartByod_social',
              (adobe_ordersUnassistedByod_social,            adobe_ordersUnassistedByod_social_wow,            adobe_ordersUnassistedByod_social_ly,            adobe_ordersUnassistedByod_social_wow_pct,            adobe_ordersUnassistedByod_social_yoy_pct)            'adobe_ordersUnassistedByod_social',
              (adobe_ordersAssistedByod_social,              adobe_ordersAssistedByod_social_wow,              adobe_ordersAssistedByod_social_ly,              adobe_ordersAssistedByod_social_wow_pct,              adobe_ordersAssistedByod_social_yoy_pct)              'adobe_ordersAssistedByod_social',
              (adobe_ordersTotalByod_social,                 adobe_ordersTotalByod_social_wow,                 adobe_ordersTotalByod_social_ly,                 adobe_ordersTotalByod_social_wow_pct,                 adobe_ordersTotalByod_social_yoy_pct)                 'adobe_ordersTotalByod_social',
              (adobe_upvByod_programmatic,                  adobe_upvByod_programmatic_wow,                  adobe_upvByod_programmatic_ly,                  adobe_upvByod_programmatic_wow_pct,                  adobe_upvByod_programmatic_yoy_pct)                  'adobe_upvByod_programmatic',
              (adobe_pctUpvByodOfTotal_programmatic,        adobe_pctUpvByodOfTotal_programmatic_wow,        adobe_pctUpvByodOfTotal_programmatic_ly,        adobe_pctUpvByodOfTotal_programmatic_wow_pct,        adobe_pctUpvByodOfTotal_programmatic_yoy_pct)        'adobe_pctUpvByodOfTotal_programmatic',
              (adobe_cartStartByod_programmatic,             adobe_cartStartByod_programmatic_wow,             adobe_cartStartByod_programmatic_ly,             adobe_cartStartByod_programmatic_wow_pct,             adobe_cartStartByod_programmatic_yoy_pct)             'adobe_cartStartByod_programmatic',
              (adobe_ordersUnassistedByod_programmatic,      adobe_ordersUnassistedByod_programmatic_wow,      adobe_ordersUnassistedByod_programmatic_ly,      adobe_ordersUnassistedByod_programmatic_wow_pct,      adobe_ordersUnassistedByod_programmatic_yoy_pct)      'adobe_ordersUnassistedByod_programmatic',
              (adobe_ordersAssistedByod_programmatic,        adobe_ordersAssistedByod_programmatic_wow,        adobe_ordersAssistedByod_programmatic_ly,        adobe_ordersAssistedByod_programmatic_wow_pct,        adobe_ordersAssistedByod_programmatic_yoy_pct)        'adobe_ordersAssistedByod_programmatic',
              (adobe_ordersTotalByod_programmatic,           adobe_ordersTotalByod_programmatic_wow,           adobe_ordersTotalByod_programmatic_ly,           adobe_ordersTotalByod_programmatic_wow_pct,           adobe_ordersTotalByod_programmatic_yoy_pct)           'adobe_ordersTotalByod_programmatic',
              (adobe_upvByod_other,                         adobe_upvByod_other_wow,                         adobe_upvByod_other_ly,                         adobe_upvByod_other_wow_pct,                         adobe_upvByod_other_yoy_pct)                         'adobe_upvByod_other',
              (adobe_pctUpvByodOfTotal_other,               adobe_pctUpvByodOfTotal_other_wow,               adobe_pctUpvByodOfTotal_other_ly,               adobe_pctUpvByodOfTotal_other_wow_pct,               adobe_pctUpvByodOfTotal_other_yoy_pct)               'adobe_pctUpvByodOfTotal_other',
              (adobe_cartStartByod_other,                    adobe_cartStartByod_other_wow,                    adobe_cartStartByod_other_ly,                    adobe_cartStartByod_other_wow_pct,                    adobe_cartStartByod_other_yoy_pct)                    'adobe_cartStartByod_other',
              (adobe_ordersUnassistedByod_other,             adobe_ordersUnassistedByod_other_wow,             adobe_ordersUnassistedByod_other_ly,             adobe_ordersUnassistedByod_other_wow_pct,             adobe_ordersUnassistedByod_other_yoy_pct)             'adobe_ordersUnassistedByod_other',
              (adobe_ordersAssistedByod_other,               adobe_ordersAssistedByod_other_wow,               adobe_ordersAssistedByod_other_ly,               adobe_ordersAssistedByod_other_wow_pct,               adobe_ordersAssistedByod_other_yoy_pct)               'adobe_ordersAssistedByod_other',
              (adobe_ordersTotalByod_other,                  adobe_ordersTotalByod_other_wow,                  adobe_ordersTotalByod_other_ly,                  adobe_ordersTotalByod_other_wow_pct,                  adobe_ordersTotalByod_other_yoy_pct)                  'adobe_ordersTotalByod_other'
          )
      )
  ),
  -- -----------------------------------------------------------------------
  -- ADOBE BYOD ENTRY PAGES: 9 metrics x 7 channels — 63 rows/week
  -- -----------------------------------------------------------------------
  entryPages_long AS (
      SELECT week_sun_to_sat, data_source, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNPIVOT (
          (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
          FOR metric_name IN (
              (adobe_byodUpvVisitors_allChannels, adobe_byodUpvVisitors_allChannels_wow, adobe_byodUpvVisitors_allChannels_ly, adobe_byodUpvVisitors_allChannels_wow_pct, adobe_byodUpvVisitors_allChannels_yoy_pct) 'adobe_byodUpvVisitors_allChannels',
              (adobe_byodEntryByodPageVisitors_allChannels, adobe_byodEntryByodPageVisitors_allChannels_wow, adobe_byodEntryByodPageVisitors_allChannels_ly, adobe_byodEntryByodPageVisitors_allChannels_wow_pct, adobe_byodEntryByodPageVisitors_allChannels_yoy_pct) 'adobe_byodEntryByodPageVisitors_allChannels',
              (adobe_byodEntryHomePageVisitors_allChannels, adobe_byodEntryHomePageVisitors_allChannels_wow, adobe_byodEntryHomePageVisitors_allChannels_ly, adobe_byodEntryHomePageVisitors_allChannels_wow_pct, adobe_byodEntryHomePageVisitors_allChannels_yoy_pct) 'adobe_byodEntryHomePageVisitors_allChannels',
              (adobe_byodEntryDevicePageVisitors_allChannels, adobe_byodEntryDevicePageVisitors_allChannels_wow, adobe_byodEntryDevicePageVisitors_allChannels_ly, adobe_byodEntryDevicePageVisitors_allChannels_wow_pct, adobe_byodEntryDevicePageVisitors_allChannels_yoy_pct) 'adobe_byodEntryDevicePageVisitors_allChannels',
              (adobe_byodEntryPlansPageVisitors_allChannels, adobe_byodEntryPlansPageVisitors_allChannels_wow, adobe_byodEntryPlansPageVisitors_allChannels_ly, adobe_byodEntryPlansPageVisitors_allChannels_wow_pct, adobe_byodEntryPlansPageVisitors_allChannels_yoy_pct) 'adobe_byodEntryPlansPageVisitors_allChannels',
              (adobe_byodEntryOtherPageVisitors_allChannels, adobe_byodEntryOtherPageVisitors_allChannels_wow, adobe_byodEntryOtherPageVisitors_allChannels_ly, adobe_byodEntryOtherPageVisitors_allChannels_wow_pct, adobe_byodEntryOtherPageVisitors_allChannels_yoy_pct) 'adobe_byodEntryOtherPageVisitors_allChannels',
              (adobe_byodEntryGetStartedLandingPageVisitors_allChannels, adobe_byodEntryGetStartedLandingPageVisitors_allChannels_wow, adobe_byodEntryGetStartedLandingPageVisitors_allChannels_ly, adobe_byodEntryGetStartedLandingPageVisitors_allChannels_wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_allChannels_yoy_pct) 'adobe_byodEntryGetStartedLandingPageVisitors_allChannels',
              (adobe_byodEntryByodLandingPageVisitors_allChannels, adobe_byodEntryByodLandingPageVisitors_allChannels_wow, adobe_byodEntryByodLandingPageVisitors_allChannels_ly, adobe_byodEntryByodLandingPageVisitors_allChannels_wow_pct, adobe_byodEntryByodLandingPageVisitors_allChannels_yoy_pct) 'adobe_byodEntryByodLandingPageVisitors_allChannels',
              (adobe_byodEntryOffersSwitchVisitors_allChannels, adobe_byodEntryOffersSwitchVisitors_allChannels_wow, adobe_byodEntryOffersSwitchVisitors_allChannels_ly, adobe_byodEntryOffersSwitchVisitors_allChannels_wow_pct, adobe_byodEntryOffersSwitchVisitors_allChannels_yoy_pct) 'adobe_byodEntryOffersSwitchVisitors_allChannels',
              (adobe_byodUpvVisitors_paidSearch, adobe_byodUpvVisitors_paidSearch_wow, adobe_byodUpvVisitors_paidSearch_ly, adobe_byodUpvVisitors_paidSearch_wow_pct, adobe_byodUpvVisitors_paidSearch_yoy_pct) 'adobe_byodUpvVisitors_paidSearch',
              (adobe_byodEntryByodPageVisitors_paidSearch, adobe_byodEntryByodPageVisitors_paidSearch_wow, adobe_byodEntryByodPageVisitors_paidSearch_ly, adobe_byodEntryByodPageVisitors_paidSearch_wow_pct, adobe_byodEntryByodPageVisitors_paidSearch_yoy_pct) 'adobe_byodEntryByodPageVisitors_paidSearch',
              (adobe_byodEntryHomePageVisitors_paidSearch, adobe_byodEntryHomePageVisitors_paidSearch_wow, adobe_byodEntryHomePageVisitors_paidSearch_ly, adobe_byodEntryHomePageVisitors_paidSearch_wow_pct, adobe_byodEntryHomePageVisitors_paidSearch_yoy_pct) 'adobe_byodEntryHomePageVisitors_paidSearch',
              (adobe_byodEntryDevicePageVisitors_paidSearch, adobe_byodEntryDevicePageVisitors_paidSearch_wow, adobe_byodEntryDevicePageVisitors_paidSearch_ly, adobe_byodEntryDevicePageVisitors_paidSearch_wow_pct, adobe_byodEntryDevicePageVisitors_paidSearch_yoy_pct) 'adobe_byodEntryDevicePageVisitors_paidSearch',
              (adobe_byodEntryPlansPageVisitors_paidSearch, adobe_byodEntryPlansPageVisitors_paidSearch_wow, adobe_byodEntryPlansPageVisitors_paidSearch_ly, adobe_byodEntryPlansPageVisitors_paidSearch_wow_pct, adobe_byodEntryPlansPageVisitors_paidSearch_yoy_pct) 'adobe_byodEntryPlansPageVisitors_paidSearch',
              (adobe_byodEntryOtherPageVisitors_paidSearch, adobe_byodEntryOtherPageVisitors_paidSearch_wow, adobe_byodEntryOtherPageVisitors_paidSearch_ly, adobe_byodEntryOtherPageVisitors_paidSearch_wow_pct, adobe_byodEntryOtherPageVisitors_paidSearch_yoy_pct) 'adobe_byodEntryOtherPageVisitors_paidSearch',
              (adobe_byodEntryGetStartedLandingPageVisitors_paidSearch, adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_wow, adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_ly, adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_yoy_pct) 'adobe_byodEntryGetStartedLandingPageVisitors_paidSearch',
              (adobe_byodEntryByodLandingPageVisitors_paidSearch, adobe_byodEntryByodLandingPageVisitors_paidSearch_wow, adobe_byodEntryByodLandingPageVisitors_paidSearch_ly, adobe_byodEntryByodLandingPageVisitors_paidSearch_wow_pct, adobe_byodEntryByodLandingPageVisitors_paidSearch_yoy_pct) 'adobe_byodEntryByodLandingPageVisitors_paidSearch',
              (adobe_byodEntryOffersSwitchVisitors_paidSearch, adobe_byodEntryOffersSwitchVisitors_paidSearch_wow, adobe_byodEntryOffersSwitchVisitors_paidSearch_ly, adobe_byodEntryOffersSwitchVisitors_paidSearch_wow_pct, adobe_byodEntryOffersSwitchVisitors_paidSearch_yoy_pct) 'adobe_byodEntryOffersSwitchVisitors_paidSearch',
              (adobe_byodUpvVisitors_organicSearch, adobe_byodUpvVisitors_organicSearch_wow, adobe_byodUpvVisitors_organicSearch_ly, adobe_byodUpvVisitors_organicSearch_wow_pct, adobe_byodUpvVisitors_organicSearch_yoy_pct) 'adobe_byodUpvVisitors_organicSearch',
              (adobe_byodEntryByodPageVisitors_organicSearch, adobe_byodEntryByodPageVisitors_organicSearch_wow, adobe_byodEntryByodPageVisitors_organicSearch_ly, adobe_byodEntryByodPageVisitors_organicSearch_wow_pct, adobe_byodEntryByodPageVisitors_organicSearch_yoy_pct) 'adobe_byodEntryByodPageVisitors_organicSearch',
              (adobe_byodEntryHomePageVisitors_organicSearch, adobe_byodEntryHomePageVisitors_organicSearch_wow, adobe_byodEntryHomePageVisitors_organicSearch_ly, adobe_byodEntryHomePageVisitors_organicSearch_wow_pct, adobe_byodEntryHomePageVisitors_organicSearch_yoy_pct) 'adobe_byodEntryHomePageVisitors_organicSearch',
              (adobe_byodEntryDevicePageVisitors_organicSearch, adobe_byodEntryDevicePageVisitors_organicSearch_wow, adobe_byodEntryDevicePageVisitors_organicSearch_ly, adobe_byodEntryDevicePageVisitors_organicSearch_wow_pct, adobe_byodEntryDevicePageVisitors_organicSearch_yoy_pct) 'adobe_byodEntryDevicePageVisitors_organicSearch',
              (adobe_byodEntryPlansPageVisitors_organicSearch, adobe_byodEntryPlansPageVisitors_organicSearch_wow, adobe_byodEntryPlansPageVisitors_organicSearch_ly, adobe_byodEntryPlansPageVisitors_organicSearch_wow_pct, adobe_byodEntryPlansPageVisitors_organicSearch_yoy_pct) 'adobe_byodEntryPlansPageVisitors_organicSearch',
              (adobe_byodEntryOtherPageVisitors_organicSearch, adobe_byodEntryOtherPageVisitors_organicSearch_wow, adobe_byodEntryOtherPageVisitors_organicSearch_ly, adobe_byodEntryOtherPageVisitors_organicSearch_wow_pct, adobe_byodEntryOtherPageVisitors_organicSearch_yoy_pct) 'adobe_byodEntryOtherPageVisitors_organicSearch',
              (adobe_byodEntryGetStartedLandingPageVisitors_organicSearch, adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_wow, adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_ly, adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_yoy_pct) 'adobe_byodEntryGetStartedLandingPageVisitors_organicSearch',
              (adobe_byodEntryByodLandingPageVisitors_organicSearch, adobe_byodEntryByodLandingPageVisitors_organicSearch_wow, adobe_byodEntryByodLandingPageVisitors_organicSearch_ly, adobe_byodEntryByodLandingPageVisitors_organicSearch_wow_pct, adobe_byodEntryByodLandingPageVisitors_organicSearch_yoy_pct) 'adobe_byodEntryByodLandingPageVisitors_organicSearch',
              (adobe_byodEntryOffersSwitchVisitors_organicSearch, adobe_byodEntryOffersSwitchVisitors_organicSearch_wow, adobe_byodEntryOffersSwitchVisitors_organicSearch_ly, adobe_byodEntryOffersSwitchVisitors_organicSearch_wow_pct, adobe_byodEntryOffersSwitchVisitors_organicSearch_yoy_pct) 'adobe_byodEntryOffersSwitchVisitors_organicSearch',
              (adobe_byodUpvVisitors_direct, adobe_byodUpvVisitors_direct_wow, adobe_byodUpvVisitors_direct_ly, adobe_byodUpvVisitors_direct_wow_pct, adobe_byodUpvVisitors_direct_yoy_pct) 'adobe_byodUpvVisitors_direct',
              (adobe_byodEntryByodPageVisitors_direct, adobe_byodEntryByodPageVisitors_direct_wow, adobe_byodEntryByodPageVisitors_direct_ly, adobe_byodEntryByodPageVisitors_direct_wow_pct, adobe_byodEntryByodPageVisitors_direct_yoy_pct) 'adobe_byodEntryByodPageVisitors_direct',
              (adobe_byodEntryHomePageVisitors_direct, adobe_byodEntryHomePageVisitors_direct_wow, adobe_byodEntryHomePageVisitors_direct_ly, adobe_byodEntryHomePageVisitors_direct_wow_pct, adobe_byodEntryHomePageVisitors_direct_yoy_pct) 'adobe_byodEntryHomePageVisitors_direct',
              (adobe_byodEntryDevicePageVisitors_direct, adobe_byodEntryDevicePageVisitors_direct_wow, adobe_byodEntryDevicePageVisitors_direct_ly, adobe_byodEntryDevicePageVisitors_direct_wow_pct, adobe_byodEntryDevicePageVisitors_direct_yoy_pct) 'adobe_byodEntryDevicePageVisitors_direct',
              (adobe_byodEntryPlansPageVisitors_direct, adobe_byodEntryPlansPageVisitors_direct_wow, adobe_byodEntryPlansPageVisitors_direct_ly, adobe_byodEntryPlansPageVisitors_direct_wow_pct, adobe_byodEntryPlansPageVisitors_direct_yoy_pct) 'adobe_byodEntryPlansPageVisitors_direct',
              (adobe_byodEntryOtherPageVisitors_direct, adobe_byodEntryOtherPageVisitors_direct_wow, adobe_byodEntryOtherPageVisitors_direct_ly, adobe_byodEntryOtherPageVisitors_direct_wow_pct, adobe_byodEntryOtherPageVisitors_direct_yoy_pct) 'adobe_byodEntryOtherPageVisitors_direct',
              (adobe_byodEntryGetStartedLandingPageVisitors_direct, adobe_byodEntryGetStartedLandingPageVisitors_direct_wow, adobe_byodEntryGetStartedLandingPageVisitors_direct_ly, adobe_byodEntryGetStartedLandingPageVisitors_direct_wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_direct_yoy_pct) 'adobe_byodEntryGetStartedLandingPageVisitors_direct',
              (adobe_byodEntryByodLandingPageVisitors_direct, adobe_byodEntryByodLandingPageVisitors_direct_wow, adobe_byodEntryByodLandingPageVisitors_direct_ly, adobe_byodEntryByodLandingPageVisitors_direct_wow_pct, adobe_byodEntryByodLandingPageVisitors_direct_yoy_pct) 'adobe_byodEntryByodLandingPageVisitors_direct',
              (adobe_byodEntryOffersSwitchVisitors_direct, adobe_byodEntryOffersSwitchVisitors_direct_wow, adobe_byodEntryOffersSwitchVisitors_direct_ly, adobe_byodEntryOffersSwitchVisitors_direct_wow_pct, adobe_byodEntryOffersSwitchVisitors_direct_yoy_pct) 'adobe_byodEntryOffersSwitchVisitors_direct',
              (adobe_byodUpvVisitors_social, adobe_byodUpvVisitors_social_wow, adobe_byodUpvVisitors_social_ly, adobe_byodUpvVisitors_social_wow_pct, adobe_byodUpvVisitors_social_yoy_pct) 'adobe_byodUpvVisitors_social',
              (adobe_byodEntryByodPageVisitors_social, adobe_byodEntryByodPageVisitors_social_wow, adobe_byodEntryByodPageVisitors_social_ly, adobe_byodEntryByodPageVisitors_social_wow_pct, adobe_byodEntryByodPageVisitors_social_yoy_pct) 'adobe_byodEntryByodPageVisitors_social',
              (adobe_byodEntryHomePageVisitors_social, adobe_byodEntryHomePageVisitors_social_wow, adobe_byodEntryHomePageVisitors_social_ly, adobe_byodEntryHomePageVisitors_social_wow_pct, adobe_byodEntryHomePageVisitors_social_yoy_pct) 'adobe_byodEntryHomePageVisitors_social',
              (adobe_byodEntryDevicePageVisitors_social, adobe_byodEntryDevicePageVisitors_social_wow, adobe_byodEntryDevicePageVisitors_social_ly, adobe_byodEntryDevicePageVisitors_social_wow_pct, adobe_byodEntryDevicePageVisitors_social_yoy_pct) 'adobe_byodEntryDevicePageVisitors_social',
              (adobe_byodEntryPlansPageVisitors_social, adobe_byodEntryPlansPageVisitors_social_wow, adobe_byodEntryPlansPageVisitors_social_ly, adobe_byodEntryPlansPageVisitors_social_wow_pct, adobe_byodEntryPlansPageVisitors_social_yoy_pct) 'adobe_byodEntryPlansPageVisitors_social',
              (adobe_byodEntryOtherPageVisitors_social, adobe_byodEntryOtherPageVisitors_social_wow, adobe_byodEntryOtherPageVisitors_social_ly, adobe_byodEntryOtherPageVisitors_social_wow_pct, adobe_byodEntryOtherPageVisitors_social_yoy_pct) 'adobe_byodEntryOtherPageVisitors_social',
              (adobe_byodEntryGetStartedLandingPageVisitors_social, adobe_byodEntryGetStartedLandingPageVisitors_social_wow, adobe_byodEntryGetStartedLandingPageVisitors_social_ly, adobe_byodEntryGetStartedLandingPageVisitors_social_wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_social_yoy_pct) 'adobe_byodEntryGetStartedLandingPageVisitors_social',
              (adobe_byodEntryByodLandingPageVisitors_social, adobe_byodEntryByodLandingPageVisitors_social_wow, adobe_byodEntryByodLandingPageVisitors_social_ly, adobe_byodEntryByodLandingPageVisitors_social_wow_pct, adobe_byodEntryByodLandingPageVisitors_social_yoy_pct) 'adobe_byodEntryByodLandingPageVisitors_social',
              (adobe_byodEntryOffersSwitchVisitors_social, adobe_byodEntryOffersSwitchVisitors_social_wow, adobe_byodEntryOffersSwitchVisitors_social_ly, adobe_byodEntryOffersSwitchVisitors_social_wow_pct, adobe_byodEntryOffersSwitchVisitors_social_yoy_pct) 'adobe_byodEntryOffersSwitchVisitors_social',
              (adobe_byodUpvVisitors_programmatic, adobe_byodUpvVisitors_programmatic_wow, adobe_byodUpvVisitors_programmatic_ly, adobe_byodUpvVisitors_programmatic_wow_pct, adobe_byodUpvVisitors_programmatic_yoy_pct) 'adobe_byodUpvVisitors_programmatic',
              (adobe_byodEntryByodPageVisitors_programmatic, adobe_byodEntryByodPageVisitors_programmatic_wow, adobe_byodEntryByodPageVisitors_programmatic_ly, adobe_byodEntryByodPageVisitors_programmatic_wow_pct, adobe_byodEntryByodPageVisitors_programmatic_yoy_pct) 'adobe_byodEntryByodPageVisitors_programmatic',
              (adobe_byodEntryHomePageVisitors_programmatic, adobe_byodEntryHomePageVisitors_programmatic_wow, adobe_byodEntryHomePageVisitors_programmatic_ly, adobe_byodEntryHomePageVisitors_programmatic_wow_pct, adobe_byodEntryHomePageVisitors_programmatic_yoy_pct) 'adobe_byodEntryHomePageVisitors_programmatic',
              (adobe_byodEntryDevicePageVisitors_programmatic, adobe_byodEntryDevicePageVisitors_programmatic_wow, adobe_byodEntryDevicePageVisitors_programmatic_ly, adobe_byodEntryDevicePageVisitors_programmatic_wow_pct, adobe_byodEntryDevicePageVisitors_programmatic_yoy_pct) 'adobe_byodEntryDevicePageVisitors_programmatic',
              (adobe_byodEntryPlansPageVisitors_programmatic, adobe_byodEntryPlansPageVisitors_programmatic_wow, adobe_byodEntryPlansPageVisitors_programmatic_ly, adobe_byodEntryPlansPageVisitors_programmatic_wow_pct, adobe_byodEntryPlansPageVisitors_programmatic_yoy_pct) 'adobe_byodEntryPlansPageVisitors_programmatic',
              (adobe_byodEntryOtherPageVisitors_programmatic, adobe_byodEntryOtherPageVisitors_programmatic_wow, adobe_byodEntryOtherPageVisitors_programmatic_ly, adobe_byodEntryOtherPageVisitors_programmatic_wow_pct, adobe_byodEntryOtherPageVisitors_programmatic_yoy_pct) 'adobe_byodEntryOtherPageVisitors_programmatic',
              (adobe_byodEntryGetStartedLandingPageVisitors_programmatic, adobe_byodEntryGetStartedLandingPageVisitors_programmatic_wow, adobe_byodEntryGetStartedLandingPageVisitors_programmatic_ly, adobe_byodEntryGetStartedLandingPageVisitors_programmatic_wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_programmatic_yoy_pct) 'adobe_byodEntryGetStartedLandingPageVisitors_programmatic',
              (adobe_byodEntryByodLandingPageVisitors_programmatic, adobe_byodEntryByodLandingPageVisitors_programmatic_wow, adobe_byodEntryByodLandingPageVisitors_programmatic_ly, adobe_byodEntryByodLandingPageVisitors_programmatic_wow_pct, adobe_byodEntryByodLandingPageVisitors_programmatic_yoy_pct) 'adobe_byodEntryByodLandingPageVisitors_programmatic',
              (adobe_byodEntryOffersSwitchVisitors_programmatic, adobe_byodEntryOffersSwitchVisitors_programmatic_wow, adobe_byodEntryOffersSwitchVisitors_programmatic_ly, adobe_byodEntryOffersSwitchVisitors_programmatic_wow_pct, adobe_byodEntryOffersSwitchVisitors_programmatic_yoy_pct) 'adobe_byodEntryOffersSwitchVisitors_programmatic',
              (adobe_byodUpvVisitors_other, adobe_byodUpvVisitors_other_wow, adobe_byodUpvVisitors_other_ly, adobe_byodUpvVisitors_other_wow_pct, adobe_byodUpvVisitors_other_yoy_pct) 'adobe_byodUpvVisitors_other',
              (adobe_byodEntryByodPageVisitors_other, adobe_byodEntryByodPageVisitors_other_wow, adobe_byodEntryByodPageVisitors_other_ly, adobe_byodEntryByodPageVisitors_other_wow_pct, adobe_byodEntryByodPageVisitors_other_yoy_pct) 'adobe_byodEntryByodPageVisitors_other',
              (adobe_byodEntryHomePageVisitors_other, adobe_byodEntryHomePageVisitors_other_wow, adobe_byodEntryHomePageVisitors_other_ly, adobe_byodEntryHomePageVisitors_other_wow_pct, adobe_byodEntryHomePageVisitors_other_yoy_pct) 'adobe_byodEntryHomePageVisitors_other',
              (adobe_byodEntryDevicePageVisitors_other, adobe_byodEntryDevicePageVisitors_other_wow, adobe_byodEntryDevicePageVisitors_other_ly, adobe_byodEntryDevicePageVisitors_other_wow_pct, adobe_byodEntryDevicePageVisitors_other_yoy_pct) 'adobe_byodEntryDevicePageVisitors_other',
              (adobe_byodEntryPlansPageVisitors_other, adobe_byodEntryPlansPageVisitors_other_wow, adobe_byodEntryPlansPageVisitors_other_ly, adobe_byodEntryPlansPageVisitors_other_wow_pct, adobe_byodEntryPlansPageVisitors_other_yoy_pct) 'adobe_byodEntryPlansPageVisitors_other',
              (adobe_byodEntryOtherPageVisitors_other, adobe_byodEntryOtherPageVisitors_other_wow, adobe_byodEntryOtherPageVisitors_other_ly, adobe_byodEntryOtherPageVisitors_other_wow_pct, adobe_byodEntryOtherPageVisitors_other_yoy_pct) 'adobe_byodEntryOtherPageVisitors_other',
              (adobe_byodEntryGetStartedLandingPageVisitors_other, adobe_byodEntryGetStartedLandingPageVisitors_other_wow, adobe_byodEntryGetStartedLandingPageVisitors_other_ly, adobe_byodEntryGetStartedLandingPageVisitors_other_wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_other_yoy_pct) 'adobe_byodEntryGetStartedLandingPageVisitors_other',
              (adobe_byodEntryByodLandingPageVisitors_other, adobe_byodEntryByodLandingPageVisitors_other_wow, adobe_byodEntryByodLandingPageVisitors_other_ly, adobe_byodEntryByodLandingPageVisitors_other_wow_pct, adobe_byodEntryByodLandingPageVisitors_other_yoy_pct) 'adobe_byodEntryByodLandingPageVisitors_other',
              (adobe_byodEntryOffersSwitchVisitors_other, adobe_byodEntryOffersSwitchVisitors_other_wow, adobe_byodEntryOffersSwitchVisitors_other_ly, adobe_byodEntryOffersSwitchVisitors_other_wow_pct, adobe_byodEntryOffersSwitchVisitors_other_yoy_pct) 'adobe_byodEntryOffersSwitchVisitors_other'
          )
      )
  ),

  -- -----------------------------------------------------------------------
  -- ADOBE BYOD OUTCOMES: 6 metrics x 7 channels — 42 rows/week
  -- -----------------------------------------------------------------------
  outcomes_long AS (
      SELECT week_sun_to_sat, data_source, max_data_date, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNPIVOT (
          (metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct)
          FOR metric_name IN (
              (adobe_byodVrChatVisitors_allChannels,       adobe_byodVrChatVisitors_allChannels_wow,       adobe_byodVrChatVisitors_allChannels_ly,       adobe_byodVrChatVisitors_allChannels_wow_pct,       adobe_byodVrChatVisitors_allChannels_yoy_pct)       'adobe_byodVrChatVisitors_allChannels',
              (adobe_byodCallVisitors_allChannels,          adobe_byodCallVisitors_allChannels_wow,          adobe_byodCallVisitors_allChannels_ly,          adobe_byodCallVisitors_allChannels_wow_pct,          adobe_byodCallVisitors_allChannels_yoy_pct)          'adobe_byodCallVisitors_allChannels',
              (adobe_byodStoreLocatorVisitors_allChannels,  adobe_byodStoreLocatorVisitors_allChannels_wow,  adobe_byodStoreLocatorVisitors_allChannels_ly,  adobe_byodStoreLocatorVisitors_allChannels_wow_pct,  adobe_byodStoreLocatorVisitors_allChannels_yoy_pct)  'adobe_byodStoreLocatorVisitors_allChannels',
              (adobe_byodInternalTmoVisitors_allChannels,   adobe_byodInternalTmoVisitors_allChannels_wow,   adobe_byodInternalTmoVisitors_allChannels_ly,   adobe_byodInternalTmoVisitors_allChannels_wow_pct,   adobe_byodInternalTmoVisitors_allChannels_yoy_pct)   'adobe_byodInternalTmoVisitors_allChannels',
              (adobe_byodBouncersVisitors_allChannels,      adobe_byodBouncersVisitors_allChannels_wow,      adobe_byodBouncersVisitors_allChannels_ly,      adobe_byodBouncersVisitors_allChannels_wow_pct,      adobe_byodBouncersVisitors_allChannels_yoy_pct)      'adobe_byodBouncersVisitors_allChannels',
              (adobe_byodOrders_allChannels,                adobe_byodOrders_allChannels_wow,                adobe_byodOrders_allChannels_ly,                adobe_byodOrders_allChannels_wow_pct,                adobe_byodOrders_allChannels_yoy_pct)                'adobe_byodOrders_allChannels',
              (adobe_byodVrChatVisitors_paidSearch,         adobe_byodVrChatVisitors_paidSearch_wow,         adobe_byodVrChatVisitors_paidSearch_ly,         adobe_byodVrChatVisitors_paidSearch_wow_pct,         adobe_byodVrChatVisitors_paidSearch_yoy_pct)         'adobe_byodVrChatVisitors_paidSearch',
              (adobe_byodCallVisitors_paidSearch,           adobe_byodCallVisitors_paidSearch_wow,           adobe_byodCallVisitors_paidSearch_ly,           adobe_byodCallVisitors_paidSearch_wow_pct,           adobe_byodCallVisitors_paidSearch_yoy_pct)           'adobe_byodCallVisitors_paidSearch',
              (adobe_byodStoreLocatorVisitors_paidSearch,   adobe_byodStoreLocatorVisitors_paidSearch_wow,   adobe_byodStoreLocatorVisitors_paidSearch_ly,   adobe_byodStoreLocatorVisitors_paidSearch_wow_pct,   adobe_byodStoreLocatorVisitors_paidSearch_yoy_pct)   'adobe_byodStoreLocatorVisitors_paidSearch',
              (adobe_byodInternalTmoVisitors_paidSearch,    adobe_byodInternalTmoVisitors_paidSearch_wow,    adobe_byodInternalTmoVisitors_paidSearch_ly,    adobe_byodInternalTmoVisitors_paidSearch_wow_pct,    adobe_byodInternalTmoVisitors_paidSearch_yoy_pct)    'adobe_byodInternalTmoVisitors_paidSearch',
              (adobe_byodBouncersVisitors_paidSearch,       adobe_byodBouncersVisitors_paidSearch_wow,       adobe_byodBouncersVisitors_paidSearch_ly,       adobe_byodBouncersVisitors_paidSearch_wow_pct,       adobe_byodBouncersVisitors_paidSearch_yoy_pct)       'adobe_byodBouncersVisitors_paidSearch',
              (adobe_byodOrders_paidSearch,                 adobe_byodOrders_paidSearch_wow,                 adobe_byodOrders_paidSearch_ly,                 adobe_byodOrders_paidSearch_wow_pct,                 adobe_byodOrders_paidSearch_yoy_pct)                 'adobe_byodOrders_paidSearch',
              (adobe_byodVrChatVisitors_organicSearch,      adobe_byodVrChatVisitors_organicSearch_wow,      adobe_byodVrChatVisitors_organicSearch_ly,      adobe_byodVrChatVisitors_organicSearch_wow_pct,      adobe_byodVrChatVisitors_organicSearch_yoy_pct)      'adobe_byodVrChatVisitors_organicSearch',
              (adobe_byodCallVisitors_organicSearch,        adobe_byodCallVisitors_organicSearch_wow,        adobe_byodCallVisitors_organicSearch_ly,        adobe_byodCallVisitors_organicSearch_wow_pct,        adobe_byodCallVisitors_organicSearch_yoy_pct)        'adobe_byodCallVisitors_organicSearch',
              (adobe_byodStoreLocatorVisitors_organicSearch,adobe_byodStoreLocatorVisitors_organicSearch_wow,adobe_byodStoreLocatorVisitors_organicSearch_ly,adobe_byodStoreLocatorVisitors_organicSearch_wow_pct,adobe_byodStoreLocatorVisitors_organicSearch_yoy_pct) 'adobe_byodStoreLocatorVisitors_organicSearch',
              (adobe_byodInternalTmoVisitors_organicSearch, adobe_byodInternalTmoVisitors_organicSearch_wow, adobe_byodInternalTmoVisitors_organicSearch_ly, adobe_byodInternalTmoVisitors_organicSearch_wow_pct, adobe_byodInternalTmoVisitors_organicSearch_yoy_pct) 'adobe_byodInternalTmoVisitors_organicSearch',
              (adobe_byodBouncersVisitors_organicSearch,    adobe_byodBouncersVisitors_organicSearch_wow,    adobe_byodBouncersVisitors_organicSearch_ly,    adobe_byodBouncersVisitors_organicSearch_wow_pct,    adobe_byodBouncersVisitors_organicSearch_yoy_pct)    'adobe_byodBouncersVisitors_organicSearch',
              (adobe_byodOrders_organicSearch,              adobe_byodOrders_organicSearch_wow,              adobe_byodOrders_organicSearch_ly,              adobe_byodOrders_organicSearch_wow_pct,              adobe_byodOrders_organicSearch_yoy_pct)              'adobe_byodOrders_organicSearch',
              (adobe_byodVrChatVisitors_direct,             adobe_byodVrChatVisitors_direct_wow,             adobe_byodVrChatVisitors_direct_ly,             adobe_byodVrChatVisitors_direct_wow_pct,             adobe_byodVrChatVisitors_direct_yoy_pct)             'adobe_byodVrChatVisitors_direct',
              (adobe_byodCallVisitors_direct,               adobe_byodCallVisitors_direct_wow,               adobe_byodCallVisitors_direct_ly,               adobe_byodCallVisitors_direct_wow_pct,               adobe_byodCallVisitors_direct_yoy_pct)               'adobe_byodCallVisitors_direct',
              (adobe_byodStoreLocatorVisitors_direct,       adobe_byodStoreLocatorVisitors_direct_wow,       adobe_byodStoreLocatorVisitors_direct_ly,       adobe_byodStoreLocatorVisitors_direct_wow_pct,       adobe_byodStoreLocatorVisitors_direct_yoy_pct)       'adobe_byodStoreLocatorVisitors_direct',
              (adobe_byodInternalTmoVisitors_direct,        adobe_byodInternalTmoVisitors_direct_wow,        adobe_byodInternalTmoVisitors_direct_ly,        adobe_byodInternalTmoVisitors_direct_wow_pct,        adobe_byodInternalTmoVisitors_direct_yoy_pct)        'adobe_byodInternalTmoVisitors_direct',
              (adobe_byodBouncersVisitors_direct,           adobe_byodBouncersVisitors_direct_wow,           adobe_byodBouncersVisitors_direct_ly,           adobe_byodBouncersVisitors_direct_wow_pct,           adobe_byodBouncersVisitors_direct_yoy_pct)           'adobe_byodBouncersVisitors_direct',
              (adobe_byodOrders_direct,                     adobe_byodOrders_direct_wow,                     adobe_byodOrders_direct_ly,                     adobe_byodOrders_direct_wow_pct,                     adobe_byodOrders_direct_yoy_pct)                     'adobe_byodOrders_direct',
              (adobe_byodVrChatVisitors_social,             adobe_byodVrChatVisitors_social_wow,             adobe_byodVrChatVisitors_social_ly,             adobe_byodVrChatVisitors_social_wow_pct,             adobe_byodVrChatVisitors_social_yoy_pct)             'adobe_byodVrChatVisitors_social',
              (adobe_byodCallVisitors_social,               adobe_byodCallVisitors_social_wow,               adobe_byodCallVisitors_social_ly,               adobe_byodCallVisitors_social_wow_pct,               adobe_byodCallVisitors_social_yoy_pct)               'adobe_byodCallVisitors_social',
              (adobe_byodStoreLocatorVisitors_social,       adobe_byodStoreLocatorVisitors_social_wow,       adobe_byodStoreLocatorVisitors_social_ly,       adobe_byodStoreLocatorVisitors_social_wow_pct,       adobe_byodStoreLocatorVisitors_social_yoy_pct)       'adobe_byodStoreLocatorVisitors_social',
              (adobe_byodInternalTmoVisitors_social,        adobe_byodInternalTmoVisitors_social_wow,        adobe_byodInternalTmoVisitors_social_ly,        adobe_byodInternalTmoVisitors_social_wow_pct,        adobe_byodInternalTmoVisitors_social_yoy_pct)        'adobe_byodInternalTmoVisitors_social',
              (adobe_byodBouncersVisitors_social,           adobe_byodBouncersVisitors_social_wow,           adobe_byodBouncersVisitors_social_ly,           adobe_byodBouncersVisitors_social_wow_pct,           adobe_byodBouncersVisitors_social_yoy_pct)           'adobe_byodBouncersVisitors_social',
              (adobe_byodOrders_social,                     adobe_byodOrders_social_wow,                     adobe_byodOrders_social_ly,                     adobe_byodOrders_social_wow_pct,                     adobe_byodOrders_social_yoy_pct)                     'adobe_byodOrders_social',
              (adobe_byodVrChatVisitors_programmatic,       adobe_byodVrChatVisitors_programmatic_wow,       adobe_byodVrChatVisitors_programmatic_ly,       adobe_byodVrChatVisitors_programmatic_wow_pct,       adobe_byodVrChatVisitors_programmatic_yoy_pct)       'adobe_byodVrChatVisitors_programmatic',
              (adobe_byodCallVisitors_programmatic,         adobe_byodCallVisitors_programmatic_wow,         adobe_byodCallVisitors_programmatic_ly,         adobe_byodCallVisitors_programmatic_wow_pct,         adobe_byodCallVisitors_programmatic_yoy_pct)         'adobe_byodCallVisitors_programmatic',
              (adobe_byodStoreLocatorVisitors_programmatic, adobe_byodStoreLocatorVisitors_programmatic_wow, adobe_byodStoreLocatorVisitors_programmatic_ly, adobe_byodStoreLocatorVisitors_programmatic_wow_pct, adobe_byodStoreLocatorVisitors_programmatic_yoy_pct) 'adobe_byodStoreLocatorVisitors_programmatic',
              (adobe_byodInternalTmoVisitors_programmatic,  adobe_byodInternalTmoVisitors_programmatic_wow,  adobe_byodInternalTmoVisitors_programmatic_ly,  adobe_byodInternalTmoVisitors_programmatic_wow_pct,  adobe_byodInternalTmoVisitors_programmatic_yoy_pct)  'adobe_byodInternalTmoVisitors_programmatic',
              (adobe_byodBouncersVisitors_programmatic,     adobe_byodBouncersVisitors_programmatic_wow,     adobe_byodBouncersVisitors_programmatic_ly,     adobe_byodBouncersVisitors_programmatic_wow_pct,     adobe_byodBouncersVisitors_programmatic_yoy_pct)     'adobe_byodBouncersVisitors_programmatic',
              (adobe_byodOrders_programmatic,               adobe_byodOrders_programmatic_wow,               adobe_byodOrders_programmatic_ly,               adobe_byodOrders_programmatic_wow_pct,               adobe_byodOrders_programmatic_yoy_pct)               'adobe_byodOrders_programmatic',
              (adobe_byodVrChatVisitors_other,              adobe_byodVrChatVisitors_other_wow,              adobe_byodVrChatVisitors_other_ly,              adobe_byodVrChatVisitors_other_wow_pct,              adobe_byodVrChatVisitors_other_yoy_pct)              'adobe_byodVrChatVisitors_other',
              (adobe_byodCallVisitors_other,                adobe_byodCallVisitors_other_wow,                adobe_byodCallVisitors_other_ly,                adobe_byodCallVisitors_other_wow_pct,                adobe_byodCallVisitors_other_yoy_pct)                'adobe_byodCallVisitors_other',
              (adobe_byodStoreLocatorVisitors_other,        adobe_byodStoreLocatorVisitors_other_wow,        adobe_byodStoreLocatorVisitors_other_ly,        adobe_byodStoreLocatorVisitors_other_wow_pct,        adobe_byodStoreLocatorVisitors_other_yoy_pct)        'adobe_byodStoreLocatorVisitors_other',
              (adobe_byodInternalTmoVisitors_other,         adobe_byodInternalTmoVisitors_other_wow,         adobe_byodInternalTmoVisitors_other_ly,         adobe_byodInternalTmoVisitors_other_wow_pct,         adobe_byodInternalTmoVisitors_other_yoy_pct)         'adobe_byodInternalTmoVisitors_other',
              (adobe_byodBouncersVisitors_other,            adobe_byodBouncersVisitors_other_wow,            adobe_byodBouncersVisitors_other_ly,            adobe_byodBouncersVisitors_other_wow_pct,            adobe_byodBouncersVisitors_other_yoy_pct)            'adobe_byodBouncersVisitors_other',
              (adobe_byodOrders_other,                      adobe_byodOrders_other_wow,                      adobe_byodOrders_other_ly,                      adobe_byodOrders_other_wow_pct,                      adobe_byodOrders_other_yoy_pct)                      'adobe_byodOrders_other'
          )
      )
  ),
  -- -----------------------------------------------------------------------
  -- COMBINE all active sources
  -- -----------------------------------------------------------------------
  combined AS (
      -- Profound
      SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING) AS dimension_name, CAST(NULL AS STRING) AS dimension_value, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM profound_long
      UNION ALL
      -- GoFish
      SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM gofish_long
      UNION ALL
      -- SA360 — COMMENTED OUT (Silver not yet built)
      -- SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM sa360_long
      -- UNION ALL
      -- GSC — COMMENTED OUT (Silver not yet built)
      -- SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM gsc_long
      -- UNION ALL
      -- Trends index — COMMENTED OUT (Silver not yet built)
      -- SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM trends_index_long
      -- UNION ALL
      -- Trends keywords — COMMENTED OUT (Silver not yet built)
      -- SELECT week_sun_to_sat, data_source, channel, max_data_date, dimension_name, dimension_value, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM trends_keywords_long
      -- UNION ALL
      -- Adobe — conversion metrics
      SELECT
          week_sun_to_sat, data_source,
          CASE
              WHEN metric_name LIKE '%_allChannels'   THEN 'All Channels'
              WHEN metric_name LIKE '%_paidSearch'    THEN 'Paid Search'
              WHEN metric_name LIKE '%_organicSearch' THEN 'Organic Search'
              WHEN metric_name LIKE '%_direct'        THEN 'Direct'
              WHEN metric_name LIKE '%_social'        THEN 'Paid Social'
              WHEN metric_name LIKE '%_programmatic'  THEN 'Programmatic'
              WHEN metric_name LIKE '%_other'         THEN 'Other'
          END AS channel,
          max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING),
          metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM adobe_long
      UNION ALL
      -- Adobe BYOD entry page metrics
      SELECT
          week_sun_to_sat, data_source,
          CASE
              WHEN metric_name LIKE '%_allChannels'   THEN 'All Channels'
              WHEN metric_name LIKE '%_paidSearch'    THEN 'Paid Search'
              WHEN metric_name LIKE '%_organicSearch' THEN 'Organic Search'
              WHEN metric_name LIKE '%_direct'        THEN 'Direct'
              WHEN metric_name LIKE '%_social'        THEN 'Paid Social'
              WHEN metric_name LIKE '%_programmatic'  THEN 'Programmatic'
              WHEN metric_name LIKE '%_other'         THEN 'Other'
          END AS channel,
          max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING),
          metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM entryPages_long
      UNION ALL
      -- Adobe BYOD outcome metrics
      SELECT
          week_sun_to_sat, data_source,
          CASE
              WHEN metric_name LIKE '%_allChannels'   THEN 'All Channels'
              WHEN metric_name LIKE '%_paidSearch'    THEN 'Paid Search'
              WHEN metric_name LIKE '%_organicSearch' THEN 'Organic Search'
              WHEN metric_name LIKE '%_direct'        THEN 'Direct'
              WHEN metric_name LIKE '%_social'        THEN 'Paid Social'
              WHEN metric_name LIKE '%_programmatic'  THEN 'Programmatic'
              WHEN metric_name LIKE '%_other'         THEN 'Other'
          END AS channel,
          max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING),
          metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct
      FROM outcomes_long
  )

  SELECT
      week_sun_to_sat,
      'WEEKLY'                                                           AS time_granularity,
      data_source,
      channel,
      dimension_name,
      dimension_value,
      metric_name,
      metric_value,
      metric_value_wow,
      metric_value_ly,
      wow_pct,
      yoy_pct,
      MAX(max_data_date) OVER (PARTITION BY data_source)                 AS max_data_date
  FROM combined
  ORDER BY
      week_sun_to_sat  ASC,
      data_source      ASC,
      channel          ASC,
      metric_name      ASC,
      dimension_name   ASC
  ;

END;