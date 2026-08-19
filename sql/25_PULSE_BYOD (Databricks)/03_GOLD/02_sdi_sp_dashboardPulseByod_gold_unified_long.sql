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
    -- -----------------------------------------------------------------------
  -- PROFOUND: 15 rows/week (explicit UNION ALL — every metric always
  -- produces a row, nulls included; no UNPIVOT EXCLUDE NULLS row-dropping)
  -- -----------------------------------------------------------------------
  profound_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_tmo_nonbrand_visibilityScore' AS metric_name, profoundVis_tmo_nonbrand_visibilityScore AS metric_value, profoundVis_tmo_nonbrand_visibilityScore_wow AS metric_value_wow, profoundVis_tmo_nonbrand_visibilityScore_ly AS metric_value_ly, profoundVis_tmo_nonbrand_visibilityScore_wow_pct AS wow_pct, profoundVis_tmo_nonbrand_visibilityScore_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_tmo_nonbrand_executions' AS metric_name, profoundVis_tmo_nonbrand_executions AS metric_value, profoundVis_tmo_nonbrand_executions_wow AS metric_value_wow, profoundVis_tmo_nonbrand_executions_ly AS metric_value_ly, profoundVis_tmo_nonbrand_executions_wow_pct AS wow_pct, profoundVis_tmo_nonbrand_executions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_tmo_nonbrand_mentionsCount' AS metric_name, profoundVis_tmo_nonbrand_mentionsCount AS metric_value, profoundVis_tmo_nonbrand_mentionsCount_wow AS metric_value_wow, profoundVis_tmo_nonbrand_mentionsCount_ly AS metric_value_ly, profoundVis_tmo_nonbrand_mentionsCount_wow_pct AS wow_pct, profoundVis_tmo_nonbrand_mentionsCount_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_tmo_nonbrand_shareOfVoice' AS metric_name, profoundVis_tmo_nonbrand_shareOfVoice AS metric_value, profoundVis_tmo_nonbrand_shareOfVoice_wow AS metric_value_wow, profoundVis_tmo_nonbrand_shareOfVoice_ly AS metric_value_ly, profoundVis_tmo_nonbrand_shareOfVoice_wow_pct AS wow_pct, profoundVis_tmo_nonbrand_shareOfVoice_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_verizon_nonbrand_visibilityScore' AS metric_name, profoundVis_verizon_nonbrand_visibilityScore AS metric_value, profoundVis_verizon_nonbrand_visibilityScore_wow AS metric_value_wow, profoundVis_verizon_nonbrand_visibilityScore_ly AS metric_value_ly, profoundVis_verizon_nonbrand_visibilityScore_wow_pct AS wow_pct, profoundVis_verizon_nonbrand_visibilityScore_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_verizon_nonbrand_executions' AS metric_name, profoundVis_verizon_nonbrand_executions AS metric_value, profoundVis_verizon_nonbrand_executions_wow AS metric_value_wow, profoundVis_verizon_nonbrand_executions_ly AS metric_value_ly, profoundVis_verizon_nonbrand_executions_wow_pct AS wow_pct, profoundVis_verizon_nonbrand_executions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_verizon_nonbrand_mentionsCount' AS metric_name, profoundVis_verizon_nonbrand_mentionsCount AS metric_value, profoundVis_verizon_nonbrand_mentionsCount_wow AS metric_value_wow, profoundVis_verizon_nonbrand_mentionsCount_ly AS metric_value_ly, profoundVis_verizon_nonbrand_mentionsCount_wow_pct AS wow_pct, profoundVis_verizon_nonbrand_mentionsCount_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_verizon_nonbrand_shareOfVoice' AS metric_name, profoundVis_verizon_nonbrand_shareOfVoice AS metric_value, profoundVis_verizon_nonbrand_shareOfVoice_wow AS metric_value_wow, profoundVis_verizon_nonbrand_shareOfVoice_ly AS metric_value_ly, profoundVis_verizon_nonbrand_shareOfVoice_wow_pct AS wow_pct, profoundVis_verizon_nonbrand_shareOfVoice_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_att_nonbrand_visibilityScore' AS metric_name, profoundVis_att_nonbrand_visibilityScore AS metric_value, profoundVis_att_nonbrand_visibilityScore_wow AS metric_value_wow, profoundVis_att_nonbrand_visibilityScore_ly AS metric_value_ly, profoundVis_att_nonbrand_visibilityScore_wow_pct AS wow_pct, profoundVis_att_nonbrand_visibilityScore_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_att_nonbrand_executions' AS metric_name, profoundVis_att_nonbrand_executions AS metric_value, profoundVis_att_nonbrand_executions_wow AS metric_value_wow, profoundVis_att_nonbrand_executions_ly AS metric_value_ly, profoundVis_att_nonbrand_executions_wow_pct AS wow_pct, profoundVis_att_nonbrand_executions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_att_nonbrand_mentionsCount' AS metric_name, profoundVis_att_nonbrand_mentionsCount AS metric_value, profoundVis_att_nonbrand_mentionsCount_wow AS metric_value_wow, profoundVis_att_nonbrand_mentionsCount_ly AS metric_value_ly, profoundVis_att_nonbrand_mentionsCount_wow_pct AS wow_pct, profoundVis_att_nonbrand_mentionsCount_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundVis_att_nonbrand_shareOfVoice' AS metric_name, profoundVis_att_nonbrand_shareOfVoice AS metric_value, profoundVis_att_nonbrand_shareOfVoice_wow AS metric_value_wow, profoundVis_att_nonbrand_shareOfVoice_ly AS metric_value_ly, profoundVis_att_nonbrand_shareOfVoice_wow_pct AS wow_pct, profoundVis_att_nonbrand_shareOfVoice_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundCit_tmo_nonbrand_shareOfVoice' AS metric_name, profoundCit_tmo_nonbrand_shareOfVoice AS metric_value, profoundCit_tmo_nonbrand_shareOfVoice_wow AS metric_value_wow, profoundCit_tmo_nonbrand_shareOfVoice_ly AS metric_value_ly, profoundCit_tmo_nonbrand_shareOfVoice_wow_pct AS wow_pct, profoundCit_tmo_nonbrand_shareOfVoice_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundCit_verizon_nonbrand_shareOfVoice' AS metric_name, profoundCit_verizon_nonbrand_shareOfVoice AS metric_value, profoundCit_verizon_nonbrand_shareOfVoice_wow AS metric_value_wow, profoundCit_verizon_nonbrand_shareOfVoice_ly AS metric_value_ly, profoundCit_verizon_nonbrand_shareOfVoice_wow_pct AS wow_pct, profoundCit_verizon_nonbrand_shareOfVoice_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'profoundCit_att_nonbrand_shareOfVoice' AS metric_name, profoundCit_att_nonbrand_shareOfVoice AS metric_value, profoundCit_att_nonbrand_shareOfVoice_wow AS metric_value_wow, profoundCit_att_nonbrand_shareOfVoice_ly AS metric_value_ly, profoundCit_att_nonbrand_shareOfVoice_wow_pct AS wow_pct, profoundCit_att_nonbrand_shareOfVoice_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profound_weekly
  ),

  -- -----------------------------------------------------------------------
  -- GOFISH: 12 rows/week
  -- -----------------------------------------------------------------------
    -- -----------------------------------------------------------------------
  -- GOFISH: 12 rows/week (explicit UNION ALL — every metric always
  -- produces a row, nulls included; no UNPIVOT EXCLUDE NULLS row-dropping)
  -- -----------------------------------------------------------------------
  gofish_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_tmo_brand_visibilityScore' AS metric_name, gofish_tmo_brand_visibilityScore AS metric_value, gofish_tmo_brand_visibilityScore_wow AS metric_value_wow, gofish_tmo_brand_visibilityScore_ly AS metric_value_ly, gofish_tmo_brand_visibilityScore_wow_pct AS wow_pct, gofish_tmo_brand_visibilityScore_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_tmo_brand_executions' AS metric_name, gofish_tmo_brand_executions AS metric_value, gofish_tmo_brand_executions_wow AS metric_value_wow, gofish_tmo_brand_executions_ly AS metric_value_ly, gofish_tmo_brand_executions_wow_pct AS wow_pct, gofish_tmo_brand_executions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_tmo_brand_mentionsCount' AS metric_name, gofish_tmo_brand_mentionsCount AS metric_value, gofish_tmo_brand_mentionsCount_wow AS metric_value_wow, gofish_tmo_brand_mentionsCount_ly AS metric_value_ly, gofish_tmo_brand_mentionsCount_wow_pct AS wow_pct, gofish_tmo_brand_mentionsCount_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_tmo_brand_shareOfVoice' AS metric_name, gofish_tmo_brand_shareOfVoice AS metric_value, gofish_tmo_brand_shareOfVoice_wow AS metric_value_wow, gofish_tmo_brand_shareOfVoice_ly AS metric_value_ly, gofish_tmo_brand_shareOfVoice_wow_pct AS wow_pct, gofish_tmo_brand_shareOfVoice_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_verizon_brand_visibilityScore' AS metric_name, gofish_verizon_brand_visibilityScore AS metric_value, gofish_verizon_brand_visibilityScore_wow AS metric_value_wow, gofish_verizon_brand_visibilityScore_ly AS metric_value_ly, gofish_verizon_brand_visibilityScore_wow_pct AS wow_pct, gofish_verizon_brand_visibilityScore_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_verizon_brand_executions' AS metric_name, gofish_verizon_brand_executions AS metric_value, gofish_verizon_brand_executions_wow AS metric_value_wow, gofish_verizon_brand_executions_ly AS metric_value_ly, gofish_verizon_brand_executions_wow_pct AS wow_pct, gofish_verizon_brand_executions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_verizon_brand_mentionsCount' AS metric_name, gofish_verizon_brand_mentionsCount AS metric_value, gofish_verizon_brand_mentionsCount_wow AS metric_value_wow, gofish_verizon_brand_mentionsCount_ly AS metric_value_ly, gofish_verizon_brand_mentionsCount_wow_pct AS wow_pct, gofish_verizon_brand_mentionsCount_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_verizon_brand_shareOfVoice' AS metric_name, gofish_verizon_brand_shareOfVoice AS metric_value, gofish_verizon_brand_shareOfVoice_wow AS metric_value_wow, gofish_verizon_brand_shareOfVoice_ly AS metric_value_ly, gofish_verizon_brand_shareOfVoice_wow_pct AS wow_pct, gofish_verizon_brand_shareOfVoice_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_att_brand_visibilityScore' AS metric_name, gofish_att_brand_visibilityScore AS metric_value, gofish_att_brand_visibilityScore_wow AS metric_value_wow, gofish_att_brand_visibilityScore_ly AS metric_value_ly, gofish_att_brand_visibilityScore_wow_pct AS wow_pct, gofish_att_brand_visibilityScore_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_att_brand_executions' AS metric_name, gofish_att_brand_executions AS metric_value, gofish_att_brand_executions_wow AS metric_value_wow, gofish_att_brand_executions_ly AS metric_value_ly, gofish_att_brand_executions_wow_pct AS wow_pct, gofish_att_brand_executions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_att_brand_mentionsCount' AS metric_name, gofish_att_brand_mentionsCount AS metric_value, gofish_att_brand_mentionsCount_wow AS metric_value_wow, gofish_att_brand_mentionsCount_ly AS metric_value_ly, gofish_att_brand_mentionsCount_wow_pct AS wow_pct, gofish_att_brand_mentionsCount_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gofish_att_brand_shareOfVoice' AS metric_name, gofish_att_brand_shareOfVoice AS metric_value, gofish_att_brand_shareOfVoice_wow AS metric_value_wow, gofish_att_brand_shareOfVoice_ly AS metric_value_ly, gofish_att_brand_shareOfVoice_wow_pct AS wow_pct, gofish_att_brand_shareOfVoice_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_profoundGofish_weekly
  ),

  -- -----------------------------------------------------------------------
  -- SA360: 12 rows/week
  -- -----------------------------------------------------------------------
    -- -----------------------------------------------------------------------
  -- SA360: 12 rows/week (explicit UNION ALL — every metric always
  -- produces a row, nulls included; no UNPIVOT EXCLUDE NULLS row-dropping)
  -- -----------------------------------------------------------------------
  sa360_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_brand_impressions' AS metric_name, sa360_tmo_brand_impressions AS metric_value, sa360_tmo_brand_impressions_wow AS metric_value_wow, sa360_tmo_brand_impressions_ly AS metric_value_ly, sa360_tmo_brand_impressions_wow_pct AS wow_pct, sa360_tmo_brand_impressions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_brand_clicks' AS metric_name, sa360_tmo_brand_clicks AS metric_value, sa360_tmo_brand_clicks_wow AS metric_value_wow, sa360_tmo_brand_clicks_ly AS metric_value_ly, sa360_tmo_brand_clicks_wow_pct AS wow_pct, sa360_tmo_brand_clicks_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_brand_cost' AS metric_name, sa360_tmo_brand_cost AS metric_value, sa360_tmo_brand_cost_wow AS metric_value_wow, sa360_tmo_brand_cost_ly AS metric_value_ly, sa360_tmo_brand_cost_wow_pct AS wow_pct, sa360_tmo_brand_cost_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_brand_orders' AS metric_name, sa360_tmo_brand_orders AS metric_value, sa360_tmo_brand_orders_wow AS metric_value_wow, sa360_tmo_brand_orders_ly AS metric_value_ly, sa360_tmo_brand_orders_wow_pct AS wow_pct, sa360_tmo_brand_orders_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_brand_cart_start' AS metric_name, sa360_tmo_brand_cart_start AS metric_value, sa360_tmo_brand_cart_start_wow AS metric_value_wow, sa360_tmo_brand_cart_start_ly AS metric_value_ly, sa360_tmo_brand_cart_start_wow_pct AS wow_pct, sa360_tmo_brand_cart_start_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_brand_postpaid_pspv' AS metric_name, sa360_tmo_brand_postpaid_pspv AS metric_value, sa360_tmo_brand_postpaid_pspv_wow AS metric_value_wow, sa360_tmo_brand_postpaid_pspv_ly AS metric_value_ly, sa360_tmo_brand_postpaid_pspv_wow_pct AS wow_pct, sa360_tmo_brand_postpaid_pspv_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_nonbrand_impressions' AS metric_name, sa360_tmo_nonbrand_impressions AS metric_value, sa360_tmo_nonbrand_impressions_wow AS metric_value_wow, sa360_tmo_nonbrand_impressions_ly AS metric_value_ly, sa360_tmo_nonbrand_impressions_wow_pct AS wow_pct, sa360_tmo_nonbrand_impressions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_nonbrand_clicks' AS metric_name, sa360_tmo_nonbrand_clicks AS metric_value, sa360_tmo_nonbrand_clicks_wow AS metric_value_wow, sa360_tmo_nonbrand_clicks_ly AS metric_value_ly, sa360_tmo_nonbrand_clicks_wow_pct AS wow_pct, sa360_tmo_nonbrand_clicks_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_nonbrand_cost' AS metric_name, sa360_tmo_nonbrand_cost AS metric_value, sa360_tmo_nonbrand_cost_wow AS metric_value_wow, sa360_tmo_nonbrand_cost_ly AS metric_value_ly, sa360_tmo_nonbrand_cost_wow_pct AS wow_pct, sa360_tmo_nonbrand_cost_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_nonbrand_orders' AS metric_name, sa360_tmo_nonbrand_orders AS metric_value, sa360_tmo_nonbrand_orders_wow AS metric_value_wow, sa360_tmo_nonbrand_orders_ly AS metric_value_ly, sa360_tmo_nonbrand_orders_wow_pct AS wow_pct, sa360_tmo_nonbrand_orders_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_nonbrand_cart_start' AS metric_name, sa360_tmo_nonbrand_cart_start AS metric_value, sa360_tmo_nonbrand_cart_start_wow AS metric_value_wow, sa360_tmo_nonbrand_cart_start_ly AS metric_value_ly, sa360_tmo_nonbrand_cart_start_wow_pct AS wow_pct, sa360_tmo_nonbrand_cart_start_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'sa360_tmo_nonbrand_postpaid_pspv' AS metric_name, sa360_tmo_nonbrand_postpaid_pspv AS metric_value, sa360_tmo_nonbrand_postpaid_pspv_wow AS metric_value_wow, sa360_tmo_nonbrand_postpaid_pspv_ly AS metric_value_ly, sa360_tmo_nonbrand_postpaid_pspv_wow_pct AS wow_pct, sa360_tmo_nonbrand_postpaid_pspv_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_sa360_weekly
  ),

  -- -----------------------------------------------------------------------
  -- GSC: 4 rows/week
  -- -----------------------------------------------------------------------
    -- -----------------------------------------------------------------------
  -- GSC: 4 rows/week (explicit UNION ALL — every metric always
  -- produces a row, nulls included; no UNPIVOT EXCLUDE NULLS row-dropping)
  -- -----------------------------------------------------------------------
  gsc_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gsc_tmo_brand_impressions' AS metric_name, gsc_tmo_brand_impressions AS metric_value, gsc_tmo_brand_impressions_wow AS metric_value_wow, gsc_tmo_brand_impressions_ly AS metric_value_ly, gsc_tmo_brand_impressions_wow_pct AS wow_pct, gsc_tmo_brand_impressions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_gsc_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gsc_tmo_brand_clicks' AS metric_name, gsc_tmo_brand_clicks AS metric_value, gsc_tmo_brand_clicks_wow AS metric_value_wow, gsc_tmo_brand_clicks_ly AS metric_value_ly, gsc_tmo_brand_clicks_wow_pct AS wow_pct, gsc_tmo_brand_clicks_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_gsc_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gsc_tmo_nonbrand_impressions' AS metric_name, gsc_tmo_nonbrand_impressions AS metric_value, gsc_tmo_nonbrand_impressions_wow AS metric_value_wow, gsc_tmo_nonbrand_impressions_ly AS metric_value_ly, gsc_tmo_nonbrand_impressions_wow_pct AS wow_pct, gsc_tmo_nonbrand_impressions_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_gsc_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'gsc_tmo_nonbrand_clicks' AS metric_name, gsc_tmo_nonbrand_clicks AS metric_value, gsc_tmo_nonbrand_clicks_wow AS metric_value_wow, gsc_tmo_nonbrand_clicks_ly AS metric_value_ly, gsc_tmo_nonbrand_clicks_wow_pct AS wow_pct, gsc_tmo_nonbrand_clicks_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_gsc_weekly
  ),

  -- -----------------------------------------------------------------------
  -- TRENDS: byod_index — 1 row/week
  -- -----------------------------------------------------------------------
    -- -----------------------------------------------------------------------
  -- TRENDS_INDEX: 1 rows/week (explicit UNION ALL — every metric always
  -- produces a row, nulls included; no UNPIVOT EXCLUDE NULLS row-dropping)
  -- -----------------------------------------------------------------------
  trends_index_long AS (
      SELECT week_sun_to_sat, data_source, channel, max_data_date, 'trends_byod_index' AS metric_name, trends_byod_index AS metric_value, trends_byod_index_wow AS metric_value_wow, trends_byod_index_ly AS metric_value_ly, trends_byod_index_wow_pct AS wow_pct, trends_byod_index_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly
  ),

  -- -----------------------------------------------------------------------
  -- TRENDS: Keywords — up to 10 rows/week
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

  -- -----------------------------------------------------------------------
  -- ADOBE: conversion metrics
  -- -----------------------------------------------------------------------
    -- -----------------------------------------------------------------------
  -- ADOBE: 50 rows/week (explicit UNION ALL — every metric always
  -- produces a row, nulls included; no UNPIVOT EXCLUDE NULLS row-dropping)
  -- -----------------------------------------------------------------------
  adobe_long AS (
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_upvByod_allChannels' AS metric_name, adobe_upvByod_allChannels AS metric_value, adobe_upvByod_allChannels_wow AS metric_value_wow, adobe_upvByod_allChannels_ly AS metric_value_ly, adobe_upvByod_allChannels_wow_pct AS wow_pct, adobe_upvByod_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_upvTotal_allChannels' AS metric_name, adobe_upvTotal_allChannels AS metric_value, adobe_upvTotal_allChannels_wow AS metric_value_wow, adobe_upvTotal_allChannels_ly AS metric_value_ly, adobe_upvTotal_allChannels_wow_pct AS wow_pct, adobe_upvTotal_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_upvFlowTotal_allChannels' AS metric_name, adobe_upvFlowTotal_allChannels AS metric_value, adobe_upvFlowTotal_allChannels_wow AS metric_value_wow, adobe_upvFlowTotal_allChannels_ly AS metric_value_ly, adobe_upvFlowTotal_allChannels_wow_pct AS wow_pct, adobe_upvFlowTotal_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_pctUpvByodOfUpvFlow_allChannels' AS metric_name, adobe_pctUpvByodOfUpvFlow_allChannels AS metric_value, adobe_pctUpvByodOfUpvFlow_allChannels_wow AS metric_value_wow, adobe_pctUpvByodOfUpvFlow_allChannels_ly AS metric_value_ly, adobe_pctUpvByodOfUpvFlow_allChannels_wow_pct AS wow_pct, adobe_pctUpvByodOfUpvFlow_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cartStartByod_allChannels' AS metric_name, adobe_cartStartByod_allChannels AS metric_value, adobe_cartStartByod_allChannels_wow AS metric_value_wow, adobe_cartStartByod_allChannels_ly AS metric_value_ly, adobe_cartStartByod_allChannels_wow_pct AS wow_pct, adobe_cartStartByod_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersUnassistedByod_allChannels' AS metric_name, adobe_ordersUnassistedByod_allChannels AS metric_value, adobe_ordersUnassistedByod_allChannels_wow AS metric_value_wow, adobe_ordersUnassistedByod_allChannels_ly AS metric_value_ly, adobe_ordersUnassistedByod_allChannels_wow_pct AS wow_pct, adobe_ordersUnassistedByod_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersAssistedByod_allChannels' AS metric_name, adobe_ordersAssistedByod_allChannels AS metric_value, adobe_ordersAssistedByod_allChannels_wow AS metric_value_wow, adobe_ordersAssistedByod_allChannels_ly AS metric_value_ly, adobe_ordersAssistedByod_allChannels_wow_pct AS wow_pct, adobe_ordersAssistedByod_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersTotalByod_allChannels' AS metric_name, adobe_ordersTotalByod_allChannels AS metric_value, adobe_ordersTotalByod_allChannels_wow AS metric_value_wow, adobe_ordersTotalByod_allChannels_ly AS metric_value_ly, adobe_ordersTotalByod_allChannels_wow_pct AS wow_pct, adobe_ordersTotalByod_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersTotal_allChannels' AS metric_name, adobe_ordersTotal_allChannels AS metric_value, adobe_ordersTotal_allChannels_wow AS metric_value_wow, adobe_ordersTotal_allChannels_ly AS metric_value_ly, adobe_ordersTotal_allChannels_wow_pct AS wow_pct, adobe_ordersTotal_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_pctOrdersByodOfOrdersTotal_allChannels' AS metric_name, adobe_pctOrdersByodOfOrdersTotal_allChannels AS metric_value, adobe_pctOrdersByodOfOrdersTotal_allChannels_wow AS metric_value_wow, adobe_pctOrdersByodOfOrdersTotal_allChannels_ly AS metric_value_ly, adobe_pctOrdersByodOfOrdersTotal_allChannels_wow_pct AS wow_pct, adobe_pctOrdersByodOfOrdersTotal_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cvrByod_allChannels' AS metric_name, adobe_cvrByod_allChannels AS metric_value, adobe_cvrByod_allChannels_wow AS metric_value_wow, adobe_cvrByod_allChannels_ly AS metric_value_ly, adobe_cvrByod_allChannels_wow_pct AS wow_pct, adobe_cvrByod_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cvrSite_allChannels' AS metric_name, adobe_cvrSite_allChannels AS metric_value, adobe_cvrSite_allChannels_wow AS metric_value_wow, adobe_cvrSite_allChannels_ly AS metric_value_ly, adobe_cvrSite_allChannels_wow_pct AS wow_pct, adobe_cvrSite_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cvrPostpaid_allChannels' AS metric_name, adobe_cvrPostpaid_allChannels AS metric_value, adobe_cvrPostpaid_allChannels_wow AS metric_value_wow, adobe_cvrPostpaid_allChannels_ly AS metric_value_ly, adobe_cvrPostpaid_allChannels_wow_pct AS wow_pct, adobe_cvrPostpaid_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cvrHsi_allChannels' AS metric_name, adobe_cvrHsi_allChannels AS metric_value, adobe_cvrHsi_allChannels_wow AS metric_value_wow, adobe_cvrHsi_allChannels_ly AS metric_value_ly, adobe_cvrHsi_allChannels_wow_pct AS wow_pct, adobe_cvrHsi_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_upvByod_paidSearch' AS metric_name, adobe_upvByod_paidSearch AS metric_value, adobe_upvByod_paidSearch_wow AS metric_value_wow, adobe_upvByod_paidSearch_ly AS metric_value_ly, adobe_upvByod_paidSearch_wow_pct AS wow_pct, adobe_upvByod_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_pctUpvByodOfTotal_paidSearch' AS metric_name, adobe_pctUpvByodOfTotal_paidSearch AS metric_value, adobe_pctUpvByodOfTotal_paidSearch_wow AS metric_value_wow, adobe_pctUpvByodOfTotal_paidSearch_ly AS metric_value_ly, adobe_pctUpvByodOfTotal_paidSearch_wow_pct AS wow_pct, adobe_pctUpvByodOfTotal_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cartStartByod_paidSearch' AS metric_name, adobe_cartStartByod_paidSearch AS metric_value, adobe_cartStartByod_paidSearch_wow AS metric_value_wow, adobe_cartStartByod_paidSearch_ly AS metric_value_ly, adobe_cartStartByod_paidSearch_wow_pct AS wow_pct, adobe_cartStartByod_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersUnassistedByod_paidSearch' AS metric_name, adobe_ordersUnassistedByod_paidSearch AS metric_value, adobe_ordersUnassistedByod_paidSearch_wow AS metric_value_wow, adobe_ordersUnassistedByod_paidSearch_ly AS metric_value_ly, adobe_ordersUnassistedByod_paidSearch_wow_pct AS wow_pct, adobe_ordersUnassistedByod_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersAssistedByod_paidSearch' AS metric_name, adobe_ordersAssistedByod_paidSearch AS metric_value, adobe_ordersAssistedByod_paidSearch_wow AS metric_value_wow, adobe_ordersAssistedByod_paidSearch_ly AS metric_value_ly, adobe_ordersAssistedByod_paidSearch_wow_pct AS wow_pct, adobe_ordersAssistedByod_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersTotalByod_paidSearch' AS metric_name, adobe_ordersTotalByod_paidSearch AS metric_value, adobe_ordersTotalByod_paidSearch_wow AS metric_value_wow, adobe_ordersTotalByod_paidSearch_ly AS metric_value_ly, adobe_ordersTotalByod_paidSearch_wow_pct AS wow_pct, adobe_ordersTotalByod_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_upvByod_organicSearch' AS metric_name, adobe_upvByod_organicSearch AS metric_value, adobe_upvByod_organicSearch_wow AS metric_value_wow, adobe_upvByod_organicSearch_ly AS metric_value_ly, adobe_upvByod_organicSearch_wow_pct AS wow_pct, adobe_upvByod_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_pctUpvByodOfTotal_organicSearch' AS metric_name, adobe_pctUpvByodOfTotal_organicSearch AS metric_value, adobe_pctUpvByodOfTotal_organicSearch_wow AS metric_value_wow, adobe_pctUpvByodOfTotal_organicSearch_ly AS metric_value_ly, adobe_pctUpvByodOfTotal_organicSearch_wow_pct AS wow_pct, adobe_pctUpvByodOfTotal_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cartStartByod_organicSearch' AS metric_name, adobe_cartStartByod_organicSearch AS metric_value, adobe_cartStartByod_organicSearch_wow AS metric_value_wow, adobe_cartStartByod_organicSearch_ly AS metric_value_ly, adobe_cartStartByod_organicSearch_wow_pct AS wow_pct, adobe_cartStartByod_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersUnassistedByod_organicSearch' AS metric_name, adobe_ordersUnassistedByod_organicSearch AS metric_value, adobe_ordersUnassistedByod_organicSearch_wow AS metric_value_wow, adobe_ordersUnassistedByod_organicSearch_ly AS metric_value_ly, adobe_ordersUnassistedByod_organicSearch_wow_pct AS wow_pct, adobe_ordersUnassistedByod_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersAssistedByod_organicSearch' AS metric_name, adobe_ordersAssistedByod_organicSearch AS metric_value, adobe_ordersAssistedByod_organicSearch_wow AS metric_value_wow, adobe_ordersAssistedByod_organicSearch_ly AS metric_value_ly, adobe_ordersAssistedByod_organicSearch_wow_pct AS wow_pct, adobe_ordersAssistedByod_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersTotalByod_organicSearch' AS metric_name, adobe_ordersTotalByod_organicSearch AS metric_value, adobe_ordersTotalByod_organicSearch_wow AS metric_value_wow, adobe_ordersTotalByod_organicSearch_ly AS metric_value_ly, adobe_ordersTotalByod_organicSearch_wow_pct AS wow_pct, adobe_ordersTotalByod_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_upvByod_direct' AS metric_name, adobe_upvByod_direct AS metric_value, adobe_upvByod_direct_wow AS metric_value_wow, adobe_upvByod_direct_ly AS metric_value_ly, adobe_upvByod_direct_wow_pct AS wow_pct, adobe_upvByod_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_pctUpvByodOfTotal_direct' AS metric_name, adobe_pctUpvByodOfTotal_direct AS metric_value, adobe_pctUpvByodOfTotal_direct_wow AS metric_value_wow, adobe_pctUpvByodOfTotal_direct_ly AS metric_value_ly, adobe_pctUpvByodOfTotal_direct_wow_pct AS wow_pct, adobe_pctUpvByodOfTotal_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cartStartByod_direct' AS metric_name, adobe_cartStartByod_direct AS metric_value, adobe_cartStartByod_direct_wow AS metric_value_wow, adobe_cartStartByod_direct_ly AS metric_value_ly, adobe_cartStartByod_direct_wow_pct AS wow_pct, adobe_cartStartByod_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersUnassistedByod_direct' AS metric_name, adobe_ordersUnassistedByod_direct AS metric_value, adobe_ordersUnassistedByod_direct_wow AS metric_value_wow, adobe_ordersUnassistedByod_direct_ly AS metric_value_ly, adobe_ordersUnassistedByod_direct_wow_pct AS wow_pct, adobe_ordersUnassistedByod_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersAssistedByod_direct' AS metric_name, adobe_ordersAssistedByod_direct AS metric_value, adobe_ordersAssistedByod_direct_wow AS metric_value_wow, adobe_ordersAssistedByod_direct_ly AS metric_value_ly, adobe_ordersAssistedByod_direct_wow_pct AS wow_pct, adobe_ordersAssistedByod_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersTotalByod_direct' AS metric_name, adobe_ordersTotalByod_direct AS metric_value, adobe_ordersTotalByod_direct_wow AS metric_value_wow, adobe_ordersTotalByod_direct_ly AS metric_value_ly, adobe_ordersTotalByod_direct_wow_pct AS wow_pct, adobe_ordersTotalByod_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_upvByod_social' AS metric_name, adobe_upvByod_social AS metric_value, adobe_upvByod_social_wow AS metric_value_wow, adobe_upvByod_social_ly AS metric_value_ly, adobe_upvByod_social_wow_pct AS wow_pct, adobe_upvByod_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_pctUpvByodOfTotal_social' AS metric_name, adobe_pctUpvByodOfTotal_social AS metric_value, adobe_pctUpvByodOfTotal_social_wow AS metric_value_wow, adobe_pctUpvByodOfTotal_social_ly AS metric_value_ly, adobe_pctUpvByodOfTotal_social_wow_pct AS wow_pct, adobe_pctUpvByodOfTotal_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cartStartByod_social' AS metric_name, adobe_cartStartByod_social AS metric_value, adobe_cartStartByod_social_wow AS metric_value_wow, adobe_cartStartByod_social_ly AS metric_value_ly, adobe_cartStartByod_social_wow_pct AS wow_pct, adobe_cartStartByod_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersUnassistedByod_social' AS metric_name, adobe_ordersUnassistedByod_social AS metric_value, adobe_ordersUnassistedByod_social_wow AS metric_value_wow, adobe_ordersUnassistedByod_social_ly AS metric_value_ly, adobe_ordersUnassistedByod_social_wow_pct AS wow_pct, adobe_ordersUnassistedByod_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersAssistedByod_social' AS metric_name, adobe_ordersAssistedByod_social AS metric_value, adobe_ordersAssistedByod_social_wow AS metric_value_wow, adobe_ordersAssistedByod_social_ly AS metric_value_ly, adobe_ordersAssistedByod_social_wow_pct AS wow_pct, adobe_ordersAssistedByod_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersTotalByod_social' AS metric_name, adobe_ordersTotalByod_social AS metric_value, adobe_ordersTotalByod_social_wow AS metric_value_wow, adobe_ordersTotalByod_social_ly AS metric_value_ly, adobe_ordersTotalByod_social_wow_pct AS wow_pct, adobe_ordersTotalByod_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_upvByod_programmatic' AS metric_name, adobe_upvByod_programmatic AS metric_value, adobe_upvByod_programmatic_wow AS metric_value_wow, adobe_upvByod_programmatic_ly AS metric_value_ly, adobe_upvByod_programmatic_wow_pct AS wow_pct, adobe_upvByod_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_pctUpvByodOfTotal_programmatic' AS metric_name, adobe_pctUpvByodOfTotal_programmatic AS metric_value, adobe_pctUpvByodOfTotal_programmatic_wow AS metric_value_wow, adobe_pctUpvByodOfTotal_programmatic_ly AS metric_value_ly, adobe_pctUpvByodOfTotal_programmatic_wow_pct AS wow_pct, adobe_pctUpvByodOfTotal_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cartStartByod_programmatic' AS metric_name, adobe_cartStartByod_programmatic AS metric_value, adobe_cartStartByod_programmatic_wow AS metric_value_wow, adobe_cartStartByod_programmatic_ly AS metric_value_ly, adobe_cartStartByod_programmatic_wow_pct AS wow_pct, adobe_cartStartByod_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersUnassistedByod_programmatic' AS metric_name, adobe_ordersUnassistedByod_programmatic AS metric_value, adobe_ordersUnassistedByod_programmatic_wow AS metric_value_wow, adobe_ordersUnassistedByod_programmatic_ly AS metric_value_ly, adobe_ordersUnassistedByod_programmatic_wow_pct AS wow_pct, adobe_ordersUnassistedByod_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersAssistedByod_programmatic' AS metric_name, adobe_ordersAssistedByod_programmatic AS metric_value, adobe_ordersAssistedByod_programmatic_wow AS metric_value_wow, adobe_ordersAssistedByod_programmatic_ly AS metric_value_ly, adobe_ordersAssistedByod_programmatic_wow_pct AS wow_pct, adobe_ordersAssistedByod_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersTotalByod_programmatic' AS metric_name, adobe_ordersTotalByod_programmatic AS metric_value, adobe_ordersTotalByod_programmatic_wow AS metric_value_wow, adobe_ordersTotalByod_programmatic_ly AS metric_value_ly, adobe_ordersTotalByod_programmatic_wow_pct AS wow_pct, adobe_ordersTotalByod_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_upvByod_other' AS metric_name, adobe_upvByod_other AS metric_value, adobe_upvByod_other_wow AS metric_value_wow, adobe_upvByod_other_ly AS metric_value_ly, adobe_upvByod_other_wow_pct AS wow_pct, adobe_upvByod_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_pctUpvByodOfTotal_other' AS metric_name, adobe_pctUpvByodOfTotal_other AS metric_value, adobe_pctUpvByodOfTotal_other_wow AS metric_value_wow, adobe_pctUpvByodOfTotal_other_ly AS metric_value_ly, adobe_pctUpvByodOfTotal_other_wow_pct AS wow_pct, adobe_pctUpvByodOfTotal_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_cartStartByod_other' AS metric_name, adobe_cartStartByod_other AS metric_value, adobe_cartStartByod_other_wow AS metric_value_wow, adobe_cartStartByod_other_ly AS metric_value_ly, adobe_cartStartByod_other_wow_pct AS wow_pct, adobe_cartStartByod_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersUnassistedByod_other' AS metric_name, adobe_ordersUnassistedByod_other AS metric_value, adobe_ordersUnassistedByod_other_wow AS metric_value_wow, adobe_ordersUnassistedByod_other_ly AS metric_value_ly, adobe_ordersUnassistedByod_other_wow_pct AS wow_pct, adobe_ordersUnassistedByod_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersAssistedByod_other' AS metric_name, adobe_ordersAssistedByod_other AS metric_value, adobe_ordersAssistedByod_other_wow AS metric_value_wow, adobe_ordersAssistedByod_other_ly AS metric_value_ly, adobe_ordersAssistedByod_other_wow_pct AS wow_pct, adobe_ordersAssistedByod_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_ordersTotalByod_other' AS metric_name, adobe_ordersTotalByod_other AS metric_value, adobe_ordersTotalByod_other_wow AS metric_value_wow, adobe_ordersTotalByod_other_ly AS metric_value_ly, adobe_ordersTotalByod_other_wow_pct AS wow_pct, adobe_ordersTotalByod_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobe_weekly
  ),
  -- -----------------------------------------------------------------------
  -- ADOBE BYOD ENTRY PAGES: 9 metrics x 7 channels — 63 rows/week
  -- -----------------------------------------------------------------------
    -- -----------------------------------------------------------------------
  -- ENTRYPAGES: 63 rows/week (explicit UNION ALL — every metric always
  -- produces a row, nulls included; no UNPIVOT EXCLUDE NULLS row-dropping)
  -- -----------------------------------------------------------------------
  entryPages_long AS (
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodUpvVisitors_allChannels' AS metric_name, adobe_byodUpvVisitors_allChannels AS metric_value, adobe_byodUpvVisitors_allChannels_wow AS metric_value_wow, adobe_byodUpvVisitors_allChannels_ly AS metric_value_ly, adobe_byodUpvVisitors_allChannels_wow_pct AS wow_pct, adobe_byodUpvVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodPageVisitors_allChannels' AS metric_name, adobe_byodEntryByodPageVisitors_allChannels AS metric_value, adobe_byodEntryByodPageVisitors_allChannels_wow AS metric_value_wow, adobe_byodEntryByodPageVisitors_allChannels_ly AS metric_value_ly, adobe_byodEntryByodPageVisitors_allChannels_wow_pct AS wow_pct, adobe_byodEntryByodPageVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryHomePageVisitors_allChannels' AS metric_name, adobe_byodEntryHomePageVisitors_allChannels AS metric_value, adobe_byodEntryHomePageVisitors_allChannels_wow AS metric_value_wow, adobe_byodEntryHomePageVisitors_allChannels_ly AS metric_value_ly, adobe_byodEntryHomePageVisitors_allChannels_wow_pct AS wow_pct, adobe_byodEntryHomePageVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryDevicePageVisitors_allChannels' AS metric_name, adobe_byodEntryDevicePageVisitors_allChannels AS metric_value, adobe_byodEntryDevicePageVisitors_allChannels_wow AS metric_value_wow, adobe_byodEntryDevicePageVisitors_allChannels_ly AS metric_value_ly, adobe_byodEntryDevicePageVisitors_allChannels_wow_pct AS wow_pct, adobe_byodEntryDevicePageVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryPlansPageVisitors_allChannels' AS metric_name, adobe_byodEntryPlansPageVisitors_allChannels AS metric_value, adobe_byodEntryPlansPageVisitors_allChannels_wow AS metric_value_wow, adobe_byodEntryPlansPageVisitors_allChannels_ly AS metric_value_ly, adobe_byodEntryPlansPageVisitors_allChannels_wow_pct AS wow_pct, adobe_byodEntryPlansPageVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOtherPageVisitors_allChannels' AS metric_name, adobe_byodEntryOtherPageVisitors_allChannels AS metric_value, adobe_byodEntryOtherPageVisitors_allChannels_wow AS metric_value_wow, adobe_byodEntryOtherPageVisitors_allChannels_ly AS metric_value_ly, adobe_byodEntryOtherPageVisitors_allChannels_wow_pct AS wow_pct, adobe_byodEntryOtherPageVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryGetStartedLandingPageVisitors_allChannels' AS metric_name, adobe_byodEntryGetStartedLandingPageVisitors_allChannels AS metric_value, adobe_byodEntryGetStartedLandingPageVisitors_allChannels_wow AS metric_value_wow, adobe_byodEntryGetStartedLandingPageVisitors_allChannels_ly AS metric_value_ly, adobe_byodEntryGetStartedLandingPageVisitors_allChannels_wow_pct AS wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodLandingPageVisitors_allChannels' AS metric_name, adobe_byodEntryByodLandingPageVisitors_allChannels AS metric_value, adobe_byodEntryByodLandingPageVisitors_allChannels_wow AS metric_value_wow, adobe_byodEntryByodLandingPageVisitors_allChannels_ly AS metric_value_ly, adobe_byodEntryByodLandingPageVisitors_allChannels_wow_pct AS wow_pct, adobe_byodEntryByodLandingPageVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOffersSwitchVisitors_allChannels' AS metric_name, adobe_byodEntryOffersSwitchVisitors_allChannels AS metric_value, adobe_byodEntryOffersSwitchVisitors_allChannels_wow AS metric_value_wow, adobe_byodEntryOffersSwitchVisitors_allChannels_ly AS metric_value_ly, adobe_byodEntryOffersSwitchVisitors_allChannels_wow_pct AS wow_pct, adobe_byodEntryOffersSwitchVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodUpvVisitors_paidSearch' AS metric_name, adobe_byodUpvVisitors_paidSearch AS metric_value, adobe_byodUpvVisitors_paidSearch_wow AS metric_value_wow, adobe_byodUpvVisitors_paidSearch_ly AS metric_value_ly, adobe_byodUpvVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodUpvVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodPageVisitors_paidSearch' AS metric_name, adobe_byodEntryByodPageVisitors_paidSearch AS metric_value, adobe_byodEntryByodPageVisitors_paidSearch_wow AS metric_value_wow, adobe_byodEntryByodPageVisitors_paidSearch_ly AS metric_value_ly, adobe_byodEntryByodPageVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodEntryByodPageVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryHomePageVisitors_paidSearch' AS metric_name, adobe_byodEntryHomePageVisitors_paidSearch AS metric_value, adobe_byodEntryHomePageVisitors_paidSearch_wow AS metric_value_wow, adobe_byodEntryHomePageVisitors_paidSearch_ly AS metric_value_ly, adobe_byodEntryHomePageVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodEntryHomePageVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryDevicePageVisitors_paidSearch' AS metric_name, adobe_byodEntryDevicePageVisitors_paidSearch AS metric_value, adobe_byodEntryDevicePageVisitors_paidSearch_wow AS metric_value_wow, adobe_byodEntryDevicePageVisitors_paidSearch_ly AS metric_value_ly, adobe_byodEntryDevicePageVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodEntryDevicePageVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryPlansPageVisitors_paidSearch' AS metric_name, adobe_byodEntryPlansPageVisitors_paidSearch AS metric_value, adobe_byodEntryPlansPageVisitors_paidSearch_wow AS metric_value_wow, adobe_byodEntryPlansPageVisitors_paidSearch_ly AS metric_value_ly, adobe_byodEntryPlansPageVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodEntryPlansPageVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOtherPageVisitors_paidSearch' AS metric_name, adobe_byodEntryOtherPageVisitors_paidSearch AS metric_value, adobe_byodEntryOtherPageVisitors_paidSearch_wow AS metric_value_wow, adobe_byodEntryOtherPageVisitors_paidSearch_ly AS metric_value_ly, adobe_byodEntryOtherPageVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodEntryOtherPageVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryGetStartedLandingPageVisitors_paidSearch' AS metric_name, adobe_byodEntryGetStartedLandingPageVisitors_paidSearch AS metric_value, adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_wow AS metric_value_wow, adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_ly AS metric_value_ly, adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodLandingPageVisitors_paidSearch' AS metric_name, adobe_byodEntryByodLandingPageVisitors_paidSearch AS metric_value, adobe_byodEntryByodLandingPageVisitors_paidSearch_wow AS metric_value_wow, adobe_byodEntryByodLandingPageVisitors_paidSearch_ly AS metric_value_ly, adobe_byodEntryByodLandingPageVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodEntryByodLandingPageVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOffersSwitchVisitors_paidSearch' AS metric_name, adobe_byodEntryOffersSwitchVisitors_paidSearch AS metric_value, adobe_byodEntryOffersSwitchVisitors_paidSearch_wow AS metric_value_wow, adobe_byodEntryOffersSwitchVisitors_paidSearch_ly AS metric_value_ly, adobe_byodEntryOffersSwitchVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodEntryOffersSwitchVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodUpvVisitors_organicSearch' AS metric_name, adobe_byodUpvVisitors_organicSearch AS metric_value, adobe_byodUpvVisitors_organicSearch_wow AS metric_value_wow, adobe_byodUpvVisitors_organicSearch_ly AS metric_value_ly, adobe_byodUpvVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodUpvVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodPageVisitors_organicSearch' AS metric_name, adobe_byodEntryByodPageVisitors_organicSearch AS metric_value, adobe_byodEntryByodPageVisitors_organicSearch_wow AS metric_value_wow, adobe_byodEntryByodPageVisitors_organicSearch_ly AS metric_value_ly, adobe_byodEntryByodPageVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodEntryByodPageVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryHomePageVisitors_organicSearch' AS metric_name, adobe_byodEntryHomePageVisitors_organicSearch AS metric_value, adobe_byodEntryHomePageVisitors_organicSearch_wow AS metric_value_wow, adobe_byodEntryHomePageVisitors_organicSearch_ly AS metric_value_ly, adobe_byodEntryHomePageVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodEntryHomePageVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryDevicePageVisitors_organicSearch' AS metric_name, adobe_byodEntryDevicePageVisitors_organicSearch AS metric_value, adobe_byodEntryDevicePageVisitors_organicSearch_wow AS metric_value_wow, adobe_byodEntryDevicePageVisitors_organicSearch_ly AS metric_value_ly, adobe_byodEntryDevicePageVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodEntryDevicePageVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryPlansPageVisitors_organicSearch' AS metric_name, adobe_byodEntryPlansPageVisitors_organicSearch AS metric_value, adobe_byodEntryPlansPageVisitors_organicSearch_wow AS metric_value_wow, adobe_byodEntryPlansPageVisitors_organicSearch_ly AS metric_value_ly, adobe_byodEntryPlansPageVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodEntryPlansPageVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOtherPageVisitors_organicSearch' AS metric_name, adobe_byodEntryOtherPageVisitors_organicSearch AS metric_value, adobe_byodEntryOtherPageVisitors_organicSearch_wow AS metric_value_wow, adobe_byodEntryOtherPageVisitors_organicSearch_ly AS metric_value_ly, adobe_byodEntryOtherPageVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodEntryOtherPageVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryGetStartedLandingPageVisitors_organicSearch' AS metric_name, adobe_byodEntryGetStartedLandingPageVisitors_organicSearch AS metric_value, adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_wow AS metric_value_wow, adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_ly AS metric_value_ly, adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodLandingPageVisitors_organicSearch' AS metric_name, adobe_byodEntryByodLandingPageVisitors_organicSearch AS metric_value, adobe_byodEntryByodLandingPageVisitors_organicSearch_wow AS metric_value_wow, adobe_byodEntryByodLandingPageVisitors_organicSearch_ly AS metric_value_ly, adobe_byodEntryByodLandingPageVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodEntryByodLandingPageVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOffersSwitchVisitors_organicSearch' AS metric_name, adobe_byodEntryOffersSwitchVisitors_organicSearch AS metric_value, adobe_byodEntryOffersSwitchVisitors_organicSearch_wow AS metric_value_wow, adobe_byodEntryOffersSwitchVisitors_organicSearch_ly AS metric_value_ly, adobe_byodEntryOffersSwitchVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodEntryOffersSwitchVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodUpvVisitors_direct' AS metric_name, adobe_byodUpvVisitors_direct AS metric_value, adobe_byodUpvVisitors_direct_wow AS metric_value_wow, adobe_byodUpvVisitors_direct_ly AS metric_value_ly, adobe_byodUpvVisitors_direct_wow_pct AS wow_pct, adobe_byodUpvVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodPageVisitors_direct' AS metric_name, adobe_byodEntryByodPageVisitors_direct AS metric_value, adobe_byodEntryByodPageVisitors_direct_wow AS metric_value_wow, adobe_byodEntryByodPageVisitors_direct_ly AS metric_value_ly, adobe_byodEntryByodPageVisitors_direct_wow_pct AS wow_pct, adobe_byodEntryByodPageVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryHomePageVisitors_direct' AS metric_name, adobe_byodEntryHomePageVisitors_direct AS metric_value, adobe_byodEntryHomePageVisitors_direct_wow AS metric_value_wow, adobe_byodEntryHomePageVisitors_direct_ly AS metric_value_ly, adobe_byodEntryHomePageVisitors_direct_wow_pct AS wow_pct, adobe_byodEntryHomePageVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryDevicePageVisitors_direct' AS metric_name, adobe_byodEntryDevicePageVisitors_direct AS metric_value, adobe_byodEntryDevicePageVisitors_direct_wow AS metric_value_wow, adobe_byodEntryDevicePageVisitors_direct_ly AS metric_value_ly, adobe_byodEntryDevicePageVisitors_direct_wow_pct AS wow_pct, adobe_byodEntryDevicePageVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryPlansPageVisitors_direct' AS metric_name, adobe_byodEntryPlansPageVisitors_direct AS metric_value, adobe_byodEntryPlansPageVisitors_direct_wow AS metric_value_wow, adobe_byodEntryPlansPageVisitors_direct_ly AS metric_value_ly, adobe_byodEntryPlansPageVisitors_direct_wow_pct AS wow_pct, adobe_byodEntryPlansPageVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOtherPageVisitors_direct' AS metric_name, adobe_byodEntryOtherPageVisitors_direct AS metric_value, adobe_byodEntryOtherPageVisitors_direct_wow AS metric_value_wow, adobe_byodEntryOtherPageVisitors_direct_ly AS metric_value_ly, adobe_byodEntryOtherPageVisitors_direct_wow_pct AS wow_pct, adobe_byodEntryOtherPageVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryGetStartedLandingPageVisitors_direct' AS metric_name, adobe_byodEntryGetStartedLandingPageVisitors_direct AS metric_value, adobe_byodEntryGetStartedLandingPageVisitors_direct_wow AS metric_value_wow, adobe_byodEntryGetStartedLandingPageVisitors_direct_ly AS metric_value_ly, adobe_byodEntryGetStartedLandingPageVisitors_direct_wow_pct AS wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodLandingPageVisitors_direct' AS metric_name, adobe_byodEntryByodLandingPageVisitors_direct AS metric_value, adobe_byodEntryByodLandingPageVisitors_direct_wow AS metric_value_wow, adobe_byodEntryByodLandingPageVisitors_direct_ly AS metric_value_ly, adobe_byodEntryByodLandingPageVisitors_direct_wow_pct AS wow_pct, adobe_byodEntryByodLandingPageVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOffersSwitchVisitors_direct' AS metric_name, adobe_byodEntryOffersSwitchVisitors_direct AS metric_value, adobe_byodEntryOffersSwitchVisitors_direct_wow AS metric_value_wow, adobe_byodEntryOffersSwitchVisitors_direct_ly AS metric_value_ly, adobe_byodEntryOffersSwitchVisitors_direct_wow_pct AS wow_pct, adobe_byodEntryOffersSwitchVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodUpvVisitors_social' AS metric_name, adobe_byodUpvVisitors_social AS metric_value, adobe_byodUpvVisitors_social_wow AS metric_value_wow, adobe_byodUpvVisitors_social_ly AS metric_value_ly, adobe_byodUpvVisitors_social_wow_pct AS wow_pct, adobe_byodUpvVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodPageVisitors_social' AS metric_name, adobe_byodEntryByodPageVisitors_social AS metric_value, adobe_byodEntryByodPageVisitors_social_wow AS metric_value_wow, adobe_byodEntryByodPageVisitors_social_ly AS metric_value_ly, adobe_byodEntryByodPageVisitors_social_wow_pct AS wow_pct, adobe_byodEntryByodPageVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryHomePageVisitors_social' AS metric_name, adobe_byodEntryHomePageVisitors_social AS metric_value, adobe_byodEntryHomePageVisitors_social_wow AS metric_value_wow, adobe_byodEntryHomePageVisitors_social_ly AS metric_value_ly, adobe_byodEntryHomePageVisitors_social_wow_pct AS wow_pct, adobe_byodEntryHomePageVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryDevicePageVisitors_social' AS metric_name, adobe_byodEntryDevicePageVisitors_social AS metric_value, adobe_byodEntryDevicePageVisitors_social_wow AS metric_value_wow, adobe_byodEntryDevicePageVisitors_social_ly AS metric_value_ly, adobe_byodEntryDevicePageVisitors_social_wow_pct AS wow_pct, adobe_byodEntryDevicePageVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryPlansPageVisitors_social' AS metric_name, adobe_byodEntryPlansPageVisitors_social AS metric_value, adobe_byodEntryPlansPageVisitors_social_wow AS metric_value_wow, adobe_byodEntryPlansPageVisitors_social_ly AS metric_value_ly, adobe_byodEntryPlansPageVisitors_social_wow_pct AS wow_pct, adobe_byodEntryPlansPageVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOtherPageVisitors_social' AS metric_name, adobe_byodEntryOtherPageVisitors_social AS metric_value, adobe_byodEntryOtherPageVisitors_social_wow AS metric_value_wow, adobe_byodEntryOtherPageVisitors_social_ly AS metric_value_ly, adobe_byodEntryOtherPageVisitors_social_wow_pct AS wow_pct, adobe_byodEntryOtherPageVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryGetStartedLandingPageVisitors_social' AS metric_name, adobe_byodEntryGetStartedLandingPageVisitors_social AS metric_value, adobe_byodEntryGetStartedLandingPageVisitors_social_wow AS metric_value_wow, adobe_byodEntryGetStartedLandingPageVisitors_social_ly AS metric_value_ly, adobe_byodEntryGetStartedLandingPageVisitors_social_wow_pct AS wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodLandingPageVisitors_social' AS metric_name, adobe_byodEntryByodLandingPageVisitors_social AS metric_value, adobe_byodEntryByodLandingPageVisitors_social_wow AS metric_value_wow, adobe_byodEntryByodLandingPageVisitors_social_ly AS metric_value_ly, adobe_byodEntryByodLandingPageVisitors_social_wow_pct AS wow_pct, adobe_byodEntryByodLandingPageVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOffersSwitchVisitors_social' AS metric_name, adobe_byodEntryOffersSwitchVisitors_social AS metric_value, adobe_byodEntryOffersSwitchVisitors_social_wow AS metric_value_wow, adobe_byodEntryOffersSwitchVisitors_social_ly AS metric_value_ly, adobe_byodEntryOffersSwitchVisitors_social_wow_pct AS wow_pct, adobe_byodEntryOffersSwitchVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodUpvVisitors_programmatic' AS metric_name, adobe_byodUpvVisitors_programmatic AS metric_value, adobe_byodUpvVisitors_programmatic_wow AS metric_value_wow, adobe_byodUpvVisitors_programmatic_ly AS metric_value_ly, adobe_byodUpvVisitors_programmatic_wow_pct AS wow_pct, adobe_byodUpvVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodPageVisitors_programmatic' AS metric_name, adobe_byodEntryByodPageVisitors_programmatic AS metric_value, adobe_byodEntryByodPageVisitors_programmatic_wow AS metric_value_wow, adobe_byodEntryByodPageVisitors_programmatic_ly AS metric_value_ly, adobe_byodEntryByodPageVisitors_programmatic_wow_pct AS wow_pct, adobe_byodEntryByodPageVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryHomePageVisitors_programmatic' AS metric_name, adobe_byodEntryHomePageVisitors_programmatic AS metric_value, adobe_byodEntryHomePageVisitors_programmatic_wow AS metric_value_wow, adobe_byodEntryHomePageVisitors_programmatic_ly AS metric_value_ly, adobe_byodEntryHomePageVisitors_programmatic_wow_pct AS wow_pct, adobe_byodEntryHomePageVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryDevicePageVisitors_programmatic' AS metric_name, adobe_byodEntryDevicePageVisitors_programmatic AS metric_value, adobe_byodEntryDevicePageVisitors_programmatic_wow AS metric_value_wow, adobe_byodEntryDevicePageVisitors_programmatic_ly AS metric_value_ly, adobe_byodEntryDevicePageVisitors_programmatic_wow_pct AS wow_pct, adobe_byodEntryDevicePageVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryPlansPageVisitors_programmatic' AS metric_name, adobe_byodEntryPlansPageVisitors_programmatic AS metric_value, adobe_byodEntryPlansPageVisitors_programmatic_wow AS metric_value_wow, adobe_byodEntryPlansPageVisitors_programmatic_ly AS metric_value_ly, adobe_byodEntryPlansPageVisitors_programmatic_wow_pct AS wow_pct, adobe_byodEntryPlansPageVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOtherPageVisitors_programmatic' AS metric_name, adobe_byodEntryOtherPageVisitors_programmatic AS metric_value, adobe_byodEntryOtherPageVisitors_programmatic_wow AS metric_value_wow, adobe_byodEntryOtherPageVisitors_programmatic_ly AS metric_value_ly, adobe_byodEntryOtherPageVisitors_programmatic_wow_pct AS wow_pct, adobe_byodEntryOtherPageVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryGetStartedLandingPageVisitors_programmatic' AS metric_name, adobe_byodEntryGetStartedLandingPageVisitors_programmatic AS metric_value, adobe_byodEntryGetStartedLandingPageVisitors_programmatic_wow AS metric_value_wow, adobe_byodEntryGetStartedLandingPageVisitors_programmatic_ly AS metric_value_ly, adobe_byodEntryGetStartedLandingPageVisitors_programmatic_wow_pct AS wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodLandingPageVisitors_programmatic' AS metric_name, adobe_byodEntryByodLandingPageVisitors_programmatic AS metric_value, adobe_byodEntryByodLandingPageVisitors_programmatic_wow AS metric_value_wow, adobe_byodEntryByodLandingPageVisitors_programmatic_ly AS metric_value_ly, adobe_byodEntryByodLandingPageVisitors_programmatic_wow_pct AS wow_pct, adobe_byodEntryByodLandingPageVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOffersSwitchVisitors_programmatic' AS metric_name, adobe_byodEntryOffersSwitchVisitors_programmatic AS metric_value, adobe_byodEntryOffersSwitchVisitors_programmatic_wow AS metric_value_wow, adobe_byodEntryOffersSwitchVisitors_programmatic_ly AS metric_value_ly, adobe_byodEntryOffersSwitchVisitors_programmatic_wow_pct AS wow_pct, adobe_byodEntryOffersSwitchVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodUpvVisitors_other' AS metric_name, adobe_byodUpvVisitors_other AS metric_value, adobe_byodUpvVisitors_other_wow AS metric_value_wow, adobe_byodUpvVisitors_other_ly AS metric_value_ly, adobe_byodUpvVisitors_other_wow_pct AS wow_pct, adobe_byodUpvVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodPageVisitors_other' AS metric_name, adobe_byodEntryByodPageVisitors_other AS metric_value, adobe_byodEntryByodPageVisitors_other_wow AS metric_value_wow, adobe_byodEntryByodPageVisitors_other_ly AS metric_value_ly, adobe_byodEntryByodPageVisitors_other_wow_pct AS wow_pct, adobe_byodEntryByodPageVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryHomePageVisitors_other' AS metric_name, adobe_byodEntryHomePageVisitors_other AS metric_value, adobe_byodEntryHomePageVisitors_other_wow AS metric_value_wow, adobe_byodEntryHomePageVisitors_other_ly AS metric_value_ly, adobe_byodEntryHomePageVisitors_other_wow_pct AS wow_pct, adobe_byodEntryHomePageVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryDevicePageVisitors_other' AS metric_name, adobe_byodEntryDevicePageVisitors_other AS metric_value, adobe_byodEntryDevicePageVisitors_other_wow AS metric_value_wow, adobe_byodEntryDevicePageVisitors_other_ly AS metric_value_ly, adobe_byodEntryDevicePageVisitors_other_wow_pct AS wow_pct, adobe_byodEntryDevicePageVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryPlansPageVisitors_other' AS metric_name, adobe_byodEntryPlansPageVisitors_other AS metric_value, adobe_byodEntryPlansPageVisitors_other_wow AS metric_value_wow, adobe_byodEntryPlansPageVisitors_other_ly AS metric_value_ly, adobe_byodEntryPlansPageVisitors_other_wow_pct AS wow_pct, adobe_byodEntryPlansPageVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOtherPageVisitors_other' AS metric_name, adobe_byodEntryOtherPageVisitors_other AS metric_value, adobe_byodEntryOtherPageVisitors_other_wow AS metric_value_wow, adobe_byodEntryOtherPageVisitors_other_ly AS metric_value_ly, adobe_byodEntryOtherPageVisitors_other_wow_pct AS wow_pct, adobe_byodEntryOtherPageVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryGetStartedLandingPageVisitors_other' AS metric_name, adobe_byodEntryGetStartedLandingPageVisitors_other AS metric_value, adobe_byodEntryGetStartedLandingPageVisitors_other_wow AS metric_value_wow, adobe_byodEntryGetStartedLandingPageVisitors_other_ly AS metric_value_ly, adobe_byodEntryGetStartedLandingPageVisitors_other_wow_pct AS wow_pct, adobe_byodEntryGetStartedLandingPageVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryByodLandingPageVisitors_other' AS metric_name, adobe_byodEntryByodLandingPageVisitors_other AS metric_value, adobe_byodEntryByodLandingPageVisitors_other_wow AS metric_value_wow, adobe_byodEntryByodLandingPageVisitors_other_ly AS metric_value_ly, adobe_byodEntryByodLandingPageVisitors_other_wow_pct AS wow_pct, adobe_byodEntryByodLandingPageVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodEntryOffersSwitchVisitors_other' AS metric_name, adobe_byodEntryOffersSwitchVisitors_other AS metric_value, adobe_byodEntryOffersSwitchVisitors_other_wow AS metric_value_wow, adobe_byodEntryOffersSwitchVisitors_other_ly AS metric_value_ly, adobe_byodEntryOffersSwitchVisitors_other_wow_pct AS wow_pct, adobe_byodEntryOffersSwitchVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodEntryPages_weekly
  ),

  -- -----------------------------------------------------------------------
  -- ADOBE BYOD OUTCOMES: 6 metrics x 7 channels — 42 rows/week
  -- -----------------------------------------------------------------------
    -- -----------------------------------------------------------------------
  -- OUTCOMES: 42 rows/week (explicit UNION ALL — every metric always
  -- produces a row, nulls included; no UNPIVOT EXCLUDE NULLS row-dropping)
  -- -----------------------------------------------------------------------
  outcomes_long AS (
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodVrChatVisitors_allChannels' AS metric_name, adobe_byodVrChatVisitors_allChannels AS metric_value, adobe_byodVrChatVisitors_allChannels_wow AS metric_value_wow, adobe_byodVrChatVisitors_allChannels_ly AS metric_value_ly, adobe_byodVrChatVisitors_allChannels_wow_pct AS wow_pct, adobe_byodVrChatVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodCallVisitors_allChannels' AS metric_name, adobe_byodCallVisitors_allChannels AS metric_value, adobe_byodCallVisitors_allChannels_wow AS metric_value_wow, adobe_byodCallVisitors_allChannels_ly AS metric_value_ly, adobe_byodCallVisitors_allChannels_wow_pct AS wow_pct, adobe_byodCallVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodStoreLocatorVisitors_allChannels' AS metric_name, adobe_byodStoreLocatorVisitors_allChannels AS metric_value, adobe_byodStoreLocatorVisitors_allChannels_wow AS metric_value_wow, adobe_byodStoreLocatorVisitors_allChannels_ly AS metric_value_ly, adobe_byodStoreLocatorVisitors_allChannels_wow_pct AS wow_pct, adobe_byodStoreLocatorVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodInternalTmoVisitors_allChannels' AS metric_name, adobe_byodInternalTmoVisitors_allChannels AS metric_value, adobe_byodInternalTmoVisitors_allChannels_wow AS metric_value_wow, adobe_byodInternalTmoVisitors_allChannels_ly AS metric_value_ly, adobe_byodInternalTmoVisitors_allChannels_wow_pct AS wow_pct, adobe_byodInternalTmoVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodBouncersVisitors_allChannels' AS metric_name, adobe_byodBouncersVisitors_allChannels AS metric_value, adobe_byodBouncersVisitors_allChannels_wow AS metric_value_wow, adobe_byodBouncersVisitors_allChannels_ly AS metric_value_ly, adobe_byodBouncersVisitors_allChannels_wow_pct AS wow_pct, adobe_byodBouncersVisitors_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodOrders_allChannels' AS metric_name, adobe_byodOrders_allChannels AS metric_value, adobe_byodOrders_allChannels_wow AS metric_value_wow, adobe_byodOrders_allChannels_ly AS metric_value_ly, adobe_byodOrders_allChannels_wow_pct AS wow_pct, adobe_byodOrders_allChannels_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodVrChatVisitors_paidSearch' AS metric_name, adobe_byodVrChatVisitors_paidSearch AS metric_value, adobe_byodVrChatVisitors_paidSearch_wow AS metric_value_wow, adobe_byodVrChatVisitors_paidSearch_ly AS metric_value_ly, adobe_byodVrChatVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodVrChatVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodCallVisitors_paidSearch' AS metric_name, adobe_byodCallVisitors_paidSearch AS metric_value, adobe_byodCallVisitors_paidSearch_wow AS metric_value_wow, adobe_byodCallVisitors_paidSearch_ly AS metric_value_ly, adobe_byodCallVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodCallVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodStoreLocatorVisitors_paidSearch' AS metric_name, adobe_byodStoreLocatorVisitors_paidSearch AS metric_value, adobe_byodStoreLocatorVisitors_paidSearch_wow AS metric_value_wow, adobe_byodStoreLocatorVisitors_paidSearch_ly AS metric_value_ly, adobe_byodStoreLocatorVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodStoreLocatorVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodInternalTmoVisitors_paidSearch' AS metric_name, adobe_byodInternalTmoVisitors_paidSearch AS metric_value, adobe_byodInternalTmoVisitors_paidSearch_wow AS metric_value_wow, adobe_byodInternalTmoVisitors_paidSearch_ly AS metric_value_ly, adobe_byodInternalTmoVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodInternalTmoVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodBouncersVisitors_paidSearch' AS metric_name, adobe_byodBouncersVisitors_paidSearch AS metric_value, adobe_byodBouncersVisitors_paidSearch_wow AS metric_value_wow, adobe_byodBouncersVisitors_paidSearch_ly AS metric_value_ly, adobe_byodBouncersVisitors_paidSearch_wow_pct AS wow_pct, adobe_byodBouncersVisitors_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodOrders_paidSearch' AS metric_name, adobe_byodOrders_paidSearch AS metric_value, adobe_byodOrders_paidSearch_wow AS metric_value_wow, adobe_byodOrders_paidSearch_ly AS metric_value_ly, adobe_byodOrders_paidSearch_wow_pct AS wow_pct, adobe_byodOrders_paidSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodVrChatVisitors_organicSearch' AS metric_name, adobe_byodVrChatVisitors_organicSearch AS metric_value, adobe_byodVrChatVisitors_organicSearch_wow AS metric_value_wow, adobe_byodVrChatVisitors_organicSearch_ly AS metric_value_ly, adobe_byodVrChatVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodVrChatVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodCallVisitors_organicSearch' AS metric_name, adobe_byodCallVisitors_organicSearch AS metric_value, adobe_byodCallVisitors_organicSearch_wow AS metric_value_wow, adobe_byodCallVisitors_organicSearch_ly AS metric_value_ly, adobe_byodCallVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodCallVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodStoreLocatorVisitors_organicSearch' AS metric_name, adobe_byodStoreLocatorVisitors_organicSearch AS metric_value, adobe_byodStoreLocatorVisitors_organicSearch_wow AS metric_value_wow, adobe_byodStoreLocatorVisitors_organicSearch_ly AS metric_value_ly, adobe_byodStoreLocatorVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodStoreLocatorVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodInternalTmoVisitors_organicSearch' AS metric_name, adobe_byodInternalTmoVisitors_organicSearch AS metric_value, adobe_byodInternalTmoVisitors_organicSearch_wow AS metric_value_wow, adobe_byodInternalTmoVisitors_organicSearch_ly AS metric_value_ly, adobe_byodInternalTmoVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodInternalTmoVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodBouncersVisitors_organicSearch' AS metric_name, adobe_byodBouncersVisitors_organicSearch AS metric_value, adobe_byodBouncersVisitors_organicSearch_wow AS metric_value_wow, adobe_byodBouncersVisitors_organicSearch_ly AS metric_value_ly, adobe_byodBouncersVisitors_organicSearch_wow_pct AS wow_pct, adobe_byodBouncersVisitors_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodOrders_organicSearch' AS metric_name, adobe_byodOrders_organicSearch AS metric_value, adobe_byodOrders_organicSearch_wow AS metric_value_wow, adobe_byodOrders_organicSearch_ly AS metric_value_ly, adobe_byodOrders_organicSearch_wow_pct AS wow_pct, adobe_byodOrders_organicSearch_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodVrChatVisitors_direct' AS metric_name, adobe_byodVrChatVisitors_direct AS metric_value, adobe_byodVrChatVisitors_direct_wow AS metric_value_wow, adobe_byodVrChatVisitors_direct_ly AS metric_value_ly, adobe_byodVrChatVisitors_direct_wow_pct AS wow_pct, adobe_byodVrChatVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodCallVisitors_direct' AS metric_name, adobe_byodCallVisitors_direct AS metric_value, adobe_byodCallVisitors_direct_wow AS metric_value_wow, adobe_byodCallVisitors_direct_ly AS metric_value_ly, adobe_byodCallVisitors_direct_wow_pct AS wow_pct, adobe_byodCallVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodStoreLocatorVisitors_direct' AS metric_name, adobe_byodStoreLocatorVisitors_direct AS metric_value, adobe_byodStoreLocatorVisitors_direct_wow AS metric_value_wow, adobe_byodStoreLocatorVisitors_direct_ly AS metric_value_ly, adobe_byodStoreLocatorVisitors_direct_wow_pct AS wow_pct, adobe_byodStoreLocatorVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodInternalTmoVisitors_direct' AS metric_name, adobe_byodInternalTmoVisitors_direct AS metric_value, adobe_byodInternalTmoVisitors_direct_wow AS metric_value_wow, adobe_byodInternalTmoVisitors_direct_ly AS metric_value_ly, adobe_byodInternalTmoVisitors_direct_wow_pct AS wow_pct, adobe_byodInternalTmoVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodBouncersVisitors_direct' AS metric_name, adobe_byodBouncersVisitors_direct AS metric_value, adobe_byodBouncersVisitors_direct_wow AS metric_value_wow, adobe_byodBouncersVisitors_direct_ly AS metric_value_ly, adobe_byodBouncersVisitors_direct_wow_pct AS wow_pct, adobe_byodBouncersVisitors_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodOrders_direct' AS metric_name, adobe_byodOrders_direct AS metric_value, adobe_byodOrders_direct_wow AS metric_value_wow, adobe_byodOrders_direct_ly AS metric_value_ly, adobe_byodOrders_direct_wow_pct AS wow_pct, adobe_byodOrders_direct_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodVrChatVisitors_social' AS metric_name, adobe_byodVrChatVisitors_social AS metric_value, adobe_byodVrChatVisitors_social_wow AS metric_value_wow, adobe_byodVrChatVisitors_social_ly AS metric_value_ly, adobe_byodVrChatVisitors_social_wow_pct AS wow_pct, adobe_byodVrChatVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodCallVisitors_social' AS metric_name, adobe_byodCallVisitors_social AS metric_value, adobe_byodCallVisitors_social_wow AS metric_value_wow, adobe_byodCallVisitors_social_ly AS metric_value_ly, adobe_byodCallVisitors_social_wow_pct AS wow_pct, adobe_byodCallVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodStoreLocatorVisitors_social' AS metric_name, adobe_byodStoreLocatorVisitors_social AS metric_value, adobe_byodStoreLocatorVisitors_social_wow AS metric_value_wow, adobe_byodStoreLocatorVisitors_social_ly AS metric_value_ly, adobe_byodStoreLocatorVisitors_social_wow_pct AS wow_pct, adobe_byodStoreLocatorVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodInternalTmoVisitors_social' AS metric_name, adobe_byodInternalTmoVisitors_social AS metric_value, adobe_byodInternalTmoVisitors_social_wow AS metric_value_wow, adobe_byodInternalTmoVisitors_social_ly AS metric_value_ly, adobe_byodInternalTmoVisitors_social_wow_pct AS wow_pct, adobe_byodInternalTmoVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodBouncersVisitors_social' AS metric_name, adobe_byodBouncersVisitors_social AS metric_value, adobe_byodBouncersVisitors_social_wow AS metric_value_wow, adobe_byodBouncersVisitors_social_ly AS metric_value_ly, adobe_byodBouncersVisitors_social_wow_pct AS wow_pct, adobe_byodBouncersVisitors_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodOrders_social' AS metric_name, adobe_byodOrders_social AS metric_value, adobe_byodOrders_social_wow AS metric_value_wow, adobe_byodOrders_social_ly AS metric_value_ly, adobe_byodOrders_social_wow_pct AS wow_pct, adobe_byodOrders_social_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodVrChatVisitors_programmatic' AS metric_name, adobe_byodVrChatVisitors_programmatic AS metric_value, adobe_byodVrChatVisitors_programmatic_wow AS metric_value_wow, adobe_byodVrChatVisitors_programmatic_ly AS metric_value_ly, adobe_byodVrChatVisitors_programmatic_wow_pct AS wow_pct, adobe_byodVrChatVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodCallVisitors_programmatic' AS metric_name, adobe_byodCallVisitors_programmatic AS metric_value, adobe_byodCallVisitors_programmatic_wow AS metric_value_wow, adobe_byodCallVisitors_programmatic_ly AS metric_value_ly, adobe_byodCallVisitors_programmatic_wow_pct AS wow_pct, adobe_byodCallVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodStoreLocatorVisitors_programmatic' AS metric_name, adobe_byodStoreLocatorVisitors_programmatic AS metric_value, adobe_byodStoreLocatorVisitors_programmatic_wow AS metric_value_wow, adobe_byodStoreLocatorVisitors_programmatic_ly AS metric_value_ly, adobe_byodStoreLocatorVisitors_programmatic_wow_pct AS wow_pct, adobe_byodStoreLocatorVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodInternalTmoVisitors_programmatic' AS metric_name, adobe_byodInternalTmoVisitors_programmatic AS metric_value, adobe_byodInternalTmoVisitors_programmatic_wow AS metric_value_wow, adobe_byodInternalTmoVisitors_programmatic_ly AS metric_value_ly, adobe_byodInternalTmoVisitors_programmatic_wow_pct AS wow_pct, adobe_byodInternalTmoVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodBouncersVisitors_programmatic' AS metric_name, adobe_byodBouncersVisitors_programmatic AS metric_value, adobe_byodBouncersVisitors_programmatic_wow AS metric_value_wow, adobe_byodBouncersVisitors_programmatic_ly AS metric_value_ly, adobe_byodBouncersVisitors_programmatic_wow_pct AS wow_pct, adobe_byodBouncersVisitors_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodOrders_programmatic' AS metric_name, adobe_byodOrders_programmatic AS metric_value, adobe_byodOrders_programmatic_wow AS metric_value_wow, adobe_byodOrders_programmatic_ly AS metric_value_ly, adobe_byodOrders_programmatic_wow_pct AS wow_pct, adobe_byodOrders_programmatic_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodVrChatVisitors_other' AS metric_name, adobe_byodVrChatVisitors_other AS metric_value, adobe_byodVrChatVisitors_other_wow AS metric_value_wow, adobe_byodVrChatVisitors_other_ly AS metric_value_ly, adobe_byodVrChatVisitors_other_wow_pct AS wow_pct, adobe_byodVrChatVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodCallVisitors_other' AS metric_name, adobe_byodCallVisitors_other AS metric_value, adobe_byodCallVisitors_other_wow AS metric_value_wow, adobe_byodCallVisitors_other_ly AS metric_value_ly, adobe_byodCallVisitors_other_wow_pct AS wow_pct, adobe_byodCallVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodStoreLocatorVisitors_other' AS metric_name, adobe_byodStoreLocatorVisitors_other AS metric_value, adobe_byodStoreLocatorVisitors_other_wow AS metric_value_wow, adobe_byodStoreLocatorVisitors_other_ly AS metric_value_ly, adobe_byodStoreLocatorVisitors_other_wow_pct AS wow_pct, adobe_byodStoreLocatorVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodInternalTmoVisitors_other' AS metric_name, adobe_byodInternalTmoVisitors_other AS metric_value, adobe_byodInternalTmoVisitors_other_wow AS metric_value_wow, adobe_byodInternalTmoVisitors_other_ly AS metric_value_ly, adobe_byodInternalTmoVisitors_other_wow_pct AS wow_pct, adobe_byodInternalTmoVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodBouncersVisitors_other' AS metric_name, adobe_byodBouncersVisitors_other AS metric_value, adobe_byodBouncersVisitors_other_wow AS metric_value_wow, adobe_byodBouncersVisitors_other_ly AS metric_value_ly, adobe_byodBouncersVisitors_other_wow_pct AS wow_pct, adobe_byodBouncersVisitors_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
      UNION ALL
      SELECT week_sun_to_sat, data_source, max_data_date, 'adobe_byodOrders_other' AS metric_name, adobe_byodOrders_other AS metric_value, adobe_byodOrders_other_wow AS metric_value_wow, adobe_byodOrders_other_ly AS metric_value_ly, adobe_byodOrders_other_wow_pct AS wow_pct, adobe_byodOrders_other_yoy_pct AS yoy_pct FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_adobeByodOutcomes_weekly
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
      -- SA360
      SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM sa360_long
      UNION ALL
      -- GSC
      SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM gsc_long
      UNION ALL
      -- Trends index
      SELECT week_sun_to_sat, data_source, channel, max_data_date, CAST(NULL AS STRING), CAST(NULL AS STRING), metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM trends_index_long
      UNION ALL
      -- Trends keywords
      SELECT week_sun_to_sat, data_source, channel, max_data_date, dimension_name, dimension_value, metric_name, metric_value, metric_value_wow, metric_value_ly, wow_pct, yoy_pct FROM trends_keywords_long
      UNION ALL
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
              ELSE 'Unknown'
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
              ELSE 'Unknown'
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
              ELSE 'Unknown'
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