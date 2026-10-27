-- STAGING ISOLATION: CREATE AS SHALLOW CLONE, WITHOUT ALTERING ANY PRODUCTION TABLE.
-- Prerequisite: production six Gold/App tables already have filterProspectVsBase.
-- Do NOT rerun after staging has data: existing staging tables will remain unchanged
-- unless intentionally dropped in a separate, approved operation.
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long_stg SHALLOW CLONE prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long;
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long_stg SHALLOW CLONE prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long;
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide_stg SHALLOW CLONE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide;
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long_stg SHALLOW CLONE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long;
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long_stg SHALLOW CLONE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long;
CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long_stg SHALLOW CLONE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long;
