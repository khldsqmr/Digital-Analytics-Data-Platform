-- ============================================================================
-- MIP ONE-TIME PROSPECT/BASE FILTER REBUILD
-- EXISTING TABLES ONLY; NO BRONZE/SILVER REBUILD.
--
-- COVERAGE:
--   Latest smoke-test week: 2026-10-11 .. 2026-10-17
--   Previous five weeks:     2026-09-06 .. 2026-10-10
--   Combined: six reporting weeks, each rebuilt exactly once.
--
-- REQUIRED BEFORE RUNNING THIS FILE:
--   1) Run 00_ONE_TIME_SCHEMA_MIGRATION.sql.
--   2) Deploy updated procedures 01..06.
--   3) Deploy compatibility procedures 07..13 BEFORE rebuilding Gold 01/02.
--      Those procedures keep non-Overview consumers pinned to
--      filterProspectVsBase='All'. They do not need historical rebuilds.
--
-- RATIONALE FOR STAGING:
--   Rebuild the newest week first and validate it before the heavier five-week
--   historical Gold 02 run. No week is processed twice.
-- ============================================================================

-- ==========================================================================
-- STAGE A: VALIDATE-ONLY FOR LATEST WEEK (cheap preflight, no writes)
-- ==========================================================================
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>TRUE
);

-- ==========================================================================
-- STAGE B: BUILD LATEST WEEK
-- Analytical Gold first, then App Gold.
-- ==========================================================================
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(
    p_asOfDate=>DATE '2026-10-17',p_weeksToRebuild=>1,p_validateOnly=>FALSE
);

-- ==========================================================================
-- STAGE C: LATEST-WEEK SMOKE CHECKS.
-- All must pass before proceeding to the previous five weeks.
-- ==========================================================================
-- C1. New contexts present. Do NOT hardcode which source states must exist;
--     the source contract currently permits Customer/Care/Prospect/Unknown.
SELECT 'Gold01' AS objectName,filterProspectVsBase,COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
WHERE targetWeekStartDate=DATE '2026-10-11'
GROUP BY filterProspectVsBase
UNION ALL
SELECT 'Gold02',filterProspectVsBase,COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate=DATE '2026-10-11'
GROUP BY filterProspectVsBase
ORDER BY objectName,filterProspectVsBase;

-- C2. Expected zero duplicate rows.
SELECT targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
WHERE targetWeekStartDate=DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName
HAVING COUNT(*)>1;

SELECT targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,breakoutType,breakoutValue,metricName,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate=DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,breakoutType,breakoutValue,metricName
HAVING COUNT(*)>1;

-- C3. Expected zero: segmented contexts must not expose the self-breakout.
SELECT filterProspectVsBase,breakoutType,COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate=DATE '2026-10-11'
  AND filterProspectVsBase<>'All'
  AND breakoutType='prospectVsBase'
GROUP BY filterProspectVsBase,breakoutType;

-- C4. Expected zero: only All may use the existing unsegmented forecast.
SELECT filterProspectVsBase,COUNT(*) AS invalidForecastRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
WHERE targetWeekStartDate=DATE '2026-10-11'
  AND filterProspectVsBase<>'All'
  AND forecastValue IS NOT NULL
GROUP BY filterProspectVsBase;

-- ==========================================================================
-- STAGE D: VALIDATE-ONLY FOR PREVIOUS FIVE WEEKS
-- p_asOfDate=2026-10-10 resolves weekTo=2026-10-04; five target weeks are
-- 2026-09-06, 09-13, 09-20, 09-27, 10-04.
-- ==========================================================================
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>TRUE
);

-- ==========================================================================
-- STAGE E: BUILD PREVIOUS FIVE WEEKS
-- ==========================================================================
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewCards_wide(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewTrend_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE
);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewConversionFunnel_long(
    p_asOfDate=>DATE '2026-10-10',p_weeksToRebuild=>5,p_validateOnly=>FALSE
);

-- Run 15_POST_MIGRATION_VALIDATION.sql after Stage E.
