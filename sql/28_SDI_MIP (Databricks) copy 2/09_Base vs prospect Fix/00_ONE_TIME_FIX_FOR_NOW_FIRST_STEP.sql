-- ============================================================================
-- FILE  : 00_ONE_TIME_ADD_PROSPECT_BASE_FILTER_AS_ALL.sql
-- PURPOSE:
--   ONE-TIME schema bridge for the future global Prospect/Base filter.
--
-- CURRENT-STATE CONTRACT:
--   - All rows currently persisted in the affected Gold/App tables represent the
--     unfiltered / total population.
--   - Therefore the correct interim value for every existing row is 'All'.
--
-- FUTURE CONTRACT:
--   filterProspectVsBase values will be:
--       All | Base | Prospect | Unknown
--
-- IMPORTANT:
--   - Run this file ONCE before deploying the revised Prospect/Base-aware procedures.
--   - This does NOT rebuild Bronze or Silver.
--   - This does NOT change any current metric values.
--   - It only adds one nullable STRING column and initializes existing rows to 'All'.
--   - After the revised procedures are deployed and the six Gold/App weeks are
--     rebuilt, the same column will contain the additional Base/Prospect/Unknown
--     contexts as well.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. ANALYTICAL GOLD 01 - OVERVIEW
-- ----------------------------------------------------------------------------
ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
ADD COLUMNS (
    filterProspectVsBase STRING
    COMMENT 'Global Prospect/Base filter context. Interim existing rows = All; future values = All|Base|Prospect|Unknown.'
    AFTER filterPlatform
);

UPDATE prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
SET filterProspectVsBase='All'
WHERE filterProspectVsBase IS NULL;

-- ----------------------------------------------------------------------------
-- 2. ANALYTICAL GOLD 02 - BREAKOUTS
-- Required so Overview Topline Movers can eventually be evaluated inside the
-- same global Prospect/Base filter context as the Overview topline.
-- ----------------------------------------------------------------------------
ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
ADD COLUMNS (
    filterProspectVsBase STRING
    COMMENT 'Global Prospect/Base filter context. Interim existing rows = All; future values = All|Base|Prospect|Unknown.'
    AFTER filterPlatform
);

UPDATE prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
SET filterProspectVsBase='All'
WHERE filterProspectVsBase IS NULL;

-- ----------------------------------------------------------------------------
-- 3. APP GOLD 01 - OVERVIEW CARDS
-- ----------------------------------------------------------------------------
ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
ADD COLUMNS (
    filterProspectVsBase STRING
    COMMENT 'Global Prospect/Base filter context. Interim existing rows = All; future values = All|Base|Prospect|Unknown.'
    AFTER filterPlatform
);

UPDATE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
SET filterProspectVsBase='All'
WHERE filterProspectVsBase IS NULL;

-- ----------------------------------------------------------------------------
-- 4. APP GOLD 02 - OVERVIEW TREND
-- ----------------------------------------------------------------------------
ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
ADD COLUMNS (
    filterProspectVsBase STRING
    COMMENT 'Global Prospect/Base filter context. Interim existing rows = All; future values = All|Base|Prospect|Unknown.'
    AFTER filterPlatform
);

UPDATE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
SET filterProspectVsBase='All'
WHERE filterProspectVsBase IS NULL;

-- ----------------------------------------------------------------------------
-- 5. APP GOLD 03 - OVERVIEW TOPLINE MOVERS
-- ----------------------------------------------------------------------------
ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
ADD COLUMNS (
    filterProspectVsBase STRING
    COMMENT 'Global Prospect/Base filter context. Interim existing rows = All; future values = All|Base|Prospect|Unknown.'
    AFTER filterPlatform
);

UPDATE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
SET filterProspectVsBase='All'
WHERE filterProspectVsBase IS NULL;

-- ----------------------------------------------------------------------------
-- 6. APP GOLD 04 - OVERVIEW CONVERSION FUNNEL
-- ----------------------------------------------------------------------------
ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long
ADD COLUMNS (
    filterProspectVsBase STRING
    COMMENT 'Global Prospect/Base filter context. Interim existing rows = All; future values = All|Base|Prospect|Unknown.'
    AFTER filterPlatform
);

UPDATE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long
SET filterProspectVsBase='All'
WHERE filterProspectVsBase IS NULL;

-- ============================================================================
-- 7. POST-MIGRATION VALIDATION
-- Expected for all six tables immediately after this one-time bridge:
--   nullRows = 0
--   nonAllRows = 0
-- ============================================================================

SELECT 'goldOverview' AS objectName,
       COUNT(*) AS rowCount,
       COUNT_IF(filterProspectVsBase IS NULL) AS nullRows,
       COUNT_IF(filterProspectVsBase<>'All') AS nonAllRows,
       CONCAT_WS(',',SORT_ARRAY(COLLECT_SET(filterProspectVsBase))) AS filterValues
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
UNION ALL
SELECT 'goldBreakouts',COUNT(*),COUNT_IF(filterProspectVsBase IS NULL),COUNT_IF(filterProspectVsBase<>'All'),CONCAT_WS(',',SORT_ARRAY(COLLECT_SET(filterProspectVsBase)))
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
UNION ALL
SELECT 'appOverviewCards',COUNT(*),COUNT_IF(filterProspectVsBase IS NULL),COUNT_IF(filterProspectVsBase<>'All'),CONCAT_WS(',',SORT_ARRAY(COLLECT_SET(filterProspectVsBase)))
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
UNION ALL
SELECT 'appOverviewTrend',COUNT(*),COUNT_IF(filterProspectVsBase IS NULL),COUNT_IF(filterProspectVsBase<>'All'),CONCAT_WS(',',SORT_ARRAY(COLLECT_SET(filterProspectVsBase)))
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
UNION ALL
SELECT 'appOverviewToplineMovers',COUNT(*),COUNT_IF(filterProspectVsBase IS NULL),COUNT_IF(filterProspectVsBase<>'All'),CONCAT_WS(',',SORT_ARRAY(COLLECT_SET(filterProspectVsBase)))
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
UNION ALL
SELECT 'appOverviewConversionFunnel',COUNT(*),COUNT_IF(filterProspectVsBase IS NULL),COUNT_IF(filterProspectVsBase<>'All'),CONCAT_WS(',',SORT_ARRAY(COLLECT_SET(filterProspectVsBase)))
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long;

-- ============================================================================
-- 8. API / FRONTEND INTERIM CONTRACT
-- Until the Prospect/Base-aware rebuild is deployed, frontend/API queries may use:
--
--   AND filterProspectVsBase = 'All'
--
-- After the rebuild, the same field supports:
--   All | Base | Prospect | Unknown
--
-- No API column rename will be required later.
-- ============================================================================
