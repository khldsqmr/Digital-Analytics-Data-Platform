-- ============================================================================
-- MIP POST-MIGRATION VALIDATION: GLOBAL PROSPECT/BASE OVERVIEW FILTER
-- Scope: six rebuilt reporting weeks 2026-09-06 .. 2026-10-11.
-- Expected failures are explicitly called out below.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- 1. Schema check: expected exactly 6 rows.
-- ---------------------------------------------------------------------------
SELECT table_name,column_name,data_type,ordinal_position
FROM prdrzranalytics.information_schema.columns
WHERE lower(table_schema)='lab42'
  AND lower(column_name)='filterprospectvsbase'
  AND lower(table_name) IN (
      'sdi_tbl_mip_gold_overviewmetricingredientsbyweek_long',
      'sdi_tbl_mip_gold_breakoutmetricingredientsbyweek_long',
      'sdi_tbl_mip_gold_appoverviewcards_wide',
      'sdi_tbl_mip_gold_appoverviewtrend_long',
      'sdi_tbl_mip_gold_appoverviewtoplinemovers_long',
      'sdi_tbl_mip_gold_appoverviewconversionfunnel_long'
  )
ORDER BY table_name;

-- ---------------------------------------------------------------------------
-- 2. NULL-context check in rebuilt scope: every nullFilterRows must be 0.
-- ---------------------------------------------------------------------------
SELECT 'goldOverview' AS objectName,COUNT_IF(filterProspectVsBase IS NULL) AS nullFilterRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'goldBreakout',COUNT_IF(filterProspectVsBase IS NULL)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'appOverviewCards',COUNT_IF(filterProspectVsBase IS NULL)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'appOverviewTrend',COUNT_IF(filterProspectVsBase IS NULL)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'appOverviewToplineMovers',COUNT_IF(filterProspectVsBase IS NULL)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'appOverviewConversionFunnel',COUNT_IF(filterProspectVsBase IS NULL)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
ORDER BY objectName;

-- ---------------------------------------------------------------------------
-- 3. Context inventory. 'All' must exist for every rebuilt week.
--    Other values are data-driven from weekly Silver and currently may include
--    Customer / Care / Prospect / Unknown.
-- ---------------------------------------------------------------------------
SELECT targetWeekStartDate,filterProspectVsBase,COUNT(*) AS overviewRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterProspectVsBase
ORDER BY targetWeekStartDate,filterProspectVsBase;

-- Expected zero rows: any rebuilt week missing All.
WITH expectedWeeks AS (
    SELECT explode(sequence(DATE '2026-09-06',DATE '2026-10-11',INTERVAL 7 DAYS)) AS weekStartDate
)
SELECT e.weekStartDate
FROM expectedWeeks e
LEFT ANTI JOIN (
    SELECT DISTINCT targetWeekStartDate
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
    WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
      AND filterProspectVsBase='All'
) a ON a.targetWeekStartDate=e.weekStartDate;

-- ---------------------------------------------------------------------------
-- 4. Analytical Gold uniqueness: both expected zero rows.
-- ---------------------------------------------------------------------------
SELECT targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName
HAVING COUNT(*)>1
ORDER BY rowCount DESC;

SELECT targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,breakoutType,breakoutValue,metricName,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,breakoutType,breakoutValue,metricName
HAVING COUNT(*)>1
ORDER BY rowCount DESC;

-- ---------------------------------------------------------------------------
-- 5. App Gold uniqueness: all four expected zero rows.
-- ---------------------------------------------------------------------------
SELECT targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName
HAVING COUNT(*)>1;

SELECT targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName,comparisonType,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName,comparisonType
HAVING COUNT(*)>1;

SELECT targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName,comparisonType,breakoutType,breakoutValue,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,metricName,comparisonType,breakoutType,breakoutValue
HAVING COUNT(*)>1;

SELECT targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,comparisonType,metricName,COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterLob,filterPlatform,filterProspectVsBase,comparisonType,metricName
HAVING COUNT(*)>1;

-- ---------------------------------------------------------------------------
-- 6. Gold Overview All-context reconciliation to weekly Silver for ALL count
--    ingredients. Expected maxAbsoluteDiff = 0 for every week/metric.
-- ---------------------------------------------------------------------------
WITH silverWide AS (
    SELECT
        weekStartDate,
        SUM(nbv) AS nbv,
        SUM(sessionCount) AS sessionCount,
        SUM(pageViews) AS pageViews,
        SUM(nbvBuyFlow) AS nbvBuyFlow,
        SUM(nbvConfigure) AS nbvConfigure,
        SUM(nbvCheckoutStart) AS nbvCheckoutStart,
        SUM(orders) AS orders,
        SUM(ordersAcquisition) AS ordersAcquisition,
        SUM(ordersBase) AS ordersBase,
        SUM(ordersUnassisted) AS ordersUnassisted,
        SUM(ordersAssisted) AS ordersAssisted,
        SUM(vrCalls) AS vrCalls,
        SUM(vrChats) AS vrChats,
        SUM(storeLocator) AS storeLocator,
        SUM(orderCount) AS orderCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
    WHERE weekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
    GROUP BY weekStartDate
),
silverLong AS (
    SELECT weekStartDate,metricName,metricValue
    FROM silverWide
    LATERAL VIEW stack(
        15,
        'nbv',cast(nbv AS DOUBLE),
        'sessionCount',cast(sessionCount AS DOUBLE),
        'pageViews',cast(pageViews AS DOUBLE),
        'nbvBuyFlow',cast(nbvBuyFlow AS DOUBLE),
        'nbvConfigure',cast(nbvConfigure AS DOUBLE),
        'nbvCheckoutStart',cast(nbvCheckoutStart AS DOUBLE),
        'orders',cast(orders AS DOUBLE),
        'ordersAcquisition',cast(ordersAcquisition AS DOUBLE),
        'ordersBase',cast(ordersBase AS DOUBLE),
        'ordersUnassisted',cast(ordersUnassisted AS DOUBLE),
        'ordersAssisted',cast(ordersAssisted AS DOUBLE),
        'vrCalls',cast(vrCalls AS DOUBLE),
        'vrChats',cast(vrChats AS DOUBLE),
        'storeLocator',cast(storeLocator AS DOUBLE),
        'orderCount',cast(orderCount AS DOUBLE)
    ) s AS metricName,metricValue
),goldAll AS (
    SELECT targetWeekStartDate AS weekStartDate,metricName,thisWeekNumerator
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
    WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
      AND filterLob='All'
      AND filterPlatform='All'
      AND filterProspectVsBase='All'
      AND metricKind='count'
)
SELECT
    s.weekStartDate,
    s.metricName,
    s.metricValue AS silverValue,
    g.thisWeekNumerator AS goldAllValue,
    coalesce(g.thisWeekNumerator,0D)-coalesce(s.metricValue,0D) AS absoluteDiff
FROM silverLong s
LEFT JOIN goldAll g
  ON g.weekStartDate=s.weekStartDate
 AND g.metricName=s.metricName
WHERE abs(coalesce(g.thisWeekNumerator,0D)-coalesce(s.metricValue,0D))>0.000001
   OR g.metricName IS NULL
ORDER BY s.weekStartDate,s.metricName;

-- ---------------------------------------------------------------------------
-- 7. Segment partition reconciliation for count ingredients.
--    Expected zero rows: sum(non-All contexts) must equal All exactly.
-- ---------------------------------------------------------------------------
WITH g AS (
    SELECT
        targetWeekStartDate,
        metricName,
        MAX(CASE WHEN filterProspectVsBase='All' THEN thisWeekNumerator END) AS allValue,
        SUM(CASE WHEN filterProspectVsBase<>'All' THEN coalesce(thisWeekNumerator,0D) ELSE 0D END) AS segmentSum
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
    WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
      AND filterLob='All'
      AND filterPlatform='All'
      AND metricKind='count'
    GROUP BY targetWeekStartDate,metricName
)
SELECT *,segmentSum-allValue AS diff
FROM g
WHERE abs(coalesce(segmentSum,0D)-coalesce(allValue,0D))>0.000001
ORDER BY targetWeekStartDate,metricName;

-- ---------------------------------------------------------------------------
-- 8. Gold 02 All-context reconciliation against the pre-existing partition
--    semantics: for each breakout/metric, All still reconciles to Gold01 All
--    for additive count metrics when summing raw breakout values.
--    NOTE: only valid for additive raw breakout rows; peer/rank fields are not summed.
--    Expected zero rows.
-- ---------------------------------------------------------------------------
WITH b AS (
    SELECT targetWeekStartDate,breakoutType,metricName,SUM(thisWeekNumerator) AS breakoutTotal
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
    WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
      AND filterLob='All' AND filterPlatform='All' AND filterProspectVsBase='All'
      AND metricKind='count'
    GROUP BY targetWeekStartDate,breakoutType,metricName
),o AS (
    SELECT targetWeekStartDate,metricName,thisWeekNumerator AS overviewTotal
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
    WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
      AND filterLob='All' AND filterPlatform='All' AND filterProspectVsBase='All'
      AND metricKind='count'
)
SELECT b.*,o.overviewTotal,b.breakoutTotal-o.overviewTotal AS diff
FROM b
JOIN o USING(targetWeekStartDate,metricName)
WHERE abs(coalesce(b.breakoutTotal,0D)-coalesce(o.overviewTotal,0D))>0.000001
ORDER BY targetWeekStartDate,breakoutType,metricName;

-- ---------------------------------------------------------------------------
-- 9. Self-breakout suppression for segmented contexts: expected zero rows.
-- ---------------------------------------------------------------------------
SELECT 'goldBreakout' AS objectName,filterProspectVsBase,COUNT(*) AS invalidRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
  AND filterProspectVsBase<>'All'
  AND breakoutType='prospectVsBase'
GROUP BY filterProspectVsBase
UNION ALL
SELECT 'appOverviewToplineMovers',filterProspectVsBase,COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
  AND filterProspectVsBase<>'All'
  AND breakoutType='prospectVsBase'
GROUP BY filterProspectVsBase;

-- ---------------------------------------------------------------------------
-- 10. Forecast isolation: segmented Cards must not reuse unsegmented forecasts.
--     Expected zero rows.
-- ---------------------------------------------------------------------------
SELECT targetWeekStartDate,filterProspectVsBase,metricName,forecastValue
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
  AND filterProspectVsBase<>'All'
  AND forecastValue IS NOT NULL
ORDER BY targetWeekStartDate,filterProspectVsBase,metricName;

-- ---------------------------------------------------------------------------
-- 11. Six-week coverage summary for all affected tables.
-- ---------------------------------------------------------------------------
SELECT 'goldOverview' AS objectName,COUNT(DISTINCT targetWeekStartDate) AS weeks,MIN(targetWeekStartDate) AS minWeek,MAX(targetWeekStartDate) AS maxWeek,COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'goldBreakout',COUNT(DISTINCT targetWeekStartDate),MIN(targetWeekStartDate),MAX(targetWeekStartDate),COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'appOverviewCards',COUNT(DISTINCT targetWeekStartDate),MIN(targetWeekStartDate),MAX(targetWeekStartDate),COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'appOverviewTrend',COUNT(DISTINCT targetWeekStartDate),MIN(targetWeekStartDate),MAX(targetWeekStartDate),COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'appOverviewToplineMovers',COUNT(DISTINCT targetWeekStartDate),MIN(targetWeekStartDate),MAX(targetWeekStartDate),COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
UNION ALL
SELECT 'appOverviewConversionFunnel',COUNT(DISTINCT targetWeekStartDate),MIN(targetWeekStartDate),MAX(targetWeekStartDate),COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
ORDER BY objectName;
