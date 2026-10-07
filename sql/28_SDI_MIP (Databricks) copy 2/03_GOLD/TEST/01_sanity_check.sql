-- ============================================================================
-- MIP GOLD - EXECUTION + SANITY CHECKS
-- TEST DATE: 2026-09-28
--
-- Reporting week:
--   weekStartDate = 2026-09-27
--   weekEndDate   = 2026-10-03
--
-- CURRENT DEVELOPMENT STATE:
-- Only Sep 28 daily data may currently exist, so this is a PARTIAL WEEK.
--
-- EXECUTION ORDER:
--
--   GOLD 01 - Overview Metric Ingredients
--       ↓
--   GOLD 02 - Breakout Metric Ingredients
--       ↓
--   GOLD 03 - Crosstab Metric Ingredients
--       ↓
--   GOLD 04 - Explore Session/PageCategory Wide
--
--   GOLD 05 = forecast target table only.
--             No actuals procedure to execute.
-- ============================================================================



-- ############################################################################
-- GOLD 01
-- OVERVIEW METRIC INGREDIENTS BY WEEK - LONG
-- ############################################################################


-- ============================================================================
-- 01A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);


-- ============================================================================
-- 01B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);


-- ============================================================================
-- 01C. VALIDATION 1
-- Basic population.
--
-- One target week should have one row per active metric
-- for the current All / All filter context.
-- ============================================================================

SELECT
    targetWeekStartDate,
    targetWeekEndDate,

    COUNT(*) AS rowCount,
    COUNT(DISTINCT metricName) AS distinctMetrics,

    MIN(goldProcessedAt) AS minProcessedAt,
    MAX(goldProcessedAt) AS maxProcessedAt

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

WHERE targetWeekStartDate = DATE '2026-09-27'

GROUP BY
    targetWeekStartDate,
    targetWeekEndDate;


-- ============================================================================
-- 01D. VALIDATION 2
-- Grain check.
--
-- Expected:
-- No rows.
-- ============================================================================

SELECT
    targetWeekStartDate,
    filterLob,
    filterPlatform,
    metricName,
    COUNT(*) AS rowCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

WHERE targetWeekStartDate = DATE '2026-09-27'

GROUP BY
    targetWeekStartDate,
    filterLob,
    filterPlatform,
    metricName

HAVING COUNT(*) > 1

ORDER BY rowCount DESC;


-- ============================================================================
-- 01E. VALIDATION 3
-- Metric ingredient sanity.
--
-- Count metrics:
--   denominator should be NULL.
--
-- Ratio metrics with current-week data:
--   denominator should be populated.
--
-- Peer-set values are intentionally NULL for now.
--
-- Expected:
-- invalidRows = 0
-- ============================================================================

SELECT
    COUNT(*) AS invalidRows

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long

WHERE targetWeekStartDate = DATE '2026-09-27'

  AND (
         (metricKind = 'count'
          AND thisWeekDenominator IS NOT NULL)

      OR (metricKind = 'ratio'
          AND thisWeekDataAvailable = TRUE
          AND thisWeekDenominator IS NULL)

      OR peerSetNumerator IS NOT NULL
      OR peerSetDenominator IS NOT NULL
  );



-- ############################################################################
-- GOLD 02
-- BREAKOUT METRIC INGREDIENTS BY WEEK - LONG
-- ############################################################################


-- ============================================================================
-- 02A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);


-- ============================================================================
-- 02B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);


-- ============================================================================
-- 02C. VALIDATION 1
-- Basic breakout population.
-- ============================================================================

SELECT
    targetWeekStartDate,

    COUNT(*) AS rowCount,

    COUNT(DISTINCT breakoutType) AS breakoutTypes,

    COUNT(
        DISTINCT concat(
            breakoutType,
            '|||',
            breakoutValue
        )
    ) AS breakoutValues,

    COUNT(DISTINCT metricName) AS distinctMetrics

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long

WHERE targetWeekStartDate = DATE '2026-09-27'

GROUP BY targetWeekStartDate;


-- ============================================================================
-- 02D. VALIDATION 2
-- Grain check.
--
-- Expected:
-- No rows.
-- ============================================================================

SELECT
    targetWeekStartDate,
    filterLob,
    filterPlatform,
    breakoutType,
    breakoutValue,
    metricName,
    COUNT(*) AS rowCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long

WHERE targetWeekStartDate = DATE '2026-09-27'

GROUP BY
    targetWeekStartDate,
    filterLob,
    filterPlatform,
    breakoutType,
    breakoutValue,
    metricName

HAVING COUNT(*) > 1

ORDER BY rowCount DESC

LIMIT 100;


-- ============================================================================
-- 02E. VALIDATION 3
-- Breakout NBV should reconcile back to Overview NBV.
--
-- Since every visitor has one attributed value for each breakout,
-- SUM(NBV across values) should equal topline NBV for each breakoutType.
--
-- Expected:
-- difference = 0
-- ============================================================================

WITH overview AS (
    SELECT
        thisWeekNumerator AS overviewNbv
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
    WHERE targetWeekStartDate = DATE '2026-09-27'
      AND metricName = 'nbv'
      AND filterLob = 'All'
      AND filterPlatform = 'All'
),

breakouts AS (
    SELECT
        breakoutType,
        SUM(thisWeekNumerator) AS breakoutNbv
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
    WHERE targetWeekStartDate = DATE '2026-09-27'
      AND metricName = 'nbv'
      AND filterLob = 'All'
      AND filterPlatform = 'All'
    GROUP BY breakoutType
)

SELECT
    b.breakoutType,
    o.overviewNbv,
    b.breakoutNbv,
    b.breakoutNbv - o.overviewNbv AS difference

FROM breakouts b

CROSS JOIN overview o

ORDER BY b.breakoutType;



-- ############################################################################
-- GOLD 03
-- CROSSTAB METRIC INGREDIENTS BY WEEK - LONG
-- ############################################################################


-- ============================================================================
-- 03A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);


-- ============================================================================
-- 03B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_crosstabMetricIngredientsByWeek_long(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);


-- ============================================================================
-- 03C. VALIDATION 1
-- Basic crosstab population.
-- ============================================================================

SELECT
    targetWeekStartDate,

    COUNT(*) AS rowCount,

    COUNT(DISTINCT pairKey) AS distinctPairs,

    COUNT(DISTINCT metricName) AS distinctMetrics,

    COUNT(
        DISTINCT concat(
            pairKey,
            '|||',
            rowBreakoutValue,
            '|||',
            columnBreakoutValue
        )
    ) AS distinctCells

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long

WHERE targetWeekStartDate = DATE '2026-09-27'

GROUP BY targetWeekStartDate;


-- ============================================================================
-- 03D. VALIDATION 2
-- Grain check.
--
-- Expected:
-- No rows.
-- ============================================================================

SELECT
    targetWeekStartDate,
    filterLob,
    filterPlatform,
    pairKey,
    rowBreakoutValue,
    columnBreakoutValue,
    metricName,
    COUNT(*) AS rowCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long

WHERE targetWeekStartDate = DATE '2026-09-27'

GROUP BY
    targetWeekStartDate,
    filterLob,
    filterPlatform,
    pairKey,
    rowBreakoutValue,
    columnBreakoutValue,
    metricName

HAVING COUNT(*) > 1

ORDER BY rowCount DESC

LIMIT 100;


-- ============================================================================
-- 03E. VALIDATION 3
-- Crosstab NBV reconciliation.
--
-- Each visitor belongs to exactly one cell for a given supported pair.
-- Therefore SUM(NBV across cells) should equal Overview NBV
-- for every pairKey.
--
-- Expected:
-- difference = 0
-- ============================================================================

WITH overview AS (
    SELECT
        thisWeekNumerator AS overviewNbv
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
    WHERE targetWeekStartDate = DATE '2026-09-27'
      AND metricName = 'nbv'
      AND filterLob = 'All'
      AND filterPlatform = 'All'
),

pairs AS (
    SELECT
        pairKey,
        MAX(pairLabel) AS pairLabel,
        SUM(thisWeekNumerator) AS crosstabNbv

    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long

    WHERE targetWeekStartDate = DATE '2026-09-27'
      AND metricName = 'nbv'
      AND filterLob = 'All'
      AND filterPlatform = 'All'

    GROUP BY pairKey
)

SELECT
    p.pairKey,
    p.pairLabel,
    o.overviewNbv,
    p.crosstabNbv,
    p.crosstabNbv - o.overviewNbv AS difference

FROM pairs p

CROSS JOIN overview o

ORDER BY p.pairKey;



-- ############################################################################
-- GOLD 04
-- EXPLORE SESSION x PAGE CATEGORY BY WEEK - WIDE
-- ############################################################################


-- ============================================================================
-- 04A. PREFLIGHT
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_exploreSessionPageCategoryByWeek_wide(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => TRUE
);


-- ============================================================================
-- 04B. LOAD
-- ============================================================================

CALL prdrzranalytics.lab42.sdi_sp_mip_gold_exploreSessionPageCategoryByWeek_wide(
    p_asOfDate       => DATE '2026-09-28',
    p_weeksToRebuild => 1,
    p_validateOnly   => FALSE
);


-- ============================================================================
-- 04C. VALIDATION 1
-- Basic Explore population.
-- ============================================================================

SELECT
    weekStartDate,
    weekEndDate,

    COUNT(*) AS rowCount,

    COUNT(DISTINCT sessionId) AS distinctSessions,

    COUNT(DISTINCT visitorId) AS distinctVisitors,

    SUM(pageViews) AS pageViews,

    SUM(orderCount) AS orderCount,

    SUM(vrCallEvents) AS vrCallEvents,

    SUM(vrChatEvents) AS vrChatEvents,

    SUM(storeLocatorEvents) AS storeLocatorEvents

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide

WHERE weekStartDate = DATE '2026-09-27'

GROUP BY
    weekStartDate,
    weekEndDate;


-- ============================================================================
-- 04D. VALIDATION 2
-- Grain check:
-- one row per week × session × pageCategory.
--
-- Expected:
-- No rows.
-- ============================================================================

SELECT
    weekStartDate,
    sessionId,
    pageCategory,
    COUNT(*) AS rowCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide

WHERE weekStartDate = DATE '2026-09-27'

GROUP BY
    weekStartDate,
    sessionId,
    pageCategory

HAVING COUNT(*) > 1

ORDER BY rowCount DESC

LIMIT 100;


-- ============================================================================
-- 04E. VALIDATION 3
-- Gold Explore vs its Silver source population.
--
-- The Gold load joins:
--   Silver attributesPerSession
--   +
--   Silver actionsPerSessionPageCategory
--
-- and requires visitorId IS NOT NULL.
--
-- Expected:
-- SOURCE_JOIN and GOLD row counts should match.
-- ============================================================================

SELECT
    'SOURCE_JOIN' AS dataset,
    COUNT(*) AS rowCount,
    SUM(a.pageViews) AS pageViews,
    SUM(a.orderCount) AS orderCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily s

JOIN prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily a
    ON  a.sessionId = s.sessionId
    AND a.weekStartDate = s.weekStartDate

WHERE s.weekStartDate = DATE '2026-09-27'
  AND a.weekStartDate = DATE '2026-09-27'
  AND s.visitorId IS NOT NULL

UNION ALL

SELECT
    'GOLD' AS dataset,
    COUNT(*) AS rowCount,
    SUM(pageViews) AS pageViews,
    SUM(orderCount) AS orderCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide

WHERE weekStartDate = DATE '2026-09-27';



-- ############################################################################
-- GOLD 05
-- OVERVIEW METRIC FORECAST BY WEEK - LONG
--
-- IMPORTANT:
-- This is NOT populated by the actuals pipeline.
-- It is only a target table for the future forecasting workflow.
-- ############################################################################


-- ============================================================================
-- 05A. TABLE SANITY
--
-- At this stage, zero rows is perfectly valid.
-- ============================================================================

SELECT
    COUNT(*) AS forecastRowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long;


-- ============================================================================
-- 05B. FUTURE FORECAST GRAIN CHECK
--
-- Once forecasts exist, review duplicates at:
-- week + filter context + metric + forecast run.
--
-- Expected now:
-- No rows.
--
-- Expected later:
-- Normally no duplicate rows for the same forecast run/context.
-- ============================================================================

SELECT
    weekStartDate,
    filterLob,
    filterPlatform,
    metricName,
    forecastRunId,
    COUNT(*) AS rowCount

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long

GROUP BY
    weekStartDate,
    filterLob,
    filterPlatform,
    metricName,
    forecastRunId

HAVING COUNT(*) > 1

ORDER BY rowCount DESC;


-- ============================================================================
-- 05C. FUTURE FORECAST BOUNDS CHECK
--
-- Expected:
-- invalidRows = 0
--
-- Low <= forecast <= High
-- ============================================================================

SELECT
    COUNT(*) AS invalidRows

FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long

WHERE forecastLow IS NOT NULL
  AND forecastValue IS NOT NULL
  AND forecastHigh IS NOT NULL
  AND (
         forecastLow > forecastValue
      OR forecastValue > forecastHigh
  );



-- ############################################################################
-- FINAL GOLD SANITY SUMMARY
-- ############################################################################


-- ============================================================================
-- 06A. ONE-WEEK ROW COUNTS
-- Quick way to confirm every Gold actuals table received data.
-- ============================================================================

SELECT
    'GOLD_01_OVERVIEW' AS objectName,
    COUNT(*) AS rowCount
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
WHERE targetWeekStartDate = DATE '2026-09-27'

UNION ALL

SELECT
    'GOLD_02_BREAKOUT',
    COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate = DATE '2026-09-27'

UNION ALL

SELECT
    'GOLD_03_CROSSTAB',
    COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
WHERE targetWeekStartDate = DATE '2026-09-27'

UNION ALL

SELECT
    'GOLD_04_EXPLORE',
    COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide
WHERE weekStartDate = DATE '2026-09-27'

UNION ALL

SELECT
    'GOLD_05_FORECAST',
    COUNT(*)
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricForecastByWeek_long
WHERE weekStartDate = DATE '2026-09-27';