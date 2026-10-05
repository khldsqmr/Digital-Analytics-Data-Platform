-- ============================================================================
-- FILE  : 30_mip_peer_impact_crosstab_sanity_checks.sql
-- PURPOSE:
--   Focused post-rebuild checks for the peer-set / impact / crosstab additions.
--
-- IMPORTANT:
--   Run section-by-section. These checks intentionally stay outside the production
--   procedures so they can later be promoted into the formal Validation layer.
-- ============================================================================

-- ============================================================================
-- 1. SILVER 04: CHANNEL LIST CONTRACT
-- Expected:
--   duplicateVisitorWeeks = 0
--   emptyChannelLists = 0
--   duplicateChannelsInsideList = 0
--   attributedChannelMissingFromList = 0
-- ============================================================================
WITH grain AS (
    SELECT weekStartDate,visitorId,COUNT(*) AS rowCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
    WHERE weekStartDate = DATE '2026-09-27'
    GROUP BY weekStartDate,visitorId
)
SELECT
    (SELECT COUNT(*) FROM grain WHERE rowCount>1) AS duplicateVisitorWeeks,
    COUNT_IF(channelList IS NULL OR size(channelList)=0) AS emptyChannelLists,
    COUNT_IF(size(channelList)<>size(array_distinct(channelList))) AS duplicateChannelsInsideList,
    COUNT_IF(NOT array_contains(channelList,channel)) AS attributedChannelMissingFromList
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
WHERE weekStartDate = DATE '2026-09-27';

-- ============================================================================
-- 2. SILVER 05: PARENT TOTALS STILL RECONCILE AFTER HELPER ADDITION
-- Expected: all diffs = 0.
-- ============================================================================
WITH expected AS (
    SELECT
        weekStartDate,
        COUNT(*) AS sessionCount,
        SUM(pageViews) AS pageViews,
        SUM(orderCount) AS orderCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
    WHERE weekStartDate = DATE '2026-09-27'
      AND visitorId IS NOT NULL
    GROUP BY weekStartDate
),
actual AS (
    SELECT
        weekStartDate,
        SUM(sessionCount) AS sessionCount,
        SUM(pageViews) AS pageViews,
        SUM(orderCount) AS orderCount
    FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
    WHERE weekStartDate = DATE '2026-09-27'
    GROUP BY weekStartDate
)
SELECT
    a.sessionCount-e.sessionCount AS sessionCountDiff,
    a.pageViews-e.pageViews AS pageViewDiff,
    a.orderCount-e.orderCount AS orderCountDiff
FROM actual a
INNER JOIN expected e USING (weekStartDate);

-- ============================================================================
-- 3. SILVER 05: CHANNEL HELPER STRUCT SANITY
-- Expected:
--   emptyMemberships = 0
--   duplicateChannelEntries = 0
--   invalidNestedFlags = 0
-- ============================================================================
SELECT
    COUNT_IF(channelMetricMemberships IS NULL OR size(channelMetricMemberships)=0) AS emptyMemberships,
    COUNT_IF(
        size(transform(channelMetricMemberships,x -> x.channel))
        <> size(array_distinct(transform(channelMetricMemberships,x -> x.channel)))
    ) AS duplicateChannelEntries,
    COUNT_IF(
        exists(
            channelMetricMemberships,
            x -> x.nbv<>1
              OR x.nbvBuyFlow NOT IN (0,1)
              OR x.nbvConfigure NOT IN (0,1)
              OR x.nbvCheckoutStart NOT IN (0,1)
              OR x.orders NOT IN (0,1)
              OR x.ordersAcquisition NOT IN (0,1)
              OR x.ordersBase NOT IN (0,1)
              OR x.ordersUnassisted NOT IN (0,1)
              OR x.ordersAssisted NOT IN (0,1)
              OR x.vrCalls NOT IN (0,1)
              OR x.vrChats NOT IN (0,1)
              OR x.storeLocator NOT IN (0,1)
        )
    ) AS invalidNestedFlags
FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
WHERE weekStartDate = DATE '2026-09-27';

-- ============================================================================
-- 4. GOLD 02: BREAKOUT PEER COUNTERFACTUAL AVAILABILITY
-- Expected: all invalid counts = 0.
-- ============================================================================
SELECT
    COUNT_IF(fourWeekTrendWeekCount<>4 AND peerSetNumerator IS NOT NULL) AS peerWithoutFullFourWeeks,
    COUNT_IF(metricKind='count' AND peerSetDenominator IS NOT NULL) AS countPeerWithDenominator,
    COUNT_IF(metricKind='ratio' AND peerSetNumerator IS NOT NULL AND peerSetDenominator IS NULL) AS ratioPeerMissingDenominator
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate = DATE '2026-09-27';

-- ============================================================================
-- 5. GOLD 03: CROSSTAB PEER COUNTERFACTUAL AVAILABILITY
-- Expected: all invalid counts = 0.
-- ============================================================================
SELECT
    COUNT_IF(fourWeekTrendWeekCount<>4 AND peerSetNumerator IS NOT NULL) AS peerWithoutFullFourWeeks,
    COUNT_IF(metricKind='count' AND peerSetDenominator IS NOT NULL) AS countPeerWithDenominator,
    COUNT_IF(metricKind='ratio' AND peerSetNumerator IS NOT NULL AND peerSetDenominator IS NULL) AS ratioPeerMissingDenominator
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
WHERE targetWeekStartDate = DATE '2026-09-27';

-- ============================================================================
-- 6. APP GOLD: COUNT IMPACT-ON-TOPLINE FORMULA
--
-- Run after the App Gold procedures are rebuilt.
-- Expected: maxAbsFormulaDiff should be 0 (allow tiny floating-point noise).
-- ============================================================================
-- SELECT
--     MAX(
--         ABS(
--             impactOnToplineValue
--             - 100D * try_divide(currentValue-comparisonValue,toplineComparisonValue)
--         )
--     ) AS maxAbsFormulaDiff
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
-- WHERE targetWeekStartDate = DATE '2026-09-27'
--   AND metricKind='count'
--   AND comparisonDataAvailable
--   AND toplineComparisonValue IS NOT NULL;

-- ============================================================================
-- 7. APP GOLD: PEER GAP CONTRACT
--
-- peerSetValue is the counterfactual expected current slice/cell value.
-- For count metrics:
--   peerSetAbsoluteDeltaValue = currentValue - peerSetValue
--   peerSetChangeValue =
--       100 * (currentValue - peerSetValue) / four-week slice baseline
--
-- Synthetic (Other) buckets made from >1 raw member intentionally have no peer
-- benchmark because individual counterfactuals are non-additive.
-- ============================================================================
-- SELECT
--     targetWeekStartDate,breakoutType,breakoutValue,metricName,
--     currentValue,peerSetValue,peerSetAbsoluteDeltaValue,peerSetChangeValue,
--     peerSetDataAvailable
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long
-- WHERE targetWeekStartDate = DATE '2026-09-27'
--   AND breakoutType='channel'
--   AND NOT isOtherBucket
-- ORDER BY ABS(peerSetChangeValue) DESC NULLS LAST
-- LIMIT 100;
