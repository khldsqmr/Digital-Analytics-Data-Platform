-- ============================================================================
-- FILE  : 03_sdi_mv_mip_serving_overviewToplineMovers_long.sql
-- LAYER : SERVING / MATERIALIZED
-- TAB   : Overview
-- SECTION: What moved the topline
-- PURPOSE:
--   Comparator-aware global mover ranking across every active prebuilt breakout.
--   The source breakout contract has already applied All = Top 100 + scoped (Other)
--   within each breakout, so high-cardinality dimensions remain bounded and ratio-safe.
--
--   Top 5 / Top 10 are exact global rank filters.
--   All is capped at the first 100 globally ranked candidate rows. Because breakout
--   dimensions overlap by design, a single global numeric "(Other)" row would double-count;
--   therefore the source's scoped per-breakout (Other) rows are retained instead.
--
-- REFRESH:
--   TRIGGER ON UPDATE keeps this object independent of browser/API refreshes.
-- ============================================================================

CREATE OR REPLACE MATERIALIZED VIEW prdrzranalytics.lab42.sdi_mv_mip_serving_overviewToplineMovers_long
COMMENT 'MIP Overview What moved the topline. Global comparator-aware ranking over breakout Top100+Other buckets.'
CLUSTER BY (targetWeekStartDate, metricName, comparisonType)
REFRESH POLICY AUTO
TRIGGER ON UPDATE AT MOST EVERY INTERVAL 1 MINUTE
AS
WITH ranked AS (
    SELECT
        b.*,
        row_number() OVER (
            PARTITION BY targetWeekStartDate, filterLob, filterPlatform, metricName, comparisonType
            ORDER BY
                abs(impactOnToplineValue) DESC NULLS LAST,
                abs(absoluteDeltaValue) DESC NULLS LAST,
                breakoutType,
                breakoutValue
        ) AS impactRankAcrossBreakouts,
        count(*) OVER (
            PARTITION BY targetWeekStartDate, filterLob, filterPlatform, metricName, comparisonType
        ) AS candidateRowCount,
        sum(CASE WHEN abs(impactOnToplineValue) >= 1D THEN 1 ELSE 0 END) OVER (
            PARTITION BY targetWeekStartDate, filterLob, filterPlatform, metricName, comparisonType
        ) AS clearsOnePercentCount
    FROM prdrzranalytics.lab42.sdi_mv_mip_serving_breakoutsComparisonTable_long b
    WHERE comparisonDataAvailable
      AND impactOnToplineValue IS NOT NULL
)
SELECT
    targetWeekStartDate,
    targetWeekEndDate,
    fiscalQuarterLabel,
    fiscalWeekCode,
    weekLabel,
    weekEndingLabel,
    filterLob,
    filterPlatform,

    metricName,
    metricLabel,
    metricDescription,
    metricKind,
    displayFormat,
    changeUnit,
    metricSortOrder,
    metricDefinitionStatus,

    comparisonType,
    comparisonLabel,
    comparisonSortOrder,
    comparisonStartDate,
    comparisonEndDate,
    comparisonWeekCount,
    comparisonDataAvailable,
    comparisonWindowComplete,

    breakoutType,
    breakoutLabel,
    breakoutValue,
    breakoutSortOrder,
    breakoutDefinitionStatus,
    isOtherBucket,
    rawMemberCount,

    currentValue,
    comparisonValue,
    absoluteDeltaValue,
    changeValue,
    changeDirection,

    peerSetValue,
    peerSetAbsoluteDeltaValue,
    peerSetChangeValue,
    peerSetDataAvailable,

    toplineCurrentValue,
    toplineComparisonValue,
    impactOnToplineValue,
    impactOnToplineUnit,

    impactRankAcrossBreakouts,
    coalesce(impactRankAcrossBreakouts <= 5, FALSE) AS isTop5,
    coalesce(impactRankAcrossBreakouts <= 10, FALSE) AS isTop10,
    coalesce(impactRankAcrossBreakouts <= 100, FALSE) AS isTop100,
    candidateRowCount,
    greatest(candidateRowCount - 100, 0) AS allSuppressedCandidateCount,
    coalesce(abs(impactOnToplineValue) >= 1D, FALSE) AS clearsOnePercent,
    clearsOnePercentCount,

    100 AS allSelectionLimit,
    'All = Top 100 global mover rows. Scoped (Other) buckets are already produced within each breakout; no cross-breakout numeric Other is created because breakout dimensions overlap.' AS allSelectionRule,

    thisWeekDataAvailable,
    goldProcessedAt
FROM ranked;
