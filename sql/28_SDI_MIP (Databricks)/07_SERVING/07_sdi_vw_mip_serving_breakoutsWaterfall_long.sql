-- ============================================================================
-- FILE  : 07_sdi_vw_mip_serving_breakoutsWaterfall_long.sql
-- LAYER : SERVING
-- TAB   : Breakouts
-- SECTION: Waterfall
-- PURPOSE:
--   Additive WoW count deltas by breakout value.
--
-- NOTE:
--   Waterfall is limited to count metrics because count slices are additive.
--   Ratio metrics are intentionally excluded from additive contribution math.
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_breakoutsWaterfall_long AS
WITH base AS (
    SELECT
        targetWeekStartDate,
        targetWeekEndDate,
        fiscalQuarterLabel,
        fiscalWeekCode,
        weekLabel,
        filterLob,
        filterPlatform,

        breakoutType,
        breakoutLabel,
        breakoutValue,
        valueRankByNbv,
        isTopN,
        breakoutSortOrder,

        metricName,
        metricLabel,
        displayFormat,
        metricSortOrder,

        priorWeekValue AS startValue,
        currentValue AS endValue,
        currentValue - priorWeekValue AS deltaValue,

        goldProcessedAt

    FROM prdrzranalytics.lab42.sdi_vw_mip_serving_breakoutsTable_long

    WHERE metricKind = 'count'
      AND priorWeekDataAvailable
      AND currentValue IS NOT NULL
      AND priorWeekValue IS NOT NULL
)
SELECT
    *,

    sum(deltaValue) OVER (
        PARTITION BY
            targetWeekStartDate,
            filterLob,
            filterPlatform,
            breakoutType,
            metricName
    ) AS totalChange,

    100D * try_divide(
        deltaValue,
        sum(deltaValue) OVER (
            PARTITION BY
                targetWeekStartDate,
                filterLob,
                filterPlatform,
                breakoutType,
                metricName
        )
    ) AS contributionPctOfTotalChange,

    dense_rank() OVER (
        PARTITION BY
            targetWeekStartDate,
            filterLob,
            filterPlatform,
            breakoutType,
            metricName
        ORDER BY abs(deltaValue) DESC NULLS LAST, breakoutValue
    ) AS contributionRank

FROM base;
