-- ============================================================================
-- FILE  : 06_sdi_vw_mip_serving_breakoutsMovers_long.sql
-- LAYER : SERVING
-- TAB   : Breakouts
-- SECTION: Movers
-- PURPOSE:
--   Ranks breakout values by absolute WoW movement for each selected metric.
--
-- NAMING:
--   Object name expresses app tab/section + physical shape only.
--   Current reporting grain is week-based, but cadence/grain is intentionally
--   not encoded in the serving-view object name.
-- ============================================================================

CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_serving_breakoutsMovers_long AS
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
    metricKind,
    displayFormat,
    changeUnit,
    metricSortOrder,

    currentValue,
    priorWeekValue,
    wowChange,

    currentValue - priorWeekValue AS absoluteValueDelta,

    CASE
        WHEN wowChange > 0 THEN 'up'
        WHEN wowChange < 0 THEN 'down'
        WHEN wowChange = 0 THEN 'flat'
        ELSE 'unavailable'
    END AS moverDirection,

    dense_rank() OVER (
        PARTITION BY
            targetWeekStartDate,
            filterLob,
            filterPlatform,
            breakoutType,
            metricName
        ORDER BY abs(wowChange) DESC NULLS LAST, breakoutValue
    ) AS moverRank,

    priorWeekDataAvailable,
    goldProcessedAt

FROM prdrzranalytics.lab42.sdi_vw_mip_serving_breakoutsTable_long
WHERE priorWeekDataAvailable;
