-- Validate that the TEMP ingestion correction factor is exactly fourWeekTrendWeekCount
-- for representative non-overlapping breakout dimensions/pairs before relying on the hotfix.
-- PASS: corrected numerator/denominator differences should be approximately 0.

WITH breakoutCorrected AS (
    SELECT
        g.targetWeekStartDate,
        g.metricName,
        g.breakoutType,
        max(g.fourWeekTrendWeekCount) AS weekCount,
        sum(try_divide(g.fourWeekTrendNumerator,cast(g.fourWeekTrendWeekCount AS DOUBLE))) AS correctedNumerator,
        sum(try_divide(g.fourWeekTrendDenominator,cast(g.fourWeekTrendWeekCount AS DOUBLE))) AS correctedDenominator
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long g
    WHERE g.targetWeekStartDate IN (DATE '2026-09-20',DATE '2026-09-27')
      AND g.fourWeekTrendWeekCount>0
      AND g.breakoutType IN ('authState','channel','device','platform','prospectVsBase')
    GROUP BY g.targetWeekStartDate,g.metricName,g.breakoutType
), overview AS (
    SELECT targetWeekStartDate,metricName,fourWeekTrendNumerator,fourWeekTrendDenominator
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
    WHERE targetWeekStartDate IN (DATE '2026-09-20',DATE '2026-09-27')
)
SELECT
    b.targetWeekStartDate,b.metricName,b.breakoutType,b.weekCount,
    b.correctedNumerator,o.fourWeekTrendNumerator AS overviewNumerator,
    b.correctedNumerator-o.fourWeekTrendNumerator AS numeratorDiff,
    b.correctedDenominator,o.fourWeekTrendDenominator AS overviewDenominator,
    b.correctedDenominator-o.fourWeekTrendDenominator AS denominatorDiff
FROM breakoutCorrected b
JOIN overview o
  ON o.targetWeekStartDate=b.targetWeekStartDate
 AND o.metricName=b.metricName
ORDER BY abs(coalesce(b.correctedNumerator-o.fourWeekTrendNumerator,0D)) DESC,
         b.targetWeekStartDate,b.metricName,b.breakoutType;

WITH crosstabCorrected AS (
    SELECT
        g.targetWeekStartDate,
        g.metricName,
        g.pairKey,
        max(g.fourWeekTrendWeekCount) AS weekCount,
        sum(try_divide(g.fourWeekTrendNumerator,cast(g.fourWeekTrendWeekCount AS DOUBLE))) AS correctedNumerator,
        sum(try_divide(g.fourWeekTrendDenominator,cast(g.fourWeekTrendWeekCount AS DOUBLE))) AS correctedDenominator
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long g
    WHERE g.targetWeekStartDate IN (DATE '2026-09-20',DATE '2026-09-27')
      AND g.fourWeekTrendWeekCount>0
      AND g.rowBreakoutType<>'lob'
      AND g.columnBreakoutType<>'lob'
    GROUP BY g.targetWeekStartDate,g.metricName,g.pairKey
), overview AS (
    SELECT targetWeekStartDate,metricName,fourWeekTrendNumerator,fourWeekTrendDenominator
    FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
    WHERE targetWeekStartDate IN (DATE '2026-09-20',DATE '2026-09-27')
)
SELECT
    c.targetWeekStartDate,c.metricName,c.pairKey,c.weekCount,
    c.correctedNumerator,o.fourWeekTrendNumerator AS overviewNumerator,
    c.correctedNumerator-o.fourWeekTrendNumerator AS numeratorDiff,
    c.correctedDenominator,o.fourWeekTrendDenominator AS overviewDenominator,
    c.correctedDenominator-o.fourWeekTrendDenominator AS denominatorDiff
FROM crosstabCorrected c
JOIN overview o
  ON o.targetWeekStartDate=c.targetWeekStartDate
 AND o.metricName=c.metricName
ORDER BY abs(coalesce(c.correctedNumerator-o.fourWeekTrendNumerator,0D)) DESC,
         c.targetWeekStartDate,c.metricName,c.pairKey;
