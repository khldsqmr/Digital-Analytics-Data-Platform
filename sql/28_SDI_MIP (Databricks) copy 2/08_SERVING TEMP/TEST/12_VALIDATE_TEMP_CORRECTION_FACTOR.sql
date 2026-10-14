-- Diagnostic only while the OLD analytical Gold02/03 rows are still present.
-- For counts the TEMP App hotfix converts the corrupted analytical ingredient back to the intended SUM:
-- correctedFourWeekSum = storedFourWeekTrendNumerator / fourWeekTrendWeekCount
-- final App average     = correctedFourWeekSum / fourWeekTrendWeekCount
SELECT
    targetWeekStartDate,metricName,breakoutType,breakoutValue,fourWeekTrendWeekCount,
    fourWeekTrendNumerator AS storedInflatedIngredient,
    try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE)) AS correctedFourWeekSum,
    try_divide(fourWeekTrendNumerator,
               cast(fourWeekTrendWeekCount AS DOUBLE)*cast(fourWeekTrendWeekCount AS DOUBLE)) AS expectedCountFourWeekAverage
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate IN (DATE '2026-09-20',DATE '2026-09-27')
  AND metricKind='count'
  AND fourWeekTrendWeekCount>0
LIMIT 100;
