-- PASS condition: both queries return 0.
SELECT COUNT(*) AS invalidBreakoutPeerRatioRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE metricKind='ratio'
  AND peerSetNumerator IS NOT NULL
  AND peerSetDenominator IS NOT NULL
  AND (try_divide(peerSetNumerator,peerSetDenominator)<0D OR try_divide(peerSetNumerator,peerSetDenominator)>1D);

SELECT COUNT(*) AS invalidCrosstabPeerRatioRows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
WHERE metricKind='ratio'
  AND peerSetNumerator IS NOT NULL
  AND peerSetDenominator IS NOT NULL
  AND (try_divide(peerSetNumerator,peerSetDenominator)<0D OR try_divide(peerSetNumerator,peerSetDenominator)>1D);
