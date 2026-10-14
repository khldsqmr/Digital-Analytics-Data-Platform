-- Example: rebuild screenshot weeks Sep 20 and Sep 27 using the temporary App hotfix.
-- Deploy the 7 affected TEMP procedures first, then run these calls.
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appOverviewToplineMovers_long(DATE '2026-10-03',2,FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsComparisonTable_long(DATE '2026-10-03',2,FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsWaterfall_long(DATE '2026-10-03',2,FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appBreakoutsAbsoluteTrend_long(DATE '2026-10-03',2,FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsMatrix_long(DATE '2026-10-03',2,FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appCrosstabsRankedPairs_long(DATE '2026-10-03',2,FALSE);
CALL prdrzranalytics.lab42.sdi_sp_mip_gold_appExploreRankedPairs_long(DATE '2026-10-03',2,FALSE);
