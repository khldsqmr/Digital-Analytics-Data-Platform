-- Non-destructive staging checks. Run AFTER six-week staging build.
-- Expected: only All, Prospect, Base, Unknown; six target weeks; no duplicate keys.
-- Historical All comparisons should be checked against production (currently still All).

-- sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
SELECT 'sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long' AS tableName, targetWeekStartDate, filterProspectVsBase, COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long_stg
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterProspectVsBase ORDER BY targetWeekStartDate,filterProspectVsBase;
SELECT 'sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long' AS tableName, COUNT(*) AS invalidFilterRows FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long_stg WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11' AND (filterProspectVsBase IS NULL OR filterProspectVsBase NOT IN ('All','Prospect','Base','Unknown'));

-- sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
SELECT 'sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long' AS tableName, targetWeekStartDate, filterProspectVsBase, COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long_stg
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterProspectVsBase ORDER BY targetWeekStartDate,filterProspectVsBase;
SELECT 'sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long' AS tableName, COUNT(*) AS invalidFilterRows FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long_stg WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11' AND (filterProspectVsBase IS NULL OR filterProspectVsBase NOT IN ('All','Prospect','Base','Unknown'));

-- sdi_tbl_mip_gold_appOverviewCards_wide
SELECT 'sdi_tbl_mip_gold_appOverviewCards_wide' AS tableName, targetWeekStartDate, filterProspectVsBase, COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide_stg
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterProspectVsBase ORDER BY targetWeekStartDate,filterProspectVsBase;
SELECT 'sdi_tbl_mip_gold_appOverviewCards_wide' AS tableName, COUNT(*) AS invalidFilterRows FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide_stg WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11' AND (filterProspectVsBase IS NULL OR filterProspectVsBase NOT IN ('All','Prospect','Base','Unknown'));

-- sdi_tbl_mip_gold_appOverviewTrend_long
SELECT 'sdi_tbl_mip_gold_appOverviewTrend_long' AS tableName, targetWeekStartDate, filterProspectVsBase, COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long_stg
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterProspectVsBase ORDER BY targetWeekStartDate,filterProspectVsBase;
SELECT 'sdi_tbl_mip_gold_appOverviewTrend_long' AS tableName, COUNT(*) AS invalidFilterRows FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long_stg WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11' AND (filterProspectVsBase IS NULL OR filterProspectVsBase NOT IN ('All','Prospect','Base','Unknown'));

-- sdi_tbl_mip_gold_appOverviewToplineMovers_long
SELECT 'sdi_tbl_mip_gold_appOverviewToplineMovers_long' AS tableName, targetWeekStartDate, filterProspectVsBase, COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long_stg
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterProspectVsBase ORDER BY targetWeekStartDate,filterProspectVsBase;
SELECT 'sdi_tbl_mip_gold_appOverviewToplineMovers_long' AS tableName, COUNT(*) AS invalidFilterRows FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long_stg WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11' AND (filterProspectVsBase IS NULL OR filterProspectVsBase NOT IN ('All','Prospect','Base','Unknown'));

-- sdi_tbl_mip_gold_appOverviewConversionFunnel_long
SELECT 'sdi_tbl_mip_gold_appOverviewConversionFunnel_long' AS tableName, targetWeekStartDate, filterProspectVsBase, COUNT(*) AS rows
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long_stg
WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11'
GROUP BY targetWeekStartDate,filterProspectVsBase ORDER BY targetWeekStartDate,filterProspectVsBase;
SELECT 'sdi_tbl_mip_gold_appOverviewConversionFunnel_long' AS tableName, COUNT(*) AS invalidFilterRows FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long_stg WHERE targetWeekStartDate BETWEEN DATE '2026-09-06' AND DATE '2026-10-11' AND (filterProspectVsBase IS NULL OR filterProspectVsBase NOT IN ('All','Prospect','Base','Unknown'));

-- Compare complete All rows by EXCEPT ALL after dropping processing metadata,
-- or compare scoped, business-column-specific checks. Do not compare raw SELECT *.
-- The existing 15_PostMigrationValidation.sql is for the PRODUCTION names;
-- adapt table names if you wish to run it against staging.
