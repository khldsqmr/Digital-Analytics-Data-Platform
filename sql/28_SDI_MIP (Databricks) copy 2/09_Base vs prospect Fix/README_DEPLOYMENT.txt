MIP GLOBAL PROSPECT/BASE FILTER — IN-PLACE DEPLOYMENT PACKAGE
=============================================================

GOAL
----
Add filterProspectVsBase to the EXISTING Overview-serving Gold/App tables without
creating v2/sidecar tables and without rerunning Bronze or Silver.

CURRENT SOURCE SEMANTICS — IMPORTANT
------------------------------------
The existing weekly Silver dimension is named prospectVsBase but its current
source values are preserved exactly (Customer / Care / Prospect / Unknown, plus
All as the precomputed global context). This package intentionally does NOT
silently collapse Customer/Care into a literal Base value. If Product later
approves an explicit Customer/Care -> Base rule, change the normalization in
Gold 01 and Gold 02 deliberately and revalidate.

WHAT CHANGES
------------
Persisted schema: one new STRING column named filterProspectVsBase is added to:
  1. sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
  2. sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
  3. sdi_tbl_mip_gold_appOverviewCards_wide
  4. sdi_tbl_mip_gold_appOverviewTrend_long
  5. sdi_tbl_mip_gold_appOverviewToplineMovers_long
  6. sdi_tbl_mip_gold_appOverviewConversionFunnel_long

No new persisted tables are created.
No Bronze table is rebuilt.
No Silver table is rebuilt.
Gold 03 Crosstabs and Gold 04 Explore schemas are unchanged.
App 05/06/07/08/09/11 schemas are unchanged; their procedures are compatibility-
patched to continue consuming only filterProspectVsBase='All'.
The POST-stage validation procedure is compatibility-patched so Gold 01/02
uniqueness includes the new filter dimension.

FORECAST CONTRACT
-----------------
Forecast Gold is still unsegmented. App Overview Cards therefore joins forecast
only for filterProspectVsBase='All'. Segmented contexts intentionally have NULL
forecastValue / forecastValueDisplay rather than reusing the topline forecast.

TOPLINE MOVERS CONTRACT
-----------------------
Gold 02 is rebuilt within each filterProspectVsBase context so Movers remains
mathematically consistent with the filtered topline. When the global filter is
not All, breakoutType='prospectVsBase' is suppressed because it would be a
self-breakout of the already-filtered population.

COST / PERFORMANCE CONTRACT
---------------------------
Each visitor-week is expanded to only two filter contexts:
  - All
  - its actual weekly prospectVsBase source state
It is NOT copied once for every possible segment value.
The expensive raw hit/session layers are not rerun.
The one-time rebuild is staged so the latest week is smoke-tested first, then the
previous five weeks are rebuilt. Each of the six target weeks is processed once.
Gold 02 is the main compute item; App rebuilds are small aggregated writes.

FILES / RUN ORDER
-----------------
1. 00_ONE_TIME_SCHEMA_MIGRATION.sql
   - Run precheck.
   - Run six ALTER TABLE ADD COLUMNS statements once.
   - Do NOT manually UPDATE old rows to All.

2. Deploy updated analytical procedures:
   01_sdi_sp_mip_gold_overviewMetricIngredientsByWeek_long.sql
   02_sdi_sp_mip_gold_breakoutMetricIngredientsByWeek_long.sql

3. Deploy updated Overview App procedures:
   03_sdi_sp_mip_gold_appOverviewCards_wide.sql
   04_sdi_sp_mip_gold_appOverviewTrend_long.sql
   05_sdi_sp_mip_gold_appOverviewToplineMovers_long.sql
   06_sdi_sp_mip_gold_appOverviewConversionFunnel_long.sql

4. Deploy compatibility procedures BEFORE rebuilding Gold 01/02:
   07_compat_appBreakoutsComparisonTable_long.sql
   08_compat_appBreakoutsWaterfall_long.sql
   09_compat_appBreakoutsAbsoluteTrend_long.sql
   10_compat_appCrosstabsMatrix_long.sql
   11_compat_appCrosstabsRankedPairs_long.sql
   12_compat_appExploreRankedPairs_long.sql
   13_compat_validation_postStage_perRun.sql

   These preserve the old non-Overview behavior by explicitly consuming the All
   context from Gold 01/02. They require deployment only; no historical rebuild.

5. Run 14_ONE_TIME_REBUILD_6_WEEKS.sql
   - Stage A: validate-only latest week.
   - Stage B: rebuild 2026-10-11 week.
   - Stage C: smoke checks.
   - Stop here if any expected-zero query returns rows.
   - Stage D: validate-only previous five weeks.
   - Stage E: rebuild 2026-09-06 through 2026-10-04.

6. Run 15_POST_MIGRATION_VALIDATION.sql
   All expected-zero reconciliation / uniqueness checks must be clean.

AFTER THE ONE-TIME MIGRATION
----------------------------
Normal weekly execution keeps the same procedure signatures. Gold 01 -> Gold 02
-> App 01/02/03/04 continues as before; the new filter contexts are generated for
the target week automatically. Existing non-Overview App procedures stay on All.

STATIC REVIEW STATUS
--------------------
The package was statically checked for balanced SQL parentheses/quotes, persisted
column/source ordering, affected MERGE keys, Gold 02 filter-aware peer joins,
Gold 02 independent four-week slice/peer aggregation, self-breakout suppression,
and compatibility source filters. It has NOT been executed against your Databricks
runtime here, so use the included validate-only and smoke-test stages before the
five-week historical rebuild.
