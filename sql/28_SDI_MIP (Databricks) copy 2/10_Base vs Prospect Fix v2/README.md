# MIP Prospect/Base — permanent SQL, isolated validation, production cutover

This bundle reuses the earlier six week-scoped procedure transformations and separates them into production and staging forms. They are candidates pending Databricks runtime compilation, SQL execution and regression checks. No procedure has been executed in your workspace.

## Scope

Updated DATA tables (exact names retained in production):
- `prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long`
- `prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long`
- `prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide`
- `prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long`
- `prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long`
- `prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long`

Other analytical Gold tables and independent forecast target: unchanged. Forecasts are only joined for All. Six compat App procedures + post-stage validation included. One-time schema migration already completed: DO NOT rerun. Weekly Silver preserved.

## Critical API contract

Old API SQL that reads a changed table without filtering `filterProspectVsBase` will RETURN EXTRA ROWS after promotion. These tables are append-in-dimension (All + segment), not backward-compatible at query-result level. Preserve original responses by enforcing `filterProspectVsBase = 'All'` for requests without a new filter parameter. For new requests allow only All/Prospect/Base/Unknown, and include this context in all API joins, aggregations and cache keys. API deployments must be coordinated with production promotion.

`filterProspectVsBase` already exists in all six production table schemas and historical rows were initialized to All. We make no additional migrations.

## Usage
1. Check Silver uniqueness (`weekStartDate`, `visitorId`) and all six weekly source dates. Confirm source breakouts attribution.
2. Snapshot production versions via `DESCRIBE HISTORY` or durable deep clones. Save existing procedure definitions.
3. Execute `00_CreateStagingClones.sql`. Clones preserve schemas and current All output.
4. Deploy six `02_STAGING_PROCEDURES` scripts. Every Overview read/write is staged; Silver/controls/forecast are read-only.
5. Run `10_STAGING_Runbook.sql` section by section; then `20_StagingQualityChecks.sql`.
6. Test SAME-week rerun, zero source rows / disappeared segments, comparator dependencies, and compare staging All vs live production All for each table; inspect actual business formulas.
7. After acceptance coordinate a write freeze and a consistent API response strategy, deploy six permanent production definitions, and call six procedures for staged historical weeks (or implement a formally reviewed promotion path). Multiple-table cutover is not atomic. Keep existing API serving All until validation complete.

## Provenance and caveats
- Code is adapted from earlier generated week-scoped package and current uploaded production definitions. No DB connectivity: parsing, query semantics, execution/performance, target-column compatibility cannot be guaranteed.
- Static SQL does not guarantee `EXECUTE IMMEDIATE` string parsing or generated CTE correctness; validate in staging.
- Scope replacement is atomic FOR EACH TABLE, not across six tables.
- Scope predicates refer to `targetWeekStartDate` Sunday dates; p_asOfDate can be any date in the chosen Sunday-Saturday week.
- Historical silver corrections may require rebuilding downstream comparator weeks in addition to the corrected week.
- Production SQL filenames retain current existing physical table/procedure names. Staging SQL alters only the six Overview source/destination table identifiers and procedure names.
- The compatibility procedures are provided as previously proposed, NOT reverified in Databricks; ensure compatibility queries use `filterProspectVsBase='All'` on every relevant join to expanded Gold.
- The replacement command uses BY NAME and must return unique, correct names for target columns.
- No `DROP TABLE` or blanket `UPDATE` statements were included.
