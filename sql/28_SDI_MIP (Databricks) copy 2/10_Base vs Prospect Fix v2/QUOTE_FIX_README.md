# v3 Quote Fix — Databricks dynamic SQL

## Observed failure
`[UNRESOLVED_COLUMN.WITH_SUGGESTION] Prospect cannot be resolved`, from Gold 01 at the generated inner SQL CASE expression.

## Cause / fix
The Gold 01 and four Overview App stored procedures generated dynamic SQL using doubled single quotes (`''Prospect''`) inside the surrounding SQL string. The observed runtime parsed the inner statement without single quotes around Prospect. To match the established working Gold 02 pattern, the generated SQL string is now encoded with Databricks backslash-escaped quotes (`\'Prospect\'`). This change is applied **only inside `SET v_writeSql = concat(...)`** and to both production/staging variants. This includes empty string literals such as `''''`, now `\'\'`.

Gold 02 already used the backslash-escaped convention and is unchanged. The rest of the transformation is unmodified.

## Critical notes
- These scripts have NOT been compiled or run in your Databricks workspace; syntax and correctness require runtime tests.
- `CREATE OR REPLACE PROCEDURE` for production objects changes the live stored-procedure definition; deploy in a controlled window. `p_validateOnly => TRUE` runs preflight but does NOT execute the dynamically assembled transformation.
- If Gold 01 had failed during the dynamic SQL execution before the `INSERT`, its selected target week has not been successfully replaced by that statement; check table history before relying on it.
- After deployment, test on staging or one carefully chosen reporting week. Check `All` historical regression, accepted segments, unique grain, and all App procedures.
- Calendar is Sun–Sat; start date is Sunday, end is Saturday.
- API consumers must default to `filterProspectVsBase = 'All'` until the new filter is enabled.

## Changed files
- `01_PRODUCTION_PROCEDURES/01_GoldOverview_WeekReplace.sql` (62 doubled-quote instances changed)
- `01_PRODUCTION_PROCEDURES/03_AppOverviewCards_WeekReplace.sql` (82 doubled-quote instances changed)
- `01_PRODUCTION_PROCEDURES/04_AppOverviewTrend_WeekReplace.sql` (106 doubled-quote instances changed)
- `01_PRODUCTION_PROCEDURES/05_AppOverviewToplineMovers_WeekReplace.sql` (415 doubled-quote instances changed)
- `01_PRODUCTION_PROCEDURES/06_AppOverviewConversionFunnel_WeekReplace.sql` (132 doubled-quote instances changed)
- `02_STAGING_PROCEDURES/01_GoldOverview_WeekReplace_STAGING.sql` (62 doubled-quote instances changed)
- `02_STAGING_PROCEDURES/03_AppOverviewCards_WeekReplace_STAGING.sql` (82 doubled-quote instances changed)
- `02_STAGING_PROCEDURES/04_AppOverviewTrend_WeekReplace_STAGING.sql` (106 doubled-quote instances changed)
- `02_STAGING_PROCEDURES/05_AppOverviewToplineMovers_WeekReplace_STAGING.sql` (415 doubled-quote instances changed)
- `02_STAGING_PROCEDURES/06_AppOverviewConversionFunnel_WeekReplace_STAGING.sql` (132 doubled-quote instances changed)
