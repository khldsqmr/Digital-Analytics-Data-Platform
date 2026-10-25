-- ============================================================================
-- MIP ONE-TIME SCHEMA MIGRATION
-- PURPOSE:
--   Add the persisted global Prospect/Base filter context to the EXISTING
--   analytical/App Overview-serving tables. No new tables are created.
--
-- IMPORTANT:
--   1) Run the PRECHECK first.
--   2) Run each ALTER exactly once. If the column already exists, skip that ALTER.
--   3) Existing rows will have NULL in the new column until their week is rebuilt.
--   4) Do NOT UPDATE historical rows to 'All'. The rebuild procedures will replace
--      the six target weeks atomically/scoped and avoid an unnecessary Delta rewrite.
--   5) Source semantics are preserved: All + existing weekly prospectVsBase values
--      (currently Customer / Care / Prospect / Unknown). No Customer/Care->Base
--      business remapping is introduced by this migration.
-- ============================================================================

-- ---------------------------------------------------------------------------
-- A. PRECHECK: expected 0 rows BEFORE the migration.
--    If a table is returned here, skip its ALTER below.
-- ---------------------------------------------------------------------------
SELECT table_name,column_name,data_type,ordinal_position
FROM prdrzranalytics.information_schema.columns
WHERE lower(table_schema)='lab42'
  AND lower(column_name)='filterprospectvsbase'
  AND lower(table_name) IN (
      'sdi_tbl_mip_gold_overviewmetricingredientsbyweek_long',
      'sdi_tbl_mip_gold_breakoutmetricingredientsbyweek_long',
      'sdi_tbl_mip_gold_appoverviewcards_wide',
      'sdi_tbl_mip_gold_appoverviewtrend_long',
      'sdi_tbl_mip_gold_appoverviewtoplinemovers_long',
      'sdi_tbl_mip_gold_appoverviewconversionfunnel_long'
  )
ORDER BY table_name;

-- ---------------------------------------------------------------------------
-- B. ONE-TIME ALTERs.
-- ---------------------------------------------------------------------------
ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
ADD COLUMNS (
    filterProspectVsBase STRING COMMENT 'Global Overview filter context: All plus existing weekly prospectVsBase source state.' AFTER filterPlatform
);

ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
ADD COLUMNS (
    filterProspectVsBase STRING COMMENT 'Global Overview filter context used by filtered topline movers: All plus existing weekly prospectVsBase source state.' AFTER filterPlatform
);

ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide
ADD COLUMNS (
    filterProspectVsBase STRING COMMENT 'Persisted global Overview Prospect/Base filter context.' AFTER filterPlatform
);

ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long
ADD COLUMNS (
    filterProspectVsBase STRING COMMENT 'Persisted global Overview Prospect/Base filter context.' AFTER filterPlatform
);

ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long
ADD COLUMNS (
    filterProspectVsBase STRING COMMENT 'Persisted global Overview Prospect/Base filter context.' AFTER filterPlatform
);

ALTER TABLE prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long
ADD COLUMNS (
    filterProspectVsBase STRING COMMENT 'Persisted global Overview Prospect/Base filter context.' AFTER filterPlatform
);

-- ---------------------------------------------------------------------------
-- C. POST-ALTER SCHEMA CHECK: expected exactly 6 rows.
-- ---------------------------------------------------------------------------
SELECT table_name,column_name,data_type,ordinal_position
FROM prdrzranalytics.information_schema.columns
WHERE lower(table_schema)='lab42'
  AND lower(column_name)='filterprospectvsbase'
  AND lower(table_name) IN (
      'sdi_tbl_mip_gold_overviewmetricingredientsbyweek_long',
      'sdi_tbl_mip_gold_breakoutmetricingredientsbyweek_long',
      'sdi_tbl_mip_gold_appoverviewcards_wide',
      'sdi_tbl_mip_gold_appoverviewtrend_long',
      'sdi_tbl_mip_gold_appoverviewtoplinemovers_long',
      'sdi_tbl_mip_gold_appoverviewconversionfunnel_long'
  )
ORDER BY table_name;
