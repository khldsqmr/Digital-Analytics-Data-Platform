/* =================================================================================================
FILE:         sdi_sp_dashboardPulseByod_bronze_googleTrends_weekly.sql
LAYER:        Bronze (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
TABLE:        sdi_tbl_dashboardPulseByod_bronze_googleTrends_weekly
PROCEDURE:    sdi_sp_dashboardPulseByod_bronze_googleTrends_weekly

SOURCE:
  prdrzranalytics.lab42.sdi_tbl_googleTrends_raw_byod_weekly
  (Databricks-native raw/landing table — replaces the earlier Improvado-based source
  prd_dbi_analytics.improvado.sdi_seo_googletrends_byod_weekly_tmo. Leaner schema — no
  account_id, account_name, insert_date, or filename columns; only file_load_datetime
  is available for dedup ordering.)

PURPOSE:
  Source-close Bronze table for Google Trends BYOD weekly search interest data.
  Deduplicates the weekly snapshot and preserves all raw fields as-is.

BUSINESS GRAIN:
  One row per date_yyyymmdd

DEDUPE LOGIC:
  Latest row per date_yyyymmdd, ordered by:
    file_load_datetime DESC

KEY MODELING NOTES:
  - date: parsed DATE version of date_yyyymmdd (assumed week-beginning Sunday, matching the
    prior Improvado source's semantics — not independently confirmed against this new table.
    If real data shows this is actually week-ending Saturday, Silver's +6-day conversion
    needs to flip to a -6-day conversion instead)
  - Rows where date_yyyymmdd doesn't parse to a valid date are excluded
  - No account/asset dimension — this source is BYOD-specific and market-level by design

DOWNSTREAM:
  Silver : sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_dashboardPulseByod_bronze_googleTrends_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_googleTrends_weekly
  USING DELTA
  AS

  WITH ranked AS (
    SELECT
      src.byod_index,
      TO_DATE(src.date_yyyymmdd, 'yyyyMMdd') AS date,
      src.date_yyyymmdd,
      src.kw1_change,
      src.kw1_interest,
      src.kw2_change,
      src.kw2_interest,
      src.kw3_change,
      src.kw3_interest,
      src.kw4_change,
      src.kw4_interest,
      src.kw5_change,
      src.kw5_interest,
      src.top_kw_1,
      src.top_kw_2,
      src.top_kw_3,
      src.top_kw_4,
      src.top_kw_5,
      src.File_Load_datetime AS file_load_datetime,
      ROW_NUMBER() OVER (
        PARTITION BY src.date_yyyymmdd
        ORDER BY src.File_Load_datetime DESC
      ) AS rn
    FROM prdrzranalytics.lab42.sdi_tbl_googleTrends_raw_byod_weekly src
    WHERE TRY_TO_DATE(src.date_yyyymmdd, 'yyyyMMdd') IS NOT NULL
  )
  SELECT
    byod_index,
    date,
    date_yyyymmdd,
    kw1_change, kw1_interest,
    kw2_change, kw2_interest,
    kw3_change, kw3_interest,
    kw4_change, kw4_interest,
    kw5_change, kw5_interest,
    top_kw_1, top_kw_2, top_kw_3, top_kw_4, top_kw_5,
    file_load_datetime
  FROM ranked
  WHERE rn = 1
  ;

END;