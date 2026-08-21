/* =================================================================================================
FILE:         sdi_sp_dashboardPulseByod_silver_googleTrends_weekly.sql
LAYER:        Silver (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
TABLE:        sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly
PROCEDURE:    sdi_sp_dashboardPulseByod_silver_googleTrends_weekly

SOURCE:
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_googleTrends_weekly
  (shared Bronze table, NOT project-specific — maintained outside this pipeline, already weekly
   grain, one row per week, BYOD-specific by design — no filtering needed)

PURPOSE:
  Silver table for Google Trends BYOD market search-interest data.
  Outputs a WIDE table — one row per week_sun_to_sat.

BUSINESS GRAIN:
  One row per week_sun_to_sat

WEEK CONVERSION:
  Bronze date is the Sunday start of the week (assumed - not independently confirmed against
  the new raw source). week_sun_to_sat = DATE_ADD(date, 6)

METRICS:
  trends_byod_index          — market-level relative interest 0-100. Gets full WoW/LY/pct
                                treatment like every other metric in this pipeline.
  trends_top_kw_1..5,
  trends_kw1_interest..kw5,
  trends_kw1_change..kw5     — top 5 keywords driving interest that week, and each one's own
                                interest score / WoW change. These are passed through as-is —
                                no WoW/LY/pct computed on them here, since Gold Long's
                                trends_keywords_long unpivots them directly as point-in-time
                                values, matching how the original design treats keyword rank
                                data (a ranked list, not a comparable time series metric).

KNOWN DATA NOTE (carried over from Bronze):
  Keyword columns (top_kw_1..5, kwN_interest, kwN_change) are only populated from 2026-05-03
  onward — empty string / 0 in earlier rows. Not corrected here.

BUSINESS LOGIC:
  - data_source = 'TRENDS'
  - channel     = 'Organic Search' (matches original BQ Silver — Google Trends channel was
                  always classified as Organic Search in the source design, not a distinct
                  Search Trends category)
  - WoW: self-join on week_sun_to_sat - 7 days (gap-safe)
  - LY:  self-join on custom_week_num - 52 (gap-safe, Sun-to-Sat anchor)
  - wow_pct / yoy_pct as decimals — NULL when prior NULL or 0
  - max_data_date: latest week_sun_to_sat with byod_index non-null

CUSTOM WEEK NUMBER:
  custom_week_num = DATEDIFF(WEEK, DATE '2023-01-01', DATE_SUB(week_sun_to_sat, 6))

DOWNSTREAM:
  Gold Wide : sdi_tbl_dashboardPulseByod_gold_unified_wide
  Gold Long : sdi_tbl_dashboardPulseByod_gold_unified_long
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_dashboardPulseByod_silver_googleTrends_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_silver_googleTrends_weekly
  USING DELTA
  AS

  WITH base AS (
      SELECT
          DATE_ADD(date, 6) AS week_sun_to_sat,
          byod_index,
          top_kw_1, kw1_interest, kw1_change,
          top_kw_2, kw2_interest, kw2_change,
          top_kw_3, kw3_interest, kw3_change,
          top_kw_4, kw4_interest, kw4_change,
          top_kw_5, kw5_interest, kw5_change
      FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_googleTrends_weekly
  ),

  with_week_num AS (
      SELECT *,
          DATEDIFF(WEEK, DATE '2023-01-01', DATE_SUB(week_sun_to_sat, 6)) AS custom_week_num
      FROM base
  ),

  with_comparisons AS (
      SELECT
          c.week_sun_to_sat,
          c.custom_week_num,
          c.byod_index,
          w.byod_index AS byod_index_wow,
          l.byod_index AS byod_index_ly,
          c.top_kw_1, c.kw1_interest, c.kw1_change,
          c.top_kw_2, c.kw2_interest, c.kw2_change,
          c.top_kw_3, c.kw3_interest, c.kw3_change,
          c.top_kw_4, c.kw4_interest, c.kw4_change,
          c.top_kw_5, c.kw5_interest, c.kw5_change
      FROM with_week_num c
      LEFT JOIN with_week_num w ON c.week_sun_to_sat = DATE_ADD(w.week_sun_to_sat, 7)
      LEFT JOIN with_week_num l ON (c.custom_week_num - l.custom_week_num) = 52
  ),

  with_pcts AS (
      SELECT
          *,
          CASE WHEN byod_index_wow IS NULL OR byod_index_wow = 0 THEN NULL ELSE ROUND((byod_index - byod_index_wow) / byod_index_wow, 6) END AS byod_index_wow_pct,
          CASE WHEN byod_index_ly  IS NULL OR byod_index_ly  = 0 THEN NULL ELSE ROUND((byod_index - byod_index_ly)  / byod_index_ly,  6) END AS byod_index_yoy_pct
      FROM with_comparisons
  ),

  with_max_date AS (
      SELECT *,
          MAX(CASE WHEN byod_index IS NOT NULL THEN week_sun_to_sat END) OVER () AS max_data_date
      FROM with_pcts
  )

  SELECT
      week_sun_to_sat,
      'TRENDS'                                    AS data_source,
      'Organic Search'                             AS channel,
      max_data_date,

      byod_index          AS trends_byod_index,
      byod_index_wow       AS trends_byod_index_wow,
      byod_index_ly        AS trends_byod_index_ly,
      byod_index_wow_pct  AS trends_byod_index_wow_pct,
      byod_index_yoy_pct  AS trends_byod_index_yoy_pct,

      top_kw_1     AS trends_top_kw_1,
      kw1_interest AS trends_kw1_interest,
      kw1_change   AS trends_kw1_change,
      top_kw_2     AS trends_top_kw_2,
      kw2_interest AS trends_kw2_interest,
      kw2_change   AS trends_kw2_change,
      top_kw_3     AS trends_top_kw_3,
      kw3_interest AS trends_kw3_interest,
      kw3_change   AS trends_kw3_change,
      top_kw_4     AS trends_top_kw_4,
      kw4_interest AS trends_kw4_interest,
      kw4_change   AS trends_kw4_change,
      top_kw_5     AS trends_top_kw_5,
      kw5_interest AS trends_kw5_interest,
      kw5_change   AS trends_kw5_change

  FROM with_max_date
  ;

END;