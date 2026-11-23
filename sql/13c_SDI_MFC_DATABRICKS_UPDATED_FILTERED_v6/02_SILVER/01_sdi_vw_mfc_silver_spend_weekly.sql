-- ============================================================
-- SILVER 1 — SPEND, NON-GRANULAR / LOB LEVEL
-- ============================================================
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mfc_silver_spend_weekly()
SQL SECURITY DEFINER
COMMENT 'Creates/refreshes sdi_tbl_mfc_silver_spend_weekly. Actual and Forecast each come from Bronze''s single latest-file-snapshot per week, processed independently. No blended Spend_Final. Raw quarter tags from the source pass through as-is — no day-proportional boundary-week reallocation.'
BEGIN
  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_mfc_silver_spend_weekly
  USING DELTA
  COMMENT 'MFC Silver Spend (non-granular / LOB level). Spend_Actual and Spend_Forecast only — no blended Spend_Final. Boundary weeks use raw quarter tags from the source: if one quarter has a value and the other does not, only the quarter with a value is populated; if both have values, each carries its own raw value; if neither has a value, both are NULL. No day-proportional split is applied.'
  AS
  WITH
  calendar_resolved AS (
    SELECT
      c.qgp_date         AS QGP_Week,
      c.week_type,
      c.quarter          AS quarter,
      c.days_in_period,
      stub.qgp_date      AS stub_QGP_Week,
      stub.quarter       AS stub_quarter
    FROM prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar c
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar stub
      ON  c.boundary_stub_date = stub.qgp_date
      AND stub.week_type = 'BOUNDARY_STUB'
    WHERE c.week_type IN ('NORMAL', 'BOUNDARY_FIRST')
  ),

  -- ── Actuals: one row per (QGP_Week, Quarter, LOB) ──────────────────────────
  -- Convert Bronze Quarter format "Q1'26" -> "2026 Q1" to match the calendar's
  -- quarter string, then roll up to LOB grain (drop channel/tactic/agency).
  actual_quarter_totals AS (
    SELECT
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      LOB_Supported,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2)) AS Quarter,
      SUM(weekly_actual)      AS quarter_total_actual,
      MAX(FileLoad_Date)      AS FileLoad_Date,
      MAX(Source_File_Date)   AS Source_File_Date
    FROM prdrzranalytics.lab42.sdi_tbl_mfc_bronze_spendActualsGranular_weekly
    GROUP BY
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2))
  ),

  -- ── Pass raw quarter totals through — no day-proportional split ─────────────
  -- Join to calendar_resolved to get the authoritative week_type and QGP_Week.
  -- For a BOUNDARY_FIRST row: the Bronze row tagged to the new quarter joins to
  --   calendar_resolved on QGP_Week + quarter match -> emits the BOUNDARY_FIRST row.
  -- For the same natural week's stub quarter: the Bronze row tagged to the old
  --   quarter joins to calendar_resolved on QGP_Week, matching stub_quarter ->
  --   emits a BOUNDARY_STUB row at stub_QGP_Week.
  -- For a NORMAL week: straight pass-through, one row per (QGP_Week, LOB).
  -- If a quarter has no data for a boundary week, no Bronze row exists for it ->
  --   no row emitted for that quarter, NULL appears downstream via LEFT JOIN.
  actual_reallocated AS (
    -- BOUNDARY_FIRST rows: Bronze quarter tag matches the new (post-boundary) quarter
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.QGP_Week,
      qt.LOB_Supported,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.quarter          AS Quarter,
      qt.quarter_total_actual AS weekly_actual,
      cr.week_type
    FROM actual_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week = cr.QGP_Week
      AND qt.Quarter  = cr.quarter
    WHERE cr.week_type = 'BOUNDARY_FIRST'

    UNION ALL

    -- BOUNDARY_STUB rows: Bronze quarter tag matches the old (pre-boundary) quarter
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.stub_QGP_Week    AS QGP_Week,
      qt.LOB_Supported,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.stub_quarter     AS Quarter,
      qt.quarter_total_actual AS weekly_actual,
      'BOUNDARY_STUB'     AS week_type
    FROM actual_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week    = cr.QGP_Week
      AND qt.Quarter     = cr.stub_quarter
    WHERE cr.week_type = 'BOUNDARY_FIRST'
      AND cr.stub_QGP_Week IS NOT NULL

    UNION ALL

    -- NORMAL weeks: straight pass-through
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.QGP_Week,
      qt.LOB_Supported,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.quarter          AS Quarter,
      qt.quarter_total_actual AS weekly_actual,
      cr.week_type
    FROM actual_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week = cr.QGP_Week
      AND qt.Quarter  = cr.quarter
    WHERE cr.week_type = 'NORMAL'
  ),

  -- ── Forecasts: same structure as actuals ────────────────────────────────────
  forecast_quarter_totals AS (
    SELECT
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      LOB_Supported,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2)) AS Quarter,
      SUM(weekly_forecast)    AS quarter_total_forecast,
      MAX(FileLoad_Date)      AS FileLoad_Date,
      MAX(Source_File_Date)   AS Source_File_Date
    FROM prdrzranalytics.lab42.sdi_tbl_mfc_bronze_spendForecastGranular_weekly
    GROUP BY
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2))
  ),

  forecast_reallocated AS (
    -- BOUNDARY_FIRST rows
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.QGP_Week,
      qt.LOB_Supported,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.quarter          AS Quarter,
      qt.quarter_total_forecast AS weekly_forecast,
      cr.week_type
    FROM forecast_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week = cr.QGP_Week
      AND qt.Quarter  = cr.quarter
    WHERE cr.week_type = 'BOUNDARY_FIRST'

    UNION ALL

    -- BOUNDARY_STUB rows
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.stub_QGP_Week    AS QGP_Week,
      qt.LOB_Supported,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.stub_quarter     AS Quarter,
      qt.quarter_total_forecast AS weekly_forecast,
      'BOUNDARY_STUB'     AS week_type
    FROM forecast_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week    = cr.QGP_Week
      AND qt.Quarter     = cr.stub_quarter
    WHERE cr.week_type = 'BOUNDARY_FIRST'
      AND cr.stub_QGP_Week IS NOT NULL

    UNION ALL

    -- NORMAL weeks
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.QGP_Week,
      qt.LOB_Supported,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.quarter          AS Quarter,
      qt.quarter_total_forecast AS weekly_forecast,
      cr.week_type
    FROM forecast_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week = cr.QGP_Week
      AND qt.Quarter  = cr.quarter
    WHERE cr.week_type = 'NORMAL'
  ),

  -- ── Merge actuals and forecasts, roll up to LOB grain ──────────────────────
  resolved_granular AS (
    SELECT
      COALESCE(a.Week_Beginning_Monday, f.Week_Beginning_Monday) AS Week_Beginning_Monday,
      COALESCE(a.Week_Ending_Sunday,    f.Week_Ending_Sunday)    AS Week_Ending_Sunday,
      COALESCE(a.QGP_Week,             f.QGP_Week)              AS QGP_Week,
      COALESCE(a.LOB_Supported,        f.LOB_Supported)         AS LOB_Supported,
      a.weekly_actual                                            AS Spend_Actual,
      f.weekly_forecast                                          AS Spend_Forecast,
      a.FileLoad_Date      AS Actual_FileLoad_Date,
      a.Source_File_Date   AS Actual_Source_File_Date,
      f.FileLoad_Date      AS Forecast_FileLoad_Date,
      f.Source_File_Date   AS Forecast_Source_File_Date
    FROM actual_reallocated a
    FULL OUTER JOIN forecast_reallocated f
      ON  a.QGP_Week      = f.QGP_Week
      AND a.LOB_Supported = f.LOB_Supported
  ),

  rolled_up AS (
    SELECT
      QGP_Week,
      LOB_Supported,
      MAX(Week_Beginning_Monday)    AS Week_Beginning_Monday,
      MAX(Week_Ending_Sunday)       AS Week_Ending_Sunday,
      SUM(Spend_Actual)             AS Spend_Actual,
      SUM(Spend_Forecast)           AS Spend_Forecast,
      MAX(Actual_FileLoad_Date)     AS Actual_FileLoad_Date,
      MAX(Actual_Source_File_Date)  AS Actual_Source_File_Date,
      MAX(Forecast_FileLoad_Date)   AS Forecast_FileLoad_Date,
      MAX(Forecast_Source_File_Date) AS Forecast_Source_File_Date
    FROM resolved_granular
    GROUP BY QGP_Week, LOB_Supported
  )

  SELECT
    cal.quarter                                                           AS Quarter,
    r.Week_Beginning_Monday,
    r.Week_Ending_Sunday,
    r.QGP_Week,
    r.LOB_Supported,
    r.Spend_Actual,
    r.Spend_Forecast,
    SUM(r.Spend_Actual)
      OVER (PARTITION BY r.Week_Beginning_Monday, r.LOB_Supported)       AS Spend_Actual_FullWeek,
    SUM(r.Spend_Forecast)
      OVER (PARTITION BY r.Week_Beginning_Monday, r.LOB_Supported)       AS Spend_Forecast_FullWeek,
    CASE
      WHEN r.Spend_Actual   IS NOT NULL AND r.Spend_Forecast IS NOT NULL THEN 'Actual+Forecast'
      WHEN r.Spend_Actual   IS NOT NULL                                  THEN 'Actual'
      WHEN r.Spend_Forecast IS NOT NULL                                  THEN 'Forecast'
    END                                                                   AS Spend_Status,
    cal.week_type,
    r.Actual_FileLoad_Date,
    r.Actual_Source_File_Date,
    r.Forecast_FileLoad_Date,
    r.Forecast_Source_File_Date
  FROM rolled_up r
  JOIN prdrzranalytics.lab42.sdi_vw_mfc_dim_qgp_calendar cal
    ON r.QGP_Week = cal.qgp_date;

END;