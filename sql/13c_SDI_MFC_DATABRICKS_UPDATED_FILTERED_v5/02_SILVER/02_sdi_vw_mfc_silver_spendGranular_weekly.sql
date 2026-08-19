-- ============================================================
-- SILVER 2 — SPEND, GRANULAR
-- ============================================================
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mfc_silver_spendGranular_weekly()
SQL SECURITY DEFINER
COMMENT 'Creates/refreshes sdi_tbl_mfc_silver_spendGranular_weekly. Actual and Forecast each come from Bronze''s single latest-file-snapshot per week, processed independently. No blended Spend_Final. Raw quarter tags from the source pass through as-is — no day-proportional boundary-week reallocation.'
BEGIN
  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_mfc_silver_spendGranular_weekly
  USING DELTA
  COMMENT 'MFC Silver Spend (granular). Spend_Actual and Spend_Forecast only — no blended Spend_Final. Boundary weeks use raw quarter tags from the source: if one quarter has a value and the other does not, only the quarter with a value is populated; if both have values, each carries its own raw value; if neither has a value, both are NULL. No day-proportional split is applied.'
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

  -- ── Actuals ─────────────────────────────────────────────────────────────────
  actual_quarter_totals AS (
    SELECT
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      LOB_Supported,
      Channel,
      Tactic,
      Message_Type,
      Agency,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2)) AS Quarter,
      SUM(weekly_actual)    AS quarter_total_actual,
      MAX(FileLoad_Date)    AS FileLoad_Date,
      MAX(Source_File_Date) AS Source_File_Date
    FROM prdrzranalytics.lab42.sdi_tbl_mfc_bronze_spendActualsGranular_weekly
    GROUP BY
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      Channel, Tactic, Message_Type, Agency,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2))
  ),

  actual_reallocated AS (
    -- BOUNDARY_FIRST rows: Bronze quarter tag matches the new (post-boundary) quarter
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.QGP_Week,
      qt.LOB_Supported,
      qt.Channel,
      qt.Tactic,
      qt.Message_Type,
      qt.Agency,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.quarter              AS Quarter,
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
      cr.stub_QGP_Week        AS QGP_Week,
      qt.LOB_Supported,
      qt.Channel,
      qt.Tactic,
      qt.Message_Type,
      qt.Agency,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.stub_quarter         AS Quarter,
      qt.quarter_total_actual AS weekly_actual,
      'BOUNDARY_STUB'         AS week_type
    FROM actual_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week  = cr.QGP_Week
      AND qt.Quarter   = cr.stub_quarter
    WHERE cr.week_type = 'BOUNDARY_FIRST'
      AND cr.stub_QGP_Week IS NOT NULL

    UNION ALL

    -- NORMAL weeks: straight pass-through
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.QGP_Week,
      qt.LOB_Supported,
      qt.Channel,
      qt.Tactic,
      qt.Message_Type,
      qt.Agency,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.quarter              AS Quarter,
      qt.quarter_total_actual AS weekly_actual,
      cr.week_type
    FROM actual_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week = cr.QGP_Week
      AND qt.Quarter  = cr.quarter
    WHERE cr.week_type = 'NORMAL'
  ),

  -- ── Forecasts ────────────────────────────────────────────────────────────────
  forecast_quarter_totals AS (
    SELECT
      Week_Beginning_Monday,
      Week_Ending_Sunday,
      QGP_Week,
      LOB_Supported,
      Channel,
      Tactic,
      Message_Type,
      Agency,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2)) AS Quarter,
      SUM(weekly_forecast)    AS quarter_total_forecast,
      MAX(FileLoad_Date)      AS FileLoad_Date,
      MAX(Source_File_Date)   AS Source_File_Date
    FROM prdrzranalytics.lab42.sdi_tbl_mfc_bronze_spendForecastGranular_weekly
    GROUP BY
      Week_Beginning_Monday, Week_Ending_Sunday, QGP_Week, LOB_Supported,
      Channel, Tactic, Message_Type, Agency,
      CONCAT('20', SUBSTRING(Quarter, 4, 2), ' ', SUBSTRING(Quarter, 1, 2))
  ),

  forecast_reallocated AS (
    -- BOUNDARY_FIRST rows
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.QGP_Week,
      qt.LOB_Supported,
      qt.Channel,
      qt.Tactic,
      qt.Message_Type,
      qt.Agency,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.quarter                  AS Quarter,
      qt.quarter_total_forecast   AS weekly_forecast,
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
      cr.stub_QGP_Week            AS QGP_Week,
      qt.LOB_Supported,
      qt.Channel,
      qt.Tactic,
      qt.Message_Type,
      qt.Agency,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.stub_quarter             AS Quarter,
      qt.quarter_total_forecast   AS weekly_forecast,
      'BOUNDARY_STUB'             AS week_type
    FROM forecast_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week  = cr.QGP_Week
      AND qt.Quarter   = cr.stub_quarter
    WHERE cr.week_type = 'BOUNDARY_FIRST'
      AND cr.stub_QGP_Week IS NOT NULL

    UNION ALL

    -- NORMAL weeks
    SELECT
      qt.Week_Beginning_Monday,
      qt.Week_Ending_Sunday,
      cr.QGP_Week,
      qt.LOB_Supported,
      qt.Channel,
      qt.Tactic,
      qt.Message_Type,
      qt.Agency,
      qt.FileLoad_Date,
      qt.Source_File_Date,
      cr.quarter                  AS Quarter,
      qt.quarter_total_forecast   AS weekly_forecast,
      cr.week_type
    FROM forecast_quarter_totals qt
    JOIN calendar_resolved cr
      ON  qt.QGP_Week = cr.QGP_Week
      AND qt.Quarter  = cr.quarter
    WHERE cr.week_type = 'NORMAL'
  )

  -- ── Final: FULL OUTER JOIN actuals + forecasts at full granularity ──────────
  SELECT
    COALESCE(a.Quarter,              f.Quarter)              AS Quarter,
    COALESCE(a.Week_Beginning_Monday,f.Week_Beginning_Monday) AS Week_Beginning_Monday,
    COALESCE(a.Week_Ending_Sunday,   f.Week_Ending_Sunday)   AS Week_Ending_Sunday,
    COALESCE(a.QGP_Week,             f.QGP_Week)             AS QGP_Week,
    COALESCE(a.LOB_Supported,        f.LOB_Supported)        AS LOB_Supported,
    COALESCE(a.Channel,              f.Channel)              AS Channel,
    COALESCE(a.Tactic,               f.Tactic)               AS Tactic,
    COALESCE(a.Message_Type,         f.Message_Type)         AS Message_Type,
    COALESCE(a.Agency,               f.Agency)               AS Agency,
    a.weekly_actual                                          AS Spend_Actual,
    f.weekly_forecast                                        AS Spend_Forecast,
    SUM(a.weekly_actual) OVER (
      PARTITION BY
        COALESCE(a.Week_Beginning_Monday, f.Week_Beginning_Monday),
        COALESCE(a.LOB_Supported,         f.LOB_Supported),
        COALESCE(a.Channel,               f.Channel),
        COALESCE(a.Tactic,                f.Tactic),
        COALESCE(a.Message_Type,          f.Message_Type),
        COALESCE(a.Agency,                f.Agency)
    )                                                        AS Spend_Actual_FullWeek,
    SUM(f.weekly_forecast) OVER (
      PARTITION BY
        COALESCE(a.Week_Beginning_Monday, f.Week_Beginning_Monday),
        COALESCE(a.LOB_Supported,         f.LOB_Supported),
        COALESCE(a.Channel,               f.Channel),
        COALESCE(a.Tactic,                f.Tactic),
        COALESCE(a.Message_Type,          f.Message_Type),
        COALESCE(a.Agency,                f.Agency)
    )                                                        AS Spend_Forecast_FullWeek,
    CASE
      WHEN a.weekly_actual   IS NOT NULL AND f.weekly_forecast IS NOT NULL THEN 'Actual+Forecast'
      WHEN a.weekly_actual   IS NOT NULL                                   THEN 'Actual'
      WHEN f.weekly_forecast IS NOT NULL                                   THEN 'Forecast'
    END                                                      AS Spend_Status,
    COALESCE(a.week_type, f.week_type)                       AS week_type,
    a.FileLoad_Date      AS Actual_FileLoad_Date,
    a.Source_File_Date   AS Actual_Source_File_Date,
    f.FileLoad_Date      AS Forecast_FileLoad_Date,
    f.Source_File_Date   AS Forecast_Source_File_Date
  FROM actual_reallocated a
  FULL OUTER JOIN forecast_reallocated f
    ON  a.QGP_Week     = f.QGP_Week
    AND a.LOB_Supported = f.LOB_Supported
    AND a.Channel       <=> f.Channel
    AND a.Tactic        <=> f.Tactic
    AND a.Message_Type  <=> f.Message_Type
    AND a.Agency        <=> f.Agency;

END;